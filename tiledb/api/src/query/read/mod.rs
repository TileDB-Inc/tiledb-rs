use super::*;

use std::cell::{RefCell, RefMut};
use std::pin::Pin;

use crate::convert::CAPISameRepr;
use crate::query::private::QueryCAPIInterface;
use crate::query::read::output::{
    BufferMut, DataReceiver, HasScratchSpaceStrategy, OutputLocation,
    ReadResult, ScratchAllocator,
};
use crate::Result as TileDBResult;

mod callback;
mod managed;
pub mod output;
mod raw;
mod typed;

pub use callback::*;
pub use managed::*;
pub use raw::*;
pub use typed::*;

/// Contains a return status and/or result from submitting a query.
pub enum ReadStepOutput<I, F> {
    /// There was not enough space to hold any results.
    /// Allocate more space and try again.
    NotEnoughSpace,
    /// There was enough space for some, but not all, results.
    /// Contains the intermediate representation of those results.
    /// Re-submitting the query will advance to the next portion of results.
    Intermediate(I),
    /// Contains the final representation of the query results.
    /// Re-submitting the query again will start over from the beginning.
    Final(F),
}

impl<I, F> ReadStepOutput<I, F> {
    pub fn is_intermediate(&self) -> bool {
        matches!(self, ReadStepOutput::Intermediate(_))
    }

    pub fn is_final(&self) -> bool {
        matches!(self, ReadStepOutput::Final(_))
    }
}

impl<U> ReadStepOutput<U, U> {
    pub fn unwrap(self) -> Option<U> {
        match self {
            ReadStepOutput::NotEnoughSpace => None,
            ReadStepOutput::Intermediate(i) => Some(i),
            ReadStepOutput::Final(f) => Some(f),
        }
    }
}

/// Trait for runnable read queries.
pub trait ReadQuery<'ctx>:
    ContextBound<'ctx> + QueryCAPIInterface + Sized
{
    type Intermediate;
    type Final;

    /// Run the query until it has filled up its scratch space.
    fn step(
        &mut self,
    ) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>>;

    /// Run the query to completion.
    /// Query adapters may interleave their operations
    /// between individual steps of the query.
    fn execute(&mut self) -> TileDBResult<Self::Final> {
        Ok(loop {
            if let ReadStepOutput::Final(result) = self.step()? {
                break result;
            }
        })
    }
}

/// Trait for constructing a read query.
/// Provides methods for flexibly adapting requested attributes into raw results,
/// callbacks, or strongly-typed objects.
pub trait ReadQueryBuilder<'ctx>: Sized + QueryBuilder<'ctx> {
    /// Register a raw memory location to write query results into.
    fn register_raw<'data, S, C>(
        self,
        field: S,
        scratch: &'data RefCell<OutputLocation<'data, C>>,
    ) -> TileDBResult<RawReadBuilder<C, Self>>
    where
        S: AsRef<str>,
        C: CAPISameRepr,
    {
        Ok(RawReadBuilder {
            field: field.as_ref().to_string(),
            raw_read_output: RawReadOutput::new(scratch),
            base: self,
        })
    }

    /// Register a callback to be run on query results
    /// which are written into the provided scratch space.
    fn register_callback<'data, S, T>(
        self,
        field: S,
        callback: T,
        scratch: &'data RefCell<
            OutputLocation<'data, <T as DataReceiver>::Unit>,
        >,
    ) -> TileDBResult<CallbackReadBuilder<T, Self>>
    where
        S: AsRef<str>,
        T: DataReceiver,
    {
        let base = self.register_raw(field, scratch)?;

        Ok(CallbackReadBuilder { callback, base })
    }

    /// Register a callback to be run on query results.
    /// Scratch space for raw results is managed by the callback.
    fn register_callback_managed<'data, S, T, C>(
        self,
        field: S,
        callback: T,
        params: <<T as HasScratchSpaceStrategy<C>>::Strategy as ScratchAllocator<C>>::Parameters,
    ) -> TileDBResult<
        ManagedReadBuilder<
            'data,
            C,
            <T as HasScratchSpaceStrategy<C>>::Strategy,
            CallbackReadBuilder<'data, T, Self>,
        >,
    >
    where
        S: AsRef<str>,
        T: DataReceiver<Unit = C> + HasScratchSpaceStrategy<C>,
    {
        let a =
            <<T as HasScratchSpaceStrategy<C>>::Strategy as ScratchAllocator<
                C,
            >>::construct(params);
        let scratch = a.scratch_space();

        let scratch = OutputLocation {
            data: BufferMut::Owned(scratch.0),
            cell_offsets: scratch.1.map(|c| BufferMut::Owned(c)),
        };

        let scratch = Box::pin(RefCell::new(scratch));

        let base = {
            let scratch = scratch.as_ref().get_ref()
                as *const RefCell<
                    OutputLocation<'data, <T as DataReceiver>::Unit>,
                >;
            let scratch = unsafe {
                &*scratch
                    as &'data RefCell<
                        OutputLocation<'data, <T as DataReceiver>::Unit>,
                    >
            };
            self.register_callback(field, callback, scratch)
        }?;

        Ok(ManagedReadBuilder {
            alloc: a,
            scratch,
            base,
        })
    }

    /// Register a typed result to be constructed from the query results.
    /// Intermediate raw results are written into the provided scratch space.
    fn register_constructor<'data, S, T>(
        self,
        field: S,
        scratch: &'data RefCell<
            OutputLocation<
                'data,
                <<T as ReadResult>::Receiver as DataReceiver>::Unit,
            >,
        >,
    ) -> TileDBResult<TypedReadBuilder<'data, T, Self>>
    where
        S: AsRef<str>,
        T: ReadResult,
    {
        let r = T::new_receiver();
        Ok(TypedReadBuilder {
            _marker: std::marker::PhantomData,
            base: self.register_callback(field, r, scratch)?,
        })
    }

    /// Register a typed result to be constructed from the query results.
    /// Scratch space for raw results is managed by the callback.
    fn register_constructor_managed<'data, S, T, R, C>(
        self,
        field: S,
        params: <<T as HasScratchSpaceStrategy<C>>::Strategy as ScratchAllocator<C>>::Parameters,
    ) -> TileDBResult<
        ManagedReadBuilder<
            'data,
            C,
            <T as HasScratchSpaceStrategy<C>>::Strategy,
            TypedReadBuilder<'data, T, Self>,
        >,
    >
    where
        S: AsRef<str>,
        T: ReadResult<Receiver = R> + HasScratchSpaceStrategy<C>,
        R: DataReceiver<Unit = C>,
    {
        let a =
            <<T as HasScratchSpaceStrategy<C>>::Strategy as ScratchAllocator<
                C,
            >>::construct(params);
        let scratch = a.scratch_space();

        let scratch = OutputLocation {
            data: BufferMut::Owned(scratch.0),
            cell_offsets: scratch.1.map(|c| BufferMut::Owned(c)),
        };

        let scratch = Box::pin(RefCell::new(scratch));

        let base = {
            let scratch = scratch.as_ref().get_ref()
                as *const RefCell<OutputLocation<'data, C>>;
            let scratch = unsafe {
                &*scratch as &'data RefCell<OutputLocation<'data, C>>
            };
            self.register_constructor::<S, T>(field, scratch)
        }?;

        Ok(ManagedReadBuilder {
            alloc: a,
            scratch,
            base,
        })
    }
}

#[derive(ContextBound, QueryCAPIInterface)]
pub struct ReadBuilder<'ctx> {
    #[base(ContextBound, QueryCAPIInterface)]
    base: BuilderBase<'ctx>,
}

impl<'ctx> ReadBuilder<'ctx> {
    pub fn new(
        context: &'ctx Context,
        array: Array<'ctx>,
    ) -> TileDBResult<Self> {
        Ok(ReadBuilder {
            base: BuilderBase::new(context, array, QueryType::Read)?,
        })
    }
}

impl<'ctx> QueryBuilder<'ctx> for ReadBuilder<'ctx> {
    type Query = Query<'ctx>;

    fn array(&self) -> &Array {
        &self.base.array()
    }

    fn build(self) -> Self::Query {
        self.base.build()
    }
}

impl<'ctx> ReadQueryBuilder<'ctx> for ReadBuilder<'ctx> {}
