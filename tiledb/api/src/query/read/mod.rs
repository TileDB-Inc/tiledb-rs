use super::*;

use std::cell::{RefCell, RefMut};
use std::pin::Pin;

use crate::convert::CAPISameRepr;
use crate::query::private::QueryCAPIInterface;
use crate::query::read::output::{
    BufferMut, HasScratchSpaceStrategy, OutputLocation, ScratchAllocator,
};
use crate::Result as TileDBResult;

mod callback;
mod managed;
pub mod output;
mod raw;
mod splitter;
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

    pub fn as_ref(&self) -> ReadStepOutput<&I, &F> {
        match self {
            ReadStepOutput::NotEnoughSpace => ReadStepOutput::NotEnoughSpace,
            ReadStepOutput::Intermediate(ref i) => {
                ReadStepOutput::Intermediate(i)
            }
            ReadStepOutput::Final(ref f) => ReadStepOutput::Final(f),
        }
    }

    pub fn map_i<U, FN>(self, f: FN) -> ReadStepOutput<U, F>
    where
        FN: FnOnce(I) -> U,
    {
        match self {
            ReadStepOutput::NotEnoughSpace => ReadStepOutput::NotEnoughSpace,
            ReadStepOutput::Intermediate(i) => {
                ReadStepOutput::Intermediate(f(i))
            }
            ReadStepOutput::Final(f) => ReadStepOutput::Final(f),
        }
    }

    pub fn map_f<U, FN>(self, f: FN) -> ReadStepOutput<I, U>
    where
        FN: FnOnce(F) -> U,
    {
        match self {
            ReadStepOutput::NotEnoughSpace => ReadStepOutput::NotEnoughSpace,
            ReadStepOutput::Intermediate(i) => ReadStepOutput::Intermediate(i),
            ReadStepOutput::Final(fr) => ReadStepOutput::Final(f(fr)),
        }
    }

    pub fn unwrap_intermediate(self) -> I {
        match self {
            ReadStepOutput::Intermediate(i) => i,
            ReadStepOutput::NotEnoughSpace => panic!("Called `ReadStepOutput::unwrap_intermediate` on `NotEnoughSpace`"),
            ReadStepOutput::Final(_) => panic!("Called `ReadStepOutput::unwrap_intermediate` on `Final`"),
        }
    }

    pub fn unwrap_final(self) -> F {
        match self {
            ReadStepOutput::Final(f) => f,
            ReadStepOutput::Intermediate(_) => panic!(
                "Called `ReadStepOutput::unwrap_final` on `Intermediate`"
            ),
            ReadStepOutput::NotEnoughSpace => panic!(
                "Called `ReadStepOutput::unwrap_final` on `NotEnoughSpace`"
            ),
        }
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

impl<I, F> Clone for ReadStepOutput<I, F>
where
    I: Clone,
    F: Clone,
{
    fn clone(&self) -> Self {
        match self {
            ReadStepOutput::NotEnoughSpace => ReadStepOutput::NotEnoughSpace,
            ReadStepOutput::Intermediate(i) => {
                ReadStepOutput::Intermediate(i.clone())
            }
            ReadStepOutput::Final(f) => ReadStepOutput::Final(f.clone()),
        }
    }
}

/// Trait for runnable read queries.
pub trait ReadQuery: Sized {
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
            OutputLocation<'data, <T as ReadCallback>::Unit>,
        >,
    ) -> TileDBResult<CallbackReadBuilder<T, Self>>
    where
        S: AsRef<str>,
        T: ReadCallback,
    {
        let base = self.register_raw(field, scratch)?;

        Ok(CallbackReadBuilder { callback, base })
    }

    /// Register a callback to be run on query results.
    /// Scratch space for raw results is managed by the callback.
    fn register_callback_managed<'data, S, T, C, A>(
        self,
        field: S,
        callback: T,
        scratch_allocator: A,
    ) -> TileDBResult<
        ManagedReadBuilder<'data, C, A, CallbackReadBuilder<'data, T, Self>>,
    >
    where
        S: AsRef<str>,
        T: ReadCallback<Unit = C> + HasScratchSpaceStrategy<C, Strategy = A>,
        A: ScratchAllocator<C>,
    {
        let scratch = scratch_allocator.alloc();

        let scratch = OutputLocation {
            data: BufferMut::Owned(scratch.0),
            cell_offsets: scratch.1.map(BufferMut::Owned),
        };

        let scratch = Box::pin(RefCell::new(scratch));

        let base = {
            let scratch = scratch.as_ref().get_ref()
                as *const RefCell<
                    OutputLocation<'data, <T as ReadCallback>::Unit>,
                >;
            let scratch = unsafe {
                &*scratch
                    as &'data RefCell<
                        OutputLocation<'data, <T as ReadCallback>::Unit>,
                    >
            };
            self.register_callback(field, callback, scratch)
        }?;

        Ok(ManagedReadBuilder {
            alloc: scratch_allocator,
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
                <<T as ReadResult>::Constructor as ReadCallback>::Unit,
            >,
        >,
    ) -> TileDBResult<TypedReadBuilder<'data, T, Self>>
    where
        S: AsRef<str>,
        T: ReadResult,
    {
        let r = <T::Constructor as Default>::default();
        Ok(TypedReadBuilder {
            _marker: std::marker::PhantomData,
            base: self.register_callback(field, r, scratch)?,
        })
    }

    /// Register a typed result to be constructed from the query results.
    /// Scratch space for raw results is managed by the callback.
    fn register_constructor_managed<'data, S, T, R, C, A>(
        self,
        field: S,
        scratch_allocator: A,
    ) -> TileDBResult<
        ManagedReadBuilder<'data, C, A, TypedReadBuilder<'data, T, Self>>,
    >
    where
        S: AsRef<str>,
        T: ReadResult<Constructor = R>
            + HasScratchSpaceStrategy<C, Strategy = A>,
        R: ReadCallback<Unit = C>,
        A: ScratchAllocator<C>,
    {
        let scratch = scratch_allocator.alloc();

        let scratch = OutputLocation {
            data: BufferMut::Owned(scratch.0),
            cell_offsets: scratch.1.map(BufferMut::Owned),
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
            alloc: scratch_allocator,
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

    fn build(self) -> Self::Query {
        self.base.build()
    }
}

impl<'ctx> ReadQueryBuilder<'ctx> for ReadBuilder<'ctx> {}
