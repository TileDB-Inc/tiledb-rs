use super::*;

use std::cell::{RefCell, RefMut};
use std::pin::Pin;

use crate::convert::{
    Buffer, BufferMut, CAPISameRepr, DataReceiver, HasScratchSpaceStrategy,
    InputData, OutputLocation, ReadResult, ScratchAllocator,
};
use crate::query::private::QueryCAPIInterface;
use crate::Result as TileDBResult;

mod callback;
mod managed;
mod raw;
mod typed;

pub use callback::*;
pub use managed::*;
pub use raw::*;
pub use typed::*;

pub enum ReadStepOutput<I, F> {
    NotEnoughSpace,
    Intermediate(I),
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

pub trait ReadQuery<'ctx>:
    ContextBound<'ctx> + QueryCAPIInterface + Sized
{
    type Intermediate;
    type Final;

    /// Run the query to fill scratch space.
    // TODO: how should this indicate "not enough space to write any data"?
    fn step(
        &mut self,
    ) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>>;

    /// Run the query to completion.
    /// Operations may be interleaved between individual steps
    /// of the query.
    fn execute(&mut self) -> TileDBResult<Self::Final> {
        Ok(loop {
            if let ReadStepOutput::Final(result) = self.step()? {
                break result;
            }
        })
    }
}

pub trait ReadQueryBuilder<'ctx>: Sized + QueryBuilder<'ctx> {
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

    fn add_result<'data, S, T>(
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

    fn add_result_managed<'data, S, T, R, C>(
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
            self.add_result::<S, T>(field, scratch)
        }?;

        Ok(ManagedReadBuilder {
            alloc: a,
            scratch,
            base,
        })
    }
}

pub struct ReadBuilder<'ctx> {
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

impl<'ctx> ContextBound<'ctx> for ReadBuilder<'ctx> {
    fn context(&self) -> &'ctx Context {
        self.base.context()
    }
}

impl<'ctx> QueryCAPIInterface for ReadBuilder<'ctx> {
    fn raw(&self) -> &RawQuery {
        &self.base.raw()
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
