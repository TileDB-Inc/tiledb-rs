use super::*;

use std::pin::Pin;

use crate::convert::{
    Buffer, DataReceiver, InputData, ReadResult, ScratchAllocator, ScratchSpace,
};
use crate::Result as TileDBResult;

pub trait ReadQuery: Sized {
    type Output;

    fn submit(self) -> TileDBResult<(Self::Output, Self)>;
}

struct RawReadOutput<C> {
    data_size: Pin<Box<u64>>,
    data: Box<[C]>,
    offsets_size: Option<Pin<Box<u64>>>,
    cell_offsets: Option<Box<[u64]>>,
}

pub struct CallbackReadQuery<T, Q>
where
    T: DataReceiver,
{
    receiver: T,
    scratch_alloc: T::ScratchAllocator,
    raw_read_output: RawReadOutput<T::Unit>,
    base: Q,
}

impl<T, Q> CallbackReadQuery<T, Q>
where
    T: DataReceiver,
    Q: ReadQuery,
{
    fn submit_impl(
        mut self,
    ) -> TileDBResult<(T, RawReadOutput<T::Unit>, <Self as ReadQuery>::Output, Q)>
    {
        let (base_result, base_query) = self.base.submit()?;

        let records_written = match self.raw_read_output.offsets_size.as_ref() {
            Some(offsets_size) => {
                **offsets_size as usize / std::mem::size_of::<u64>()
            }
            None => {
                *self.raw_read_output.data_size as usize
                    / std::mem::size_of::<<T as DataReceiver>::Unit>()
            }
        };
        let bytes_written = *self.raw_read_output.data_size as usize;

        /* TODO: check status and invoke callback with either borrowed or owned buffer */
        let input_data = InputData {
            data: Buffer::Borrowed(&*self.raw_read_output.data),
            cell_offsets: self
                .raw_read_output
                .cell_offsets
                .as_ref()
                .map(|c| Buffer::Borrowed(&*c)),
        };

        self.receiver
            .receive(records_written, bytes_written, input_data)?;

        Ok((self.receiver, self.raw_read_output, base_result, base_query))
    }
}

impl<T, Q> ReadQuery for CallbackReadQuery<T, Q>
where
    T: DataReceiver,
    Q: ReadQuery,
{
    type Output = Q::Output;

    fn submit(self) -> TileDBResult<(Self::Output, Self)> {
        let (receiver, raw_read_output, result, query) = self.submit_impl()?;
        Ok((
            result,
            CallbackReadQuery {
                receiver,
                scratch_alloc: T::ScratchAllocator::construct(
                    Default::default(),
                ),
                raw_read_output,
                base: query,
            },
        ))
    }
}

pub struct TypedReadQuery<T, Q>
where
    T: ReadResult,
{
    _marker: std::marker::PhantomData<T>,
    base: CallbackReadQuery<<T as ReadResult>::Receiver, Q>,
}

impl<T, Q> ReadQuery for TypedReadQuery<T, Q>
where
    T: ReadResult,
    Q: ReadQuery,
{
    type Output = (T, Q::Output);

    fn submit(self) -> TileDBResult<(Self::Output, Self)> {
        let (receiver, raw_read_output, base_result, base_query) =
            self.base.submit_impl()?;

        /* TODO: check status and if complete then do into self */

        let my_result = receiver.into();

        Ok((
            (my_result, base_result),
            TypedReadQuery {
                _marker: std::marker::PhantomData,
                base: CallbackReadQuery {
                    receiver: T::new_receiver(),
                    scratch_alloc:
                        <<T as ReadResult>::Receiver as DataReceiver>::ScratchAllocator::construct(
                            Default::default(),
                        ),
                    raw_read_output,
                    base: base_query,
                },
            },
        ))
    }
}

pub trait ReadQueryBuilder<'ctx>: Sized + QueryBuilder<'ctx> {
    fn add_callback<S, T>(
        self,
        field: S,
        callback: T,
    ) -> TileDBResult<CallbackReadBuilder<T, Self>>
    where
        S: AsRef<str>,
        T: DataReceiver,
    {
        let c_context = self.context().capi();
        let c_query = **self.raw();
        let c_name = cstring!(field.as_ref());

        let scratch_alloc = T::ScratchAllocator::construct(Default::default());
        let ScratchSpace(mut data, mut cell_offsets) =
            scratch_alloc.scratch_space();

        let (mut data_size, mut offsets_size) = {
            (
                Box::pin(std::mem::size_of_val(&*data) as u64),
                cell_offsets
                    .as_ref()
                    .map(|off| Box::pin(std::mem::size_of_val(&*off) as u64)),
            )
        };

        self.capi_return({
            let c_bufptr = data.as_mut().as_ptr() as *mut std::ffi::c_void;
            let c_sizeptr = data_size.as_mut().get_mut() as *mut u64;

            unsafe {
                ffi::tiledb_query_set_data_buffer(
                    c_context,
                    c_query,
                    c_name.as_ptr(),
                    c_bufptr,
                    c_sizeptr,
                )
            }
        })?;

        if let Some(ref mut offsets_size) = offsets_size.as_mut() {
            let c_offptr =
                cell_offsets.as_mut().unwrap().as_mut_ptr() as *mut u64;
            let c_sizeptr = offsets_size.as_mut().get_mut() as *mut u64;

            self.capi_return(unsafe {
                ffi::tiledb_query_set_offsets_buffer(
                    c_context,
                    c_query,
                    c_name.as_ptr(),
                    c_offptr,
                    c_sizeptr,
                )
            })?;
        }

        let raw_read_output = RawReadOutput {
            data_size,
            offsets_size,
            data,
            cell_offsets,
        };

        Ok(CallbackReadBuilder {
            callback,
            scratch_alloc,
            raw_read_output,
            base: self,
        })
    }

    fn add_result<S, T>(
        self,
        field: S,
    ) -> TileDBResult<TypedReadBuilder<T, Self>>
    where
        S: AsRef<str>,
        T: ReadResult,
    {
        let r = T::new_receiver();
        Ok(TypedReadBuilder {
            _marker: std::marker::PhantomData,
            base: self.add_callback(field, r)?,
        })
    }
}

pub struct CallbackReadBuilder<T, B>
where
    T: DataReceiver,
{
    callback: T,
    scratch_alloc: T::ScratchAllocator,
    raw_read_output: RawReadOutput<<T as DataReceiver>::Unit>,
    base: B,
}

impl<'ctx, T, B> ContextBound<'ctx> for CallbackReadBuilder<T, B>
where
    T: DataReceiver,
    B: ContextBound<'ctx>,
{
    fn context(&self) -> &'ctx Context {
        self.base.context()
    }
}

impl<T, B> crate::query::private::QueryCAPIInterface
    for CallbackReadBuilder<T, B>
where
    T: DataReceiver,
    B: crate::query::private::QueryCAPIInterface,
{
    fn raw(&self) -> &RawQuery {
        self.base.raw()
    }
}

impl<'ctx, T, B> QueryBuilder<'ctx> for CallbackReadBuilder<T, B>
where
    T: DataReceiver,
    B: QueryBuilder<'ctx>,
{
    type Query = CallbackReadQuery<T, B::Query>;

    fn array(&self) -> &Array {
        self.base.array()
    }

    fn build(self) -> Self::Query {
        CallbackReadQuery {
            receiver: self.callback,
            scratch_alloc: self.scratch_alloc,
            raw_read_output: self.raw_read_output,
            base: self.base.build(),
        }
    }
}

impl<'ctx, T, B> ReadQueryBuilder<'ctx> for CallbackReadBuilder<T, B>
where
    T: DataReceiver,
    B: ReadQueryBuilder<'ctx>,
{
}

pub struct TypedReadBuilder<T, B>
where
    T: ReadResult,
{
    _marker: std::marker::PhantomData<T>,
    base: CallbackReadBuilder<<T as ReadResult>::Receiver, B>,
}

impl<'ctx, T, B> ContextBound<'ctx> for TypedReadBuilder<T, B>
where
    T: ReadResult,
    B: ContextBound<'ctx>,
{
    fn context(&self) -> &'ctx Context {
        self.base.context()
    }
}

impl<T, B> crate::query::private::QueryCAPIInterface for TypedReadBuilder<T, B>
where
    T: ReadResult,
    B: crate::query::private::QueryCAPIInterface,
{
    fn raw(&self) -> &RawQuery {
        self.base.raw()
    }
}

impl<'ctx, T, B> QueryBuilder<'ctx> for TypedReadBuilder<T, B>
where
    T: ReadResult,
    B: QueryBuilder<'ctx>,
{
    type Query = TypedReadQuery<T, B::Query>;

    fn array(&self) -> &Array {
        self.base.array()
    }

    fn build(self) -> Self::Query {
        TypedReadQuery {
            _marker: self._marker,
            base: self.base.build(),
        }
    }
}

impl<'ctx, T, B> ReadQueryBuilder<'ctx> for TypedReadBuilder<T, B>
where
    T: ReadResult,
    B: ReadQueryBuilder<'ctx>,
{
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

impl<'ctx> private::QueryCAPIInterface for ReadBuilder<'ctx> {
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
