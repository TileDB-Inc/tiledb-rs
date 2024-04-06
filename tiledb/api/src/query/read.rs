use super::*;

use std::ops::Deref;
use std::pin::Pin;

use crate::convert::{DataCollector, OutputLocation, ReadResult};
use crate::Result as TileDBResult;

pub trait ReadQuery: Sized {
    type Output;

    fn submit(self) -> TileDBResult<(Self::Output, Self)>;
}

struct RawReadOutput<'data, C> {
    data_size: Pin<Box<u64>>,
    offsets_size: Option<Pin<Box<u64>>>,
    destination: OutputLocation<'data, C>,
}

pub struct TypedReadQuery<'data, T, Q>
where
    T: DataCollector<'data>,
{
    _marker: std::marker::PhantomData<T>,
    base: Q,
    /* TODO: this isn't needed here, it should be something with 'data lifetime
     * that is capable of recycling the buffer if needed */
    raw_read_output: RawReadOutput<'data, <T as DataCollector<'data>>::Unit>,
}

impl<'data, T, Q> ReadQuery for TypedReadQuery<'data, T, Q>
where
    T: DataCollector<'data>,
    Q: ReadQuery,
{
    type Output = (T, Q::Output);

    fn submit(mut self) -> TileDBResult<(Self::Output, Self)> {
        let (base_result, base_query) = self.base.submit()?;

        let records_written = match self.raw_read_output.offsets_size.as_ref() {
            Some(offsets_size) => {
                **offsets_size as usize / std::mem::size_of::<u64>()
            }
            None => {
                *self.raw_read_output.data_size as usize
                    / std::mem::size_of::<<T as DataCollector<'data>>::Unit>()
            }
        };
        let bytes_written = *self.raw_read_output.data_size as usize;

        let this_result = T::construct(ReadResult {
            buffers: &mut self.raw_read_output.destination,
            records: records_written,
            bytes: bytes_written,
        })?;

        Ok((
            (this_result, base_result),
            TypedReadQuery {
                _marker: std::marker::PhantomData,
                base: base_query,
                raw_read_output: self.raw_read_output,
            },
        ))
    }
}

pub trait ReadQueryBuilder<'ctx>: Sized + QueryBuilder<'ctx> {
    fn data_typed<'data, S, T>(
        self,
        field: S,
        parameters: <T as DataCollector<'data>>::Parameters,
    ) -> TileDBResult<TypedReadBuilder<T, Self>>
    where
        S: AsRef<str>,
        T: DataCollector<'data>,
    {
        let c_context = self.context().capi();
        let c_query = **self.raw();
        let c_name = cstring!(field.as_ref());

        let mut destination = T::prepare(parameters);

        let (mut data_size, mut offsets_size) = {
            (
                Box::pin(destination.data.size() as u64),
                destination
                    .cell_offsets
                    .as_ref()
                    .map(|off| Box::pin(off.size() as u64)),
            )
        };

        self.capi_return({
            let c_bufptr =
                destination.data.as_mut().as_ptr() as *mut std::ffi::c_void;
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
                destination.cell_offsets.as_mut().unwrap().as_mut_ptr()
                    as *mut u64;
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
            destination,
        };

        Ok(TypedReadBuilder {
            _marker: std::marker::PhantomData,
            raw_read_output,
            base: self,
        })
    }
}

pub struct TypedReadBuilder<'data, T, B>
where
    T: DataCollector<'data>,
{
    _marker: std::marker::PhantomData<T>,
    raw_read_output: RawReadOutput<'data, <T as DataCollector<'data>>::Unit>,
    base: B,
}

impl<'ctx, 'data, T, B> ContextBound<'ctx> for TypedReadBuilder<'data, T, B>
where
    T: DataCollector<'data>,
    B: ContextBound<'ctx>,
{
    fn context(&self) -> &'ctx Context {
        self.base.context()
    }
}

impl<'data, T, B> crate::query::private::QueryCAPIInterface
    for TypedReadBuilder<'data, T, B>
where
    T: DataCollector<'data>,
    B: crate::query::private::QueryCAPIInterface,
{
    fn raw(&self) -> &RawQuery {
        self.base.raw()
    }
}

impl<'ctx, 'data, T, B> Deref for TypedReadBuilder<'data, T, B>
where
    T: DataCollector<'data>,
{
    type Target = B;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<'ctx, 'data, T, B> QueryBuilder<'ctx> for TypedReadBuilder<'data, T, B>
where
    T: DataCollector<'data>,
    B: QueryBuilder<'ctx>,
{
    type Query = TypedReadQuery<'data, T, B::Query>;

    fn array(&self) -> &Array {
        self.base.array()
    }

    fn build(self) -> Self::Query {
        TypedReadQuery {
            _marker: self._marker,
            raw_read_output: self.raw_read_output,
            base: self.base.build(),
        }
    }
}

impl<'ctx, 'data, T, B> ReadQueryBuilder<'ctx> for TypedReadBuilder<'data, T, B>
where
    T: DataCollector<'data>,
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
