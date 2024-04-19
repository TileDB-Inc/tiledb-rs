use super::*;

use crate::error::Error;
use crate::query::buffer::QueryBuffersMut;

/// Encapsulates data for writing intermediate query results for a data field.
pub(crate) struct RawReadHandle<'data, C> {
    /// Name of the field which this handle receives data from
    pub field: String,

    /// As input to the C API, the size of the data buffer.
    /// As output from the C API, the size in bytes of an intermediate result.
    pub data_size: Pin<Box<u64>>,

    /// As input to the C API, the size of the cell offsets buffer.
    /// As output from the C API, the size in bytes of intermediate offset results.
    pub offsets_size: Option<Pin<Box<u64>>>,

    /// Buffers for writing data and cell offsets.
    /// These are re-registered with the query at each step.
    /// The application which owns the query may own these buffers,
    /// or defer their management to the reader.
    // In the case of the former, the application can do whatever it wants with the
    // buffers between steps of a query.
    // RefCell is used so that the query can write to the buffers when it is executing
    // but the application can do whatever with the buffers between steps.
    pub location: &'data RefCell<QueryBuffersMut<'data, C>>,
}

impl<'data, C> RawReadHandle<'data, C> {
    pub fn new<S>(
        field: S,
        location: &'data RefCell<QueryBuffersMut<'data, C>>,
    ) -> Self
    where
        S: AsRef<str>,
    {
        let (data, cell_offsets) = {
            let mut scratch: RefMut<QueryBuffersMut<'data, C>> =
                location.borrow_mut();

            let data = scratch.data.as_mut() as *mut [C];
            let data = unsafe { &mut *data as &mut [C] };

            let cell_offsets = scratch.cell_offsets.as_mut().map(|c| {
                let c = c.as_mut() as *mut [u64];
                unsafe { &mut *c as &mut [u64] }
            });

            (data, cell_offsets)
        };

        let data_size = Box::pin(std::mem::size_of_val(&*data) as u64);

        let offsets_size = cell_offsets.as_ref().map(|off| {
            let sz = std::mem::size_of_val::<[u64]>(*off);
            Box::pin(sz as u64)
        });

        RawReadHandle {
            field: field.as_ref().to_string(),
            data_size,
            offsets_size,
            location,
        }
    }

    pub(crate) fn attach_query(
        &mut self,
        context: &Context,
        c_query: *mut ffi::tiledb_query_t,
    ) -> TileDBResult<()> {
        let c_context = context.capi();
        let c_name = cstring!(&*self.field);

        let mut location = self.location.borrow_mut();

        *self.data_size.as_mut() =
            std::mem::size_of_val::<[C]>(&location.data) as u64;

        context.capi_return({
            let data = &mut location.data;
            let c_bufptr = data.as_mut().as_ptr() as *mut std::ffi::c_void;
            let c_sizeptr = self.data_size.as_mut().get_mut() as *mut u64;

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

        let cell_offsets = &mut location.cell_offsets;

        if let Some(ref mut offsets_size) = self.offsets_size.as_mut() {
            let cell_offsets = cell_offsets.as_mut().unwrap();

            *offsets_size.as_mut() =
                std::mem::size_of_val::<[u64]>(cell_offsets) as u64;

            let c_offptr = cell_offsets.as_mut_ptr();
            let c_sizeptr = offsets_size.as_mut().get_mut() as *mut u64;

            context.capi_return(unsafe {
                ffi::tiledb_query_set_offsets_buffer(
                    c_context,
                    c_query,
                    c_name.as_ptr(),
                    c_offptr,
                    c_sizeptr,
                )
            })?;
        }

        Ok(())
    }

    pub fn last_read_size(&self) -> (usize, usize) {
        let records_written = match self.offsets_size.as_ref() {
            Some(offsets_size) => {
                **offsets_size as usize / std::mem::size_of::<u64>()
            }
            None => *self.data_size as usize / std::mem::size_of::<C>(),
        };
        let bytes_written = *self.data_size as usize;

        (records_written, bytes_written)
    }
}

/// Reads query results into a raw buffer.
/// This is the most flexible way to read data but also the most cumbersome.
/// Recommended usage is to run the query one step at a time, and borrow
/// the buffers between each step to process intermediate results.
#[derive(ContextBound, QueryCAPIInterface)]
pub struct RawReadQuery<'data, C, Q> {
    pub(crate) raw_read_output: RawReadHandle<'data, C>,
    #[base(ContextBound, QueryCAPIInterface)]
    pub(crate) base: Q,
}

impl<'ctx, 'data, C, Q> ReadQuery for RawReadQuery<'data, C, Q>
where
    Q: ReadQuery + ContextBound<'ctx> + QueryCAPIInterface,
{
    type Intermediate = (usize, usize, Q::Intermediate);
    type Final = (usize, usize, Q::Final);

    fn step(
        &mut self,
    ) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>> {
        /* update the internal buffers */
        self.raw_read_output
            .attach_query(self.context(), **self.cquery())?;

        /* then execute */
        let base_result = {
            let _ = self.raw_read_output.location.borrow_mut();
            self.base.step()?
        };

        let (records_written, bytes_written) =
            self.raw_read_output.last_read_size();

        Ok(match base_result {
            ReadStepOutput::NotEnoughSpace => {
                /* TODO: check that records/bytes are zero and produce an internal error if not */
                ReadStepOutput::NotEnoughSpace
            }
            ReadStepOutput::Intermediate(base_result) => {
                if records_written == 0 && bytes_written == 0 {
                    ReadStepOutput::NotEnoughSpace
                } else if records_written == 0 {
                    return Err(Error::Internal(format!(
                        "Invalid read: returned {} offsets but {} bytes",
                        records_written, bytes_written
                    )));
                } else {
                    ReadStepOutput::Intermediate((
                        records_written,
                        bytes_written,
                        base_result,
                    ))
                }
            }
            ReadStepOutput::Final(base_result) => ReadStepOutput::Final((
                records_written,
                bytes_written,
                base_result,
            )),
        })
    }
}

#[derive(ContextBound, QueryCAPIInterface)]
pub struct RawReadBuilder<'data, C, B> {
    pub(crate) raw_read_output: RawReadHandle<'data, C>,
    #[base(ContextBound, QueryCAPIInterface)]
    pub(crate) base: B,
}

impl<'ctx, 'data, C, B> QueryBuilder<'ctx> for RawReadBuilder<'data, C, B>
where
    B: QueryBuilder<'ctx>,
{
    type Query = RawReadQuery<'data, C, B::Query>;

    fn build(self) -> Self::Query {
        RawReadQuery {
            raw_read_output: self.raw_read_output,
            base: self.base.build(),
        }
    }
}

impl<'ctx, 'data, C, B> ReadQueryBuilder<'ctx> for RawReadBuilder<'data, C, B> where
    B: ReadQueryBuilder<'ctx>
{
}
