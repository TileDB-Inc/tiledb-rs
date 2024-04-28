use std::pin::Pin;

use anyhow::anyhow;

use super::buffer::BufferCollectionItem;
use super::conditions::QueryCondition;
use super::sizeinfo::SizeEntry;
use super::subarray::RawSubarray;
use super::{QueryLayout, Subarray, SubarrayBuilder};
use crate::array::Array;
use crate::context::{CApiInterface, Context};
use crate::error::Error;
use crate::range::Range;
use crate::Result as TileDBResult;

pub trait Query: Sized {
    fn context(&self) -> &Context;
    fn array(&self) -> &Array;
    fn capi(&self) -> *mut ffi::tiledb_query_t;

    fn subarray(&self) -> TileDBResult<Subarray> {
        let context = self.context();
        let c_query = self.capi();
        let mut c_subarray: *mut ffi::tiledb_subarray_t = out_ptr!();
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_query_get_subarray_t(ctx, c_query, &mut c_subarray)
        })?;

        Ok(Subarray::new(context, RawSubarray::Owned(c_subarray)))
    }

    fn ranges(&self) -> TileDBResult<Vec<Vec<Range>>> {
        let schema = self.array().schema()?;
        let subarray = self.subarray()?;
        subarray.ranges(&schema)
    }

    /// Returns the ffi status of the last submit()
    fn capi_status(&self) -> TileDBResult<ffi::tiledb_query_status_t> {
        let c_query = self.capi();
        let mut c_status: ffi::tiledb_query_status_t = out_ptr!();
        self.context()
            .capi_call(|ctx| unsafe {
                ffi::tiledb_query_get_status(ctx, c_query, &mut c_status)
            })
            .map(|_| c_status)
    }
}

pub(crate) trait QueryInternal {
    fn context(&self) -> &Context;
    fn capi(&self) -> *mut ffi::tiledb_query_t;
    fn buffer_info(&self, name: &str) -> Option<(bool, bool)>;
    fn submitted(&self) -> bool;

    fn attach_buffer(
        &self,
        buffer: &dyn BufferCollectionItem,
    ) -> TileDBResult<SizeEntry> {
        let field = buffer.name();

        let c_query = self.capi();
        let c_name = cstring!(field);

        // If this is a query resubmission we need to check that the buffers
        // being provided exactly match the previous submission. That doesn't
        // mean the pointers need to be set, just that a buffer either exists
        // or doesn't matching the previous submissions.
        if self.submitted() {
            if let Some((had_offsets, had_validity)) = self.buffer_info(field) {
                if had_offsets != buffer.offsets_ptr().is_some() {
                    if had_offsets {
                        return Err(Error::InvalidArgument(anyhow!(
                            "Missing offsets buffer for field: {}",
                            field
                        )));
                    } else {
                        return Err(Error::InvalidArgument(anyhow!(
                            "Offsets buffer was not previously set for: {}",
                            field
                        )));
                    }
                }

                if had_validity != buffer.validity_ptr().is_some() {
                    if had_validity {
                        return Err(Error::InvalidArgument(anyhow!(
                            "Missing validity buffer for field: {}",
                            field
                        )));
                    } else {
                        return Err(Error::InvalidArgument(anyhow!(
                            "Validity buffer was not previously set for: {}",
                            field
                        )));
                    }
                }
            } else {
                return Err(Error::InvalidArgument(anyhow!(
                    "Field was not previously part of this query: {}",
                    field
                )));
            }
        }

        // Set the data buffer, then the offset and validity if they're
        // present on the WriteBuffer.
        let mut data_size = Box::pin(buffer.data_size());
        let c_data_size = data_size.as_mut().get_mut() as *mut u64;
        self.context().capi_call(|ctx| unsafe {
            ffi::tiledb_query_set_data_buffer(
                ctx,
                c_query,
                c_name.as_ptr(),
                buffer.data_ptr(),
                c_data_size,
            )
        })?;

        let mut offsets_size: Option<Pin<Box<u64>>> = None;
        if let Some(c_offsets) = buffer.offsets_ptr() {
            let mut tmp_size = Box::pin(buffer.offsets_size().unwrap());
            let c_offsets_size = tmp_size.as_mut().get_mut() as *mut u64;
            self.context().capi_call(|ctx| unsafe {
                ffi::tiledb_query_set_offsets_buffer(
                    ctx,
                    c_query,
                    c_name.as_ptr(),
                    c_offsets,
                    c_offsets_size,
                )
            })?;
            offsets_size = Some(tmp_size);
        }

        let mut validity_size: Option<Pin<Box<u64>>> = None;
        if let Some(c_validity) = buffer.validity_ptr() {
            let mut tmp_size = Box::pin(buffer.validity_size().unwrap());
            let c_validity_size = tmp_size.as_mut().get_mut() as *mut u64;
            self.context().capi_call(|ctx| unsafe {
                ffi::tiledb_query_set_validity_buffer(
                    ctx,
                    c_query,
                    c_name.as_ptr(),
                    c_validity,
                    c_validity_size,
                )
            })?;
            validity_size = Some(tmp_size);
        }

        Ok(SizeEntry {
            data_size,
            offsets_size,
            validity_size,
        })
    }

    /// Execute the query
    fn do_submit(&self) -> TileDBResult<()> {
        let c_query = self.capi();
        self.context().capi_call(|ctx| unsafe {
            ffi::tiledb_query_submit(ctx, c_query)
        })?;
        Ok(())
    }

    fn do_finalize(&self) -> TileDBResult<()> {
        let c_query = self.capi();
        self.context().capi_call(|ctx| unsafe {
            ffi::tiledb_query_finalize(ctx, c_query)
        })?;
        Ok(())
    }
}

pub trait QueryBuilder
where
    Self: Sized,
{
    fn context(&self) -> &Context;
    fn array(&self) -> &Array;
    fn capi(&self) -> *mut ffi::tiledb_query_t;

    fn layout(self, layout: QueryLayout) -> TileDBResult<Self> {
        let c_query = self.capi();
        let c_layout = layout.capi_enum();
        self.context().capi_call(|ctx| unsafe {
            ffi::tiledb_query_set_layout(ctx, c_query, c_layout)
        })?;
        Ok(self)
    }

    fn start_subarray(self) -> TileDBResult<SubarrayBuilder<Self>> {
        SubarrayBuilder::for_query(self)
    }

    fn query_condition(self, qc: QueryCondition) -> TileDBResult<Self> {
        let c_query = self.capi();
        let c_cond = qc.capi();
        self.context().capi_call(|ctx| unsafe {
            ffi::tiledb_query_set_condition(ctx, c_query, c_cond)
        })?;
        Ok(self)
    }
}
