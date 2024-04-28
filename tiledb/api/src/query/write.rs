use std::collections::HashMap;
use std::pin::Pin;

use anyhow::anyhow;

use super::sizeinfo::SizeEntry;
use super::traits::{Query, QueryBuilder};
use super::RawQuery;
use crate::array::Array;
use crate::context::{CApiInterface, Context, ContextBound};
use crate::error::Error;
use crate::query::buffer::{
    WriteBufferCollection, WriteBufferCollectionEntry,
    WriteBufferCollectionItem,
};
use crate::query::QueryType;
use crate::wb_collection_entry_go;
use crate::Result as TileDBResult;

pub struct WriteQuery {
    array: Array,
    raw: RawQuery,
    buffers: HashMap<String, (bool, bool)>,
    submitted: bool,
}

impl ContextBound for WriteQuery {
    fn context(&self) -> &Context {
        self.array.context()
    }
}

impl Query for WriteQuery {
    fn context(&self) -> &Context {
        self.array.context()
    }

    fn array(&self) -> &Array {
        &self.array
    }

    fn capi(&self) -> *mut ffi::tiledb_query_t {
        *self.raw
    }
}

impl WriteQuery {
    pub fn new(array: Array, raw: RawQuery) -> Self {
        Self {
            array,
            raw,
            buffers: HashMap::new(),
            submitted: false,
        }
    }

    pub fn submit(
        &mut self,
        buffers: &WriteBufferCollection,
    ) -> TileDBResult<HashMap<String, SizeEntry>> {
        let mut ret: HashMap<String, SizeEntry> = HashMap::new();

        for buffer in buffers.iter() {
            let entry = self.attach_buffer(buffer)?;
            ret.insert(buffer.name().to_owned(), entry);
        }

        // Ensure that all buffers were provided if this is a resubmission.
        if self.submitted {
            for (field, _) in self.buffers.iter() {
                if !ret.contains_key(field) {
                    return Err(Error::InvalidArgument(anyhow!(
                        "Missing buffer for field: {}",
                        field
                    )));
                }
            }
        }

        // Set our buffer info for possible resubmission.
        if !self.submitted {
            for (field, sizes) in ret.iter() {
                let has_offsets = sizes.offsets_size.is_some();
                let has_validity = sizes.validity_size.is_some();
                self.buffers
                    .insert(field.to_owned(), (has_offsets, has_validity));
            }
        }

        // Mark after the first submission to make sure we're consistently
        // providing the same exact buffers for subsequent submissions.
        self.submitted = true;

        Ok(ret)
    }

    fn attach_buffer(
        &self,
        buffer: &WriteBufferCollectionItem,
    ) -> TileDBResult<SizeEntry> {
        let field = buffer.name();
        let entry = buffer.entry();

        let c_query = self.capi();
        let c_name = cstring!(field);

        wb_collection_entry_go!(entry, _DT, buf, {
            // If this is a query resubmission we need to check that the buffers
            // being provided exactly match the previous submission. That doesn't
            // mean the pointers need to be set, just that a buffer either exists
            // or doesn't matching the previous submissions.
            if self.submitted {
                if let Some((had_offsets, had_validity)) =
                    self.buffers.get(field)
                {
                    if *had_offsets != buf.offsets_ptr().is_some() {
                        if *had_offsets {
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

                    if *had_validity != buf.validity_ptr().is_some() {
                        if *had_validity {
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
            let mut data_size = Box::pin(buf.data_size());
            let c_data_size = data_size.as_mut().get_mut() as *mut u64;
            self.capi_call(|ctx| unsafe {
                ffi::tiledb_query_set_data_buffer(
                    ctx,
                    c_query,
                    c_name.as_ptr(),
                    buf.data_ptr() as *mut std::ffi::c_void,
                    c_data_size,
                )
            })?;

            let mut offsets_size: Option<Pin<Box<u64>>> = None;
            if let Some(c_offsets) = buf.offsets_ptr() {
                let mut tmp_size = Box::pin(buf.offsets_size().unwrap());
                let c_offsets_size = tmp_size.as_mut().get_mut() as *mut u64;
                self.capi_call(|ctx| unsafe {
                    ffi::tiledb_query_set_offsets_buffer(
                        ctx,
                        c_query,
                        c_name.as_ptr(),
                        c_offsets as *mut u64,
                        c_offsets_size,
                    )
                })?;
                offsets_size = Some(tmp_size);
            }

            let mut validity_size: Option<Pin<Box<u64>>> = None;
            if let Some(c_validity) = buf.validity_ptr() {
                let mut tmp_size = Box::pin(buf.validity_size().unwrap());
                let c_validity_size = tmp_size.as_mut().get_mut() as *mut u64;
                self.capi_call(|ctx| unsafe {
                    ffi::tiledb_query_set_validity_buffer(
                        ctx,
                        c_query,
                        c_name.as_ptr(),
                        c_validity as *mut u8,
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
        })
    }
}

pub struct WriteQueryBuilder {
    query: WriteQuery,
}

impl ContextBound for WriteQueryBuilder {
    fn context(&self) -> &Context {
        self.query.array.context()
    }
}

impl QueryBuilder for WriteQueryBuilder {
    fn context(&self) -> &Context {
        self.query.array.context()
    }

    fn array(&self) -> &Array {
        self.query.array()
    }

    fn capi(&self) -> *mut ffi::tiledb_query_t {
        *self.query.raw
    }
}

impl WriteQueryBuilder {
    pub fn new(array: Array) -> TileDBResult<Self> {
        let c_array = array.capi();
        let c_query_type = QueryType::Read.capi_enum();
        let mut c_query: *mut ffi::tiledb_query_t = out_ptr!();
        array.capi_call(|ctx| unsafe {
            ffi::tiledb_query_alloc(ctx, c_array, c_query_type, &mut c_query)
        })?;

        Ok(Self {
            query: WriteQuery::new(array, RawQuery::Owned(c_query)),
        })
    }

    pub fn build(self) -> WriteQuery {
        self.query
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    use crate::array::Mode;
    use crate::array::{
        ArrayType, AttributeBuilder, DimensionBuilder, DomainBuilder,
        SchemaBuilder,
    };
    use crate::context::Context;
    use crate::query::QueryLayout;
    use crate::Datatype;

    #[test]
    fn basic_write() -> TileDBResult<()> {
        let ctx = Context::new()?;

        // Create a temp array uri
        let dir =
            TempDir::new().map_err(|e| Error::InvalidArgument(anyhow!(e)))?;
        let array_dir = dir.path().join("fragment_info_test_dense");
        let array_uri = String::from(array_dir.to_str().unwrap());

        create_dense_array(&ctx, &array_uri);

        // A basic write of some data to the array.
        let id_data = vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let attr_data = vec![1u64, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        let mut buffers = WriteBufferCollection::new()
            .add_buffer("id", id_data.as_slice())?
            .add_buffer("attr", attr_data.as_slice())?;

        let array = Array::open(&ctx, array_uri, Mode::Write)?;
        let mut query = WriteQueryBuilder::new(array)?
            .layout(QueryLayout::RowMajor)?
            .build();

        query.submit()?;

        Ok(())
    }

    /// Create a simple dense test array
    pub fn create_dense_array(ctx: &Context, uri: &str) -> TileDBResult<()> {
        let domain = {
            let rows = DimensionBuilder::new::<i32>(
                ctx,
                "id",
                Datatype::Int32,
                &[1, 10],
                &4,
            )?
            .build();

            DomainBuilder::new(ctx)?.add_dimension(rows)?.build()
        };

        let schema = SchemaBuilder::new(ctx, ArrayType::Dense, domain)?
            .add_attribute(
                AttributeBuilder::new(ctx, "attr", Datatype::UInt64)?.build(),
            )?
            .build()?;

        Array::create(ctx, uri, schema)
    }
}
