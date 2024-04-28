use std::collections::HashMap;

use anyhow::anyhow;

use super::sizeinfo::SizeEntry;
use super::traits::{Query, QueryBuilder, QueryInternal};
use super::RawQuery;
use crate::array::Array;
use crate::context::{CApiInterface, Context, ContextBound};
use crate::error::Error;
use crate::query::buffer::WriteBufferCollection;
use crate::query::QueryType;
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

impl QueryInternal for WriteQuery {
    fn context(&self) -> &Context {
        self.array.context()
    }

    fn capi(&self) -> *mut ffi::tiledb_query_t {
        *self.raw
    }

    fn buffer_info(&self, name: &str) -> Option<(bool, bool)> {
        self.buffers.get(name).copied()
    }

    fn submitted(&self) -> bool {
        self.submitted
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

    pub fn finalize(self) -> TileDBResult<Array> {
        self.do_finalize()?;
        Ok(self.array)
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
        let c_query_type = QueryType::Write.capi_enum();
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
pub mod tests {
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
        let dir =
            TempDir::new().map_err(|e| Error::InvalidArgument(anyhow!(e)))?;
        let array_dir = dir.path().join("fragment_info_test_dense");
        let array_uri = String::from(array_dir.to_str().unwrap());

        create_sparse_array(&ctx, &array_uri)?;
        write_sparse_data(&ctx, &array_uri)?;

        Ok(())
    }

    pub fn write_sparse_data(ctx: &Context, uri: &str) -> TileDBResult<()> {
        // A basic write of some data to the array.
        let id_data = vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let attr_data = vec![1u64, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        let buffers = WriteBufferCollection::new()
            .add_buffer("id", id_data.as_slice())?
            .add_buffer("attr", attr_data.as_slice())?;

        let array = Array::open(ctx, uri, Mode::Write)?;
        let mut query = WriteQueryBuilder::new(array)?
            .layout(QueryLayout::Unordered)?
            .build();

        query.submit(&buffers)?;
        query.finalize()?;

        Ok(())
    }

    /// Create a simple dense test array
    pub fn create_sparse_array(ctx: &Context, uri: &str) -> TileDBResult<()> {
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

        let schema = SchemaBuilder::new(ctx, ArrayType::Sparse, domain)?
            .add_attribute(
                AttributeBuilder::new(ctx, "attr", Datatype::UInt64)?.build(),
            )?
            .build()?;

        Array::create(ctx, uri, schema)
    }
}
