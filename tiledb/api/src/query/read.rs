use std::collections::HashMap;

use anyhow::anyhow;

use super::buffer::ReadBufferCollection;
use super::sizeinfo::SizeEntry;
use super::traits::{Query, QueryBuilder, QueryInternal};
use super::{QueryType, RawQuery};
use crate::array::Array;
use crate::context::{CApiInterface, Context, ContextBound};
use crate::error::Error;
use crate::Result as TileDBResult;

pub struct ReadQuery {
    array: Array,
    raw: RawQuery,
    buffers: HashMap<String, (bool, bool)>,
    submitted: bool,
}

impl Query for ReadQuery {
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

impl QueryInternal for ReadQuery {
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

impl ReadQuery {
    fn new(array: Array, raw: RawQuery) -> Self {
        Self {
            array,
            raw,
            buffers: HashMap::new(),
            submitted: false,
        }
    }

    pub fn submit(
        &mut self,
        buffers: &ReadBufferCollection,
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

pub struct ReadQueryBuilder {
    query: ReadQuery,
}

impl ContextBound for ReadQueryBuilder {
    fn context(&self) -> &Context {
        self.query.array.context()
    }
}

impl QueryBuilder for ReadQueryBuilder {
    fn context(&self) -> &Context {
        ContextBound::context(self)
    }

    fn array(&self) -> &Array {
        self.query.array()
    }

    fn capi(&self) -> *mut ffi::tiledb_query_t {
        *self.query.raw
    }
}

impl ReadQueryBuilder {
    pub fn new(array: Array) -> TileDBResult<Self> {
        let c_array = array.capi();
        let c_query_type = QueryType::Read.capi_enum();
        let mut c_query: *mut ffi::tiledb_query_t = out_ptr!();
        array.capi_call(|ctx| unsafe {
            ffi::tiledb_query_alloc(ctx, c_array, c_query_type, &mut c_query)
        })?;

        Ok(Self {
            query: ReadQuery::new(array, RawQuery::Owned(c_query)),
        })
    }

    pub fn build(self) -> ReadQuery {
        self.query
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    use crate::array::Mode;
    use crate::context::Context;
    use crate::query::QueryLayout;

    #[test]
    fn basic_read() -> TileDBResult<()> {
        let ctx = Context::new()?;

        // Create a temp array uri
        let dir =
            TempDir::new().map_err(|e| Error::InvalidArgument(anyhow!(e)))?;
        let array_dir = dir.path().join("fragment_info_test_dense");
        let array_uri = String::from(array_dir.to_str().unwrap());

        super::super::write::tests::create_sparse_array(&ctx, &array_uri)?;
        super::super::write::tests::write_sparse_data(&ctx, &array_uri)?;

        // A basic write of some data to the array.
        let id_data = vec![0i32; 10].into_boxed_slice();
        let attr_data = vec![0u64; 10].into_boxed_slice();

        let buffers = ReadBufferCollection::new()
            .add_buffer("id", id_data)?
            .add_buffer("attr", attr_data)?;

        let array = Array::open(&ctx, array_uri, Mode::Read)?;
        let mut query = ReadQueryBuilder::new(array)?
            .layout(QueryLayout::Unordered)?
            .build();

        let result = query.submit(&buffers)?;

        println!("Result: {:?}", result);

        Ok(())
    }
}
