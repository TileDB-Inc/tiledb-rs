use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use anyhow::anyhow;

use super::buffer::ReadBufferCollection;
use super::sizeinfo::SizeEntry;
use super::status::{QueryStatus, QueryStatusDetails};
use super::traits::{Query, QueryBuilder, QueryInternal};
use super::{QueryType, RawQuery};
use crate::array::{Array, CellValNum, Schema};
use crate::context::{CApiInterface, Context, ContextBound};
use crate::error::{DatatypeErrorKind, Error};
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
        buffers: Rc<RefCell<ReadBufferCollection>>,
    ) -> TileDBResult<ReadQueryResult> {
        let mut sizes: HashMap<String, SizeEntry> = HashMap::new();

        let bufref = buffers.try_borrow_mut().map_err(|e| {
            Error::InvalidArgument(
                anyhow!("The buffers argument is not borrowable.").context(e),
            )
        })?;

        for buffer in bufref.iter() {
            let entry = self.attach_buffer(buffer)?;
            sizes.insert(buffer.name().to_owned(), entry);
        }

        // Ensure that all buffers were provided if this is a resubmission.
        if self.submitted {
            for (field, _) in self.buffers.iter() {
                if !sizes.contains_key(field) {
                    return Err(Error::InvalidArgument(anyhow!(
                        "Missing buffer for field: {}",
                        field
                    )));
                }
            }
        }

        // Set our buffer info for possible resubmission.
        if !self.submitted {
            for (field, sizes) in sizes.iter() {
                let has_offsets = sizes.offsets_size.is_some();
                let has_validity = sizes.validity_size.is_some();
                self.buffers
                    .insert(field.to_owned(), (has_offsets, has_validity));
            }
        }

        // Mark after the first submission to make sure we're consistently
        // providing the same exact buffers for subsequent submissions.
        self.submitted = true;

        let schema = self.array.schema()?;
        let status = self.capi_status()?;
        let details = self.capi_status_details()?;
        Ok(ReadQueryResult::new(
            schema,
            sizes,
            status,
            details,
            buffers.clone(),
        ))
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

pub struct ReadQueryResult {
    schema: Schema,
    sizes: HashMap<String, SizeEntry>,
    status: QueryStatus,
    details: QueryStatusDetails,
    buffers: Rc<RefCell<ReadBufferCollection>>,
}

impl ReadQueryResult {
    pub fn new(
        schema: Schema,
        sizes: HashMap<String, SizeEntry>,
        status: ffi::tiledb_query_status_t,
        details: ffi::tiledb_query_status_details_reason_t,
        buffers: Rc<RefCell<ReadBufferCollection>>,
    ) -> Self {
        Self {
            schema,
            sizes,
            status: QueryStatus::from(status),
            details: QueryStatusDetails::from(details),
            buffers,
        }
    }

    pub fn nresults(&self) -> TileDBResult<u64> {
        if let Some((name, sizes)) = self.sizes.iter().next() {
            let field = self.schema.field(name)?;
            if matches!(field.cell_val_num()?, CellValNum::Var) {
                // Unwrap guaranteed given that the query returned results.
                let nbytes =
                    sizes.offsets_size.as_ref().unwrap().as_ref().get_ref();
                Ok(nbytes / std::mem::size_of::<u64>() as u64)
            } else {
                let nbytes = sizes.data_size.as_ref().get_ref();
                let cvn = u32::from(field.cell_val_num()?);
                let bytes_per = field.datatype()?.size() * cvn as u64;
                Ok(nbytes / bytes_per)
            }
        } else {
            Ok(0)
        }
    }

    pub fn details(&self) -> QueryStatusDetails {
        self.details.clone()
    }

    pub fn completed(&self) -> bool {
        matches!(self.status, QueryStatus::Completed)
    }

    pub fn slices(&mut self) -> TileDBResult<ReadQueryResultSlices> {
        Ok(ReadQueryResultSlices::new(
            &self.schema,
            self.sizes.clone(),
            self.buffers.as_ref().borrow(),
        ))
    }
}

pub struct ReadQueryResultSlices<'result> {
    schema: &'result Schema,
    sizes: HashMap<String, SizeEntry>,
    buffers: Ref<'result, ReadBufferCollection>,
}

impl<'result> ReadQueryResultSlices<'result> {
    pub fn new(
        schema: &'result Schema,
        sizes: HashMap<String, SizeEntry>,
        buffers: Ref<'result, ReadBufferCollection>,
    ) -> Self {
        Self {
            schema,
            sizes,
            buffers,
        }
    }

    pub fn field<T>(&self, name: &str) -> TileDBResult<&'result [T]> {
        let field = self.schema.field(name)?;
        let dtype = field.datatype()?;
        if dtype.is_compatible_type::<T>() {
            return Err(Error::Datatype(DatatypeErrorKind::TypeMismatch {
                user_type: std::any::type_name::<T>(),
                tiledb_type: dtype,
            }));
        }

        let size = self.sizes.get(name);
        if size.is_none() {
            return Err(Error::InvalidArgument(anyhow!(
                "No buffer present for name: {}",
                name
            )));
        }

        let iter_len = *size.unwrap().data_size.as_ref().get_ref();

        for buffer in self.buffers.iter() {
            if buffer.name() == name {
                return Ok(buffer.as_slice(iter_len));
            }
        }

        // This should be an internal error because we've violated internal
        // constraints if we've gotten this far.
        Err(Error::InvalidArgument(anyhow!(
            "No buffer found for field: {}",
            name
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::izip;
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

        // Create our query
        let array = Array::open(&ctx, array_uri, Mode::Read)?;
        let mut query = ReadQueryBuilder::new(array)?
            .layout(QueryLayout::Unordered)?
            .build();

        // Create our buffer collection
        let mut curr_capacity = 1;
        let id_data = vec![0i32; curr_capacity].into_boxed_slice();
        let attr_data = vec![0u64; curr_capacity].into_boxed_slice();

        let buffers = ReadBufferCollection::new();
        buffers
            .borrow_mut()
            .add_buffer("id", id_data)?
            .add_buffer("attr", attr_data)?;

        loop {
            let result = query.submit(buffers.clone())?;
            if result.nresults()? == 0 && result.details().user_buffer_size() {
                // Not enough space in our buffers to make progress so we have
                // to reallocate them with larger storage capacity.
                curr_capacity *= 2;
                let id_data = vec![0i32; curr_capacity].into_boxed_slice();
                let attr_data = vec![0u64; curr_capacity].into_boxed_slice();
                buffers
                    .borrow_mut()
                    .clear()
                    .add_buffer("id", id_data)?
                    .add_buffer("attr", attr_data)?;
                continue;
            }

            let slices = result.slices()?;
            let ids = slices.field::<i32>("id")?;
            let attrs = slices.field::<u64>("attr")?;
            for (id, attr) in izip!(ids, attrs) {
                println!("Id: {} Attr: {}", id, attr);
            }

            if result.completed() {
                break;
            }
        }

        Ok(())
    }
}
