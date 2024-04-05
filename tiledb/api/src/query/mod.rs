pub mod subarray;

use std::collections::HashMap;
use std::ops::Deref;

use crate::context::{CApiInterface, Context, ContextBound};
use crate::convert::CAPIConverter;
use crate::{Array, Result as TileDBResult};

pub use crate::query::subarray::{Builder as SubarrayBuilder, Subarray};

pub type QueryType = crate::array::Mode;
pub type QueryLayout = crate::array::CellOrder;

pub(crate) enum RawQuery {
    Owned(*mut ffi::tiledb_query_t),
}

impl Deref for RawQuery {
    type Target = *mut ffi::tiledb_query_t;
    fn deref(&self) -> &Self::Target {
        match *self {
            RawQuery::Owned(ref ffi) => ffi,
        }
    }
}

impl Drop for RawQuery {
    fn drop(&mut self) {
        let RawQuery::Owned(ref mut ffi) = *self;
        unsafe { ffi::tiledb_query_free(ffi) };
    }
}

#[derive(ContextBound)]
pub struct Query<'ctx> {
    #[context]
    context: &'ctx Context,
    array: Array<'ctx>,
    // This is a bit gross but the buffer sizes must out-live the query.
    // That's very C-like, Rust wants to use slices or something, so we do this
    // in order to pin the size to a fixed address
    result_buffers: HashMap<String, Box<u64>>,
    raw: RawQuery,
}

impl<'ctx> Query<'ctx> {
    // TODO: what should the return type be?
    // if you can re-submit the query then Self makes sense.
    // if not then Array makes more sense
    pub fn submit(self) -> TileDBResult<Self> {
        let c_context = self.context.capi();
        let c_query = *self.raw;
        self.capi_return(unsafe {
            ffi::tiledb_query_submit(c_context, c_query)
        })?;
        Ok(self)
    }
}

#[derive(ContextBound)]
pub struct Builder<'ctx> {
    #[base(ContextBound)]
    query: Query<'ctx>,
}

impl<'ctx> Builder<'ctx> {
    pub fn new(
        context: &'ctx Context,
        array: Array<'ctx>,
        query_type: QueryType,
    ) -> TileDBResult<Self> {
        let c_context = context.capi();
        let c_array = array.capi();
        let c_query_type = query_type.capi_enum();
        let mut c_query: *mut ffi::tiledb_query_t = out_ptr!();
        context.capi_return(unsafe {
            ffi::tiledb_query_alloc(
                c_context,
                c_array,
                c_query_type,
                &mut c_query,
            )
        })?;
        Ok(Builder {
            query: Query {
                context,
                array,
                result_buffers: HashMap::new(),
                raw: RawQuery::Owned(c_query),
            },
        })
    }

    pub fn layout(self, layout: QueryLayout) -> TileDBResult<Self> {
        let c_context = self.query.context.capi();
        let c_query = *self.query.raw;
        let c_layout = layout.capi_enum();
        self.capi_return(unsafe {
            ffi::tiledb_query_set_layout(c_context, c_query, c_layout)
        })?;
        Ok(self)
    }

    pub fn add_subarray(self) -> TileDBResult<SubarrayBuilder<'ctx>> {
        SubarrayBuilder::for_query(self)
    }

    pub fn dimension_buffer_typed<Conv: CAPIConverter>(
        mut self,
        name: &str,
        data: &mut [Conv],
    ) -> TileDBResult<Self> {
        let c_context = self.query.context.capi();
        let c_query = *self.query.raw;
        let c_name = cstring!(name);

        let c_bufptr = &mut data[0] as *mut Conv as *mut std::ffi::c_void;

        let mut c_size =
            Box::new((std::mem::size_of_val(data)).try_into().unwrap());

        // TODO: this is not safe because the C API keeps a pointer to the size
        // and may write back to it

        self.capi_return(unsafe {
            ffi::tiledb_query_set_data_buffer(
                c_context,
                c_query,
                c_name.as_ptr(),
                c_bufptr,
                &mut *c_size,
            )
        })?;
        self.query.result_buffers.insert(String::from(name), c_size);
        Ok(self)
    }

    pub fn build(self) -> Query<'ctx> {
        self.query
    }
}

impl<'ctx> From<Builder<'ctx>> for Query<'ctx> {
    fn from(builder: Builder<'ctx>) -> Query<'ctx> {
        builder.build()
    }
}
