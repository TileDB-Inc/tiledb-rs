pub mod subarray;

use std::collections::HashMap;
use std::ops::Deref;

use crate::array::Layout;
use crate::context::Context;
use crate::datatype::DomainType;
use crate::{Array, Result as TileDBResult};

pub use crate::query::subarray::{Builder as SubarrayBuilder, Subarray};

pub type QueryType = crate::array::Mode;

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

pub struct Query<'ctx> {
    context: &'ctx Context,
    array: Array<'ctx>,
    subarrays: Vec<Subarray<'ctx>>,
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
        let c_context = self.context.as_mut_ptr();
        let c_query = *self.raw;
        let c_ret = unsafe { ffi::tiledb_query_submit(c_context, c_query) };
        if c_ret == ffi::TILEDB_OK {
            Ok(self)
        } else {
            Err(self.context.expect_last_error())
        }
    }
}

pub struct Builder<'ctx> {
    query: Query<'ctx>,
}

impl<'ctx> Builder<'ctx> {
    pub fn new(
        context: &'ctx Context,
        array: Array<'ctx>,
        query_type: QueryType,
    ) -> TileDBResult<Self> {
        let c_context = context.as_mut_ptr();
        let c_array = array.capi();
        let c_query_type = query_type.capi_enum();
        let mut c_query: *mut ffi::tiledb_query_t = out_ptr!();
        let c_ret = unsafe {
            ffi::tiledb_query_alloc(
                c_context,
                c_array,
                c_query_type,
                &mut c_query,
            )
        };
        if c_ret == ffi::TILEDB_OK {
            Ok(Builder {
                query: Query {
                    context,
                    array,
                    subarrays: vec![],
                    result_buffers: HashMap::new(),
                    raw: RawQuery::Owned(c_query),
                },
            })
        } else {
            Err(context.expect_last_error())
        }
    }

    pub fn layout(self, layout: Layout) -> TileDBResult<Self> {
        let c_context = self.query.context.as_mut_ptr();
        let c_query = *self.query.raw;
        let c_layout = layout.capi_enum();
        let c_ret = unsafe {
            ffi::tiledb_query_set_layout(c_context, c_query, c_layout)
        };
        if c_ret == ffi::TILEDB_OK {
            Ok(self)
        } else {
            Err(self.query.context.expect_last_error())
        }
    }

    pub fn add_subarray(self) -> TileDBResult<SubarrayBuilder<'ctx>> {
        SubarrayBuilder::for_query(self)
    }

    pub fn dimension_buffer_typed<DT: DomainType>(
        mut self,
        name: &str,
        data: &mut [DT],
    ) -> TileDBResult<Self> {
        let c_context = self.query.context.as_mut_ptr();
        let c_query = *self.query.raw;
        let c_name = cstring!(name);

        let c_bufptr = &mut data[0] as *mut DT as *mut std::ffi::c_void;

        let mut c_size =
            Box::new((std::mem::size_of_val(data)).try_into().unwrap());

        // TODO: this is not safe because the C API keeps a pointer to the size
        // and may write back to it

        let c_ret = unsafe {
            ffi::tiledb_query_set_data_buffer(
                c_context,
                c_query,
                c_name.as_ptr(),
                c_bufptr,
                &mut *c_size,
            )
        };
        if c_ret == ffi::TILEDB_OK {
            self.query.result_buffers.insert(String::from(name), c_size);
            Ok(self)
        } else {
            Err(self.query.context.expect_last_error())
        }
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
