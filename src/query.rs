use std::ops::Deref;

use crate::array::Layout;
use crate::context::Context;
use crate::datatype::DomainType;
use crate::{Array, Result as TileDBResult};

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
        if let RawQuery::Owned(ref mut ffi) = *self {
            unsafe { ffi::tiledb_query_free(ffi) }
        }
    }
}

pub struct Query<'ctx> {
    context: &'ctx Context,
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
        array: Array,
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

    pub fn dimension_buffer_typed<DT: DomainType>(
        self,
        name: &str,
        data: &mut [DT],
    ) -> TileDBResult<Self> {
        let c_context = self.query.context.as_mut_ptr();
        let c_query = *self.query.raw;
        let c_name = cstring!(name);

        let c_bufptr = unsafe {
            std::mem::transmute::<&mut DT, *mut std::ffi::c_void>(&mut data[0])
        };
        let mut c_size: u64 =
            (data.len() * std::mem::size_of::<DT>()).try_into().unwrap();

        let c_ret = unsafe {
            ffi::tiledb_query_set_data_buffer(
                c_context,
                c_query,
                c_name.as_ptr(),
                c_bufptr,
                &mut c_size,
            )
        };
        if c_ret == ffi::TILEDB_OK {
            Ok(self)
        } else {
            Err(self.query.context.expect_last_error())
        }
    }

    pub fn build(self) -> Query<'ctx> {
        self.query
    }
}

impl<'ctx> Into<Query<'ctx>> for Builder<'ctx> {
    fn into(self) -> Query<'ctx> {
        self.build()
    }
}
