use crate::capi_enum::{tiledb_filter_option_t, tiledb_filter_type_t};
use crate::types::{capi_return_t, tiledb_ctx_t, tiledb_filter_t};

unsafe extern "C" {
    pub fn tiledb_filter_alloc(
        ctx: *mut tiledb_ctx_t,
        type_: tiledb_filter_type_t,
        filter: *mut *mut tiledb_filter_t,
    ) -> capi_return_t;

    pub fn tiledb_filter_free(filter: *mut *mut tiledb_filter_t);

    pub fn tiledb_filter_get_type(
        ctx: *mut tiledb_ctx_t,
        filter: *mut tiledb_filter_t,
        type_: *mut tiledb_filter_type_t,
    ) -> capi_return_t;

    pub fn tiledb_filter_set_option(
        ctx: *mut tiledb_ctx_t,
        filter: *mut tiledb_filter_t,
        option: tiledb_filter_option_t,
        value: *const ::std::os::raw::c_void,
    ) -> capi_return_t;

    pub fn tiledb_filter_get_option(
        ctx: *mut tiledb_ctx_t,
        filter: *mut tiledb_filter_t,
        option: tiledb_filter_option_t,
        value: *mut ::std::os::raw::c_void,
    ) -> capi_return_t;
}
