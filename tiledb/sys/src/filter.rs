use crate::types::capi_return_t;
use crate::types::tiledb_ctx_t;
use crate::types::tiledb_filter_t;

extern "C" {
    pub fn tiledb_filter_alloc(
        ctx: *mut tiledb_ctx_t,
        type_: u32,
        filter: *mut *mut tiledb_filter_t,
    ) -> capi_return_t;

    pub fn tiledb_filter_free(filter: *mut *mut tiledb_filter_t);

    pub fn tiledb_filter_get_type(
        ctx: *mut tiledb_ctx_t,
        filter: *mut tiledb_filter_t,
        type_: *mut u32,
    ) -> capi_return_t;

    pub fn tiledb_filter_set_option(
        ctx: *mut tiledb_ctx_t,
        filter: *mut tiledb_filter_t,
        option: u32,
        value: *const ::std::os::raw::c_void,
    ) -> capi_return_t;

    pub fn tiledb_filter_get_option(
        ctx: *mut tiledb_ctx_t,
        filter: *mut tiledb_filter_t,
        option: u32,
        value: *mut ::std::os::raw::c_void,
    ) -> capi_return_t;
}
