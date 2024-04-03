use crate::capi_enum::tiledb_query_type_t;
use crate::types::{
    tiledb_array_schema_t, tiledb_array_t, tiledb_config_t, tiledb_ctx_t,
};

extern "C" {
    pub fn tiledb_array_alloc(
        ctx: *mut tiledb_ctx_t,
        array_uri: *const ::std::os::raw::c_char,
        array: *mut *mut tiledb_array_t,
    ) -> i32;

    pub fn tiledb_array_free(array: *mut *mut tiledb_array_t);

    pub fn tiledb_array_create(
        ctx: *mut tiledb_ctx_t,
        array_uri: *const ::std::os::raw::c_char,
        array_schema: *const tiledb_array_schema_t,
    ) -> i32;

    pub fn tiledb_array_open(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        query_type: tiledb_query_type_t,
    ) -> i32;

    pub fn tiledb_array_is_open(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        is_open: *mut i32,
    ) -> i32;

    pub fn tiledb_array_reopen(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
    ) -> i32;

    pub fn tiledb_array_set_config(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        config: *mut tiledb_config_t,
    ) -> i32;

    pub fn tiledb_array_get_config(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        config: *mut *mut tiledb_config_t,
    ) -> i32;

    pub fn tiledb_array_close(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
    ) -> i32;
}
