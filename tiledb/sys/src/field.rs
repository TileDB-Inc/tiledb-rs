use crate::capi_enum::{tiledb_datatype_t, tiledb_field_origin_t};
use crate::types::{
    capi_return_t, tiledb_ctx_t, tiledb_query_channel_t, tiledb_query_field_t,
    tiledb_query_t,
};

extern "C" {
    pub fn tiledb_query_get_field(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        field_name: *const ::std::os::raw::c_char,
        field: *mut *mut tiledb_query_field_t,
    ) -> capi_return_t;

    pub fn tiledb_query_field_free(
        ctx: *mut tiledb_ctx_t,
        field: *mut *mut tiledb_query_field_t,
    ) -> capi_return_t;

    pub fn tiledb_field_datatype(
        ctx: *mut tiledb_ctx_t,
        field: *mut tiledb_query_field_t,
        type_: *mut tiledb_datatype_t,
    ) -> capi_return_t;

    pub fn tiledb_field_cell_val_num(
        ctx: *mut tiledb_ctx_t,
        field: *mut tiledb_query_field_t,
        cell_val_num: *mut u32,
    ) -> capi_return_t;

    pub fn tiledb_field_origin(
        ctx: *mut tiledb_ctx_t,
        field: *mut tiledb_query_field_t,
        origin: *mut tiledb_field_origin_t,
    ) -> capi_return_t;

    pub fn tiledb_field_channel(
        ctx: *mut tiledb_ctx_t,
        field: *mut tiledb_query_field_t,
        channel: *mut *mut tiledb_query_channel_t,
    ) -> capi_return_t;
}
