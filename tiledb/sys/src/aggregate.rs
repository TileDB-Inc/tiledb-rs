use crate::types::{tiledb_channel_operation_t, tiledb_channel_operator_t, tiledb_ctx_t, tiledb_query_channel_t, tiledb_query_t, capi_return_t};

extern "C" {
    pub fn tiledb_channel_operator_sum_get(
        ctx: *mut tiledb_ctx_t,
        op: *mut *const tiledb_channel_operator_t,
    ) -> i32;

    pub fn tiledb_channel_operator_min_get(
        ctx: *mut tiledb_ctx_t,
        op: *mut *const tiledb_channel_operator_t,
    ) -> i32;

    pub fn tiledb_channel_operator_max_get(
        ctx: *mut tiledb_ctx_t,
        op: *mut *const tiledb_channel_operator_t,
    ) -> i32;

    pub fn tiledb_aggregate_count_get(
        ctx: *mut tiledb_ctx_t,
        operation: *mut *const tiledb_channel_operation_t,
    ) -> i32;

    pub fn tiledb_channel_operator_mean_get(
        ctx: *mut tiledb_ctx_t,
        op: *mut *const tiledb_channel_operator_t,
    ) -> i32;
    
    pub fn tiledb_channel_operator_null_count_get(
        ctx: *mut tiledb_ctx_t,
        op: *mut *const tiledb_channel_operator_t,
    ) -> i32;
    
    pub fn tiledb_query_get_default_channel(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        channel: *mut *mut tiledb_query_channel_t,
    ) -> i32;
    
    pub fn tiledb_create_unary_aggregate(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        op: *const tiledb_channel_operator_t,
        input_field_name: *const ::std::os::raw::c_char,
        operation: *mut *mut tiledb_channel_operation_t,
    ) -> i32;
    
    pub fn tiledb_channel_apply_aggregate(
        ctx: *mut tiledb_ctx_t,
        channel: *mut tiledb_query_channel_t,
        output_field_name: *const ::std::os::raw::c_char,
        operation: *const tiledb_channel_operation_t,
    ) -> i32;
    
    pub fn tiledb_aggregate_free(
        ctx: *mut tiledb_ctx_t,
        op: *mut *mut tiledb_channel_operation_t,
    ) -> capi_return_t;
    
    pub fn tiledb_query_channel_free(
        ctx: *mut tiledb_ctx_t,
        channel: *mut *mut tiledb_query_channel_t,
    ) -> capi_return_t;
}