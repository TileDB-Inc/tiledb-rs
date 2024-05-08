use crate::capi_enum::{
    tiledb_layout_t, tiledb_query_status_t, tiledb_query_type_t,
};
use crate::types::{
    tiledb_array_t, tiledb_config_t, tiledb_ctx_t, tiledb_query_condition_t,
    tiledb_query_t, tiledb_subarray_t,
};

extern "C" {
    pub fn tiledb_query_alloc(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        query_type: tiledb_query_type_t,
        query: *mut *mut tiledb_query_t,
    ) -> i32;

    pub fn tiledb_query_free(query: *mut *mut tiledb_query_t);

    pub fn tiledb_query_get_stats(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        stats_json: *mut *mut ::std::os::raw::c_char,
    ) -> i32;

    pub fn tiledb_query_set_config(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        config: *mut tiledb_config_t,
    ) -> i32;

    pub fn tiledb_query_get_config(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        config: *mut *mut tiledb_config_t,
    ) -> i32;

    pub fn tiledb_query_set_subarray(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        subarray: *const ::std::os::raw::c_void,
    ) -> i32;

    pub fn tiledb_query_set_subarray_t(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        subarray: *const tiledb_subarray_t,
    ) -> i32;

    pub fn tiledb_query_set_data_buffer(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        name: *const ::std::os::raw::c_char,
        buffer: *mut ::std::os::raw::c_void,
        buffer_size: *mut u64,
    ) -> i32;

    pub fn tiledb_query_set_offsets_buffer(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        name: *const ::std::os::raw::c_char,
        buffer: *mut u64,
        buffer_size: *mut u64,
    ) -> i32;

    pub fn tiledb_query_set_validity_buffer(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        name: *const ::std::os::raw::c_char,
        buffer: *mut u8,
        buffer_size: *mut u64,
    ) -> i32;

    pub fn tiledb_query_get_data_buffer(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        name: *const ::std::os::raw::c_char,
        buffer: *mut *mut ::std::os::raw::c_void,
        buffer_size: *mut *mut u64,
    ) -> i32;

    pub fn tiledb_query_get_offsets_buffer(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        name: *const ::std::os::raw::c_char,
        buffer: *mut *mut u64,
        buffer_size: *mut *mut u64,
    ) -> i32;

    pub fn tiledb_query_get_validity_buffer(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        name: *const ::std::os::raw::c_char,
        buffer: *mut *mut u8,
        buffer_size: *mut *mut u64,
    ) -> i32;

    pub fn tiledb_query_set_layout(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        layout: tiledb_layout_t,
    ) -> i32;

    pub fn tiledb_query_set_condition(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        cond: *const tiledb_query_condition_t,
    ) -> i32;

    pub fn tiledb_query_finalize(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
    ) -> i32;

    pub fn tiledb_query_submit_and_finalize(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
    ) -> i32;

    pub fn tiledb_query_submit(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
    ) -> i32;

    pub fn tiledb_query_has_results(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        has_results: *mut i32,
    ) -> i32;

    pub fn tiledb_query_get_status(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        status: *mut tiledb_query_status_t,
    ) -> i32;

    pub fn tiledb_query_get_type(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        query_type: *mut tiledb_query_type_t,
    ) -> i32;

    pub fn tiledb_query_get_layout(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        query_layout: *mut tiledb_layout_t,
    ) -> i32;

    pub fn tiledb_query_get_array(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        array: *mut *mut tiledb_array_t,
    ) -> i32;

    pub fn tiledb_query_get_est_result_size(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        name: *const ::std::os::raw::c_char,
        size: *mut u64,
    ) -> i32;

    pub fn tiledb_query_get_est_result_size_var(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        name: *const ::std::os::raw::c_char,
        size_off: *mut u64,
        size_val: *mut u64,
    ) -> i32;

    pub fn tiledb_query_get_est_result_size_nullable(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        name: *const ::std::os::raw::c_char,
        size_val: *mut u64,
        size_validity: *mut u64,
    ) -> i32;

    pub fn tiledb_query_get_est_result_size_var_nullable(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        name: *const ::std::os::raw::c_char,
        size_off: *mut u64,
        size_val: *mut u64,
        size_validity: *mut u64,
    ) -> i32;

    pub fn tiledb_query_get_fragment_num(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        num: *mut u32,
    ) -> i32;

    pub fn tiledb_query_get_fragment_uri(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        idx: u64,
        uri: *mut *const ::std::os::raw::c_char,
    ) -> i32;

    pub fn tiledb_query_get_fragment_timestamp_range(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        idx: u64,
        t1: *mut u64,
        t2: *mut u64,
    ) -> i32;

    pub fn tiledb_query_get_subarray_t(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        subarray: *mut *mut tiledb_subarray_t,
    ) -> i32;
}
