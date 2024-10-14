use crate::types::{
    capi_return_t, tiledb_array_t, tiledb_config_t, tiledb_ctx_t,
    tiledb_subarray_t,
};

extern "C" {
    pub fn tiledb_subarray_alloc(
        ctx: *mut tiledb_ctx_t,
        array: *const tiledb_array_t,
        subarray: *mut *mut tiledb_subarray_t,
    ) -> capi_return_t;

    pub fn tiledb_subarray_free(subarray: *mut *mut tiledb_subarray_t);

    pub fn tiledb_subarray_set_config(
        ctx: *mut tiledb_ctx_t,
        subarray: *mut tiledb_subarray_t,
        config: *mut tiledb_config_t,
    ) -> capi_return_t;

    pub fn tiledb_subarray_set_coalesce_ranges(
        ctx: *mut tiledb_ctx_t,
        subarray: *mut tiledb_subarray_t,
        coalesce_ranges: ::std::os::raw::c_int,
    ) -> capi_return_t;

    pub fn tiledb_subarray_set_subarray(
        ctx: *mut tiledb_ctx_t,
        subarray: *mut tiledb_subarray_t,
        subarray_v: *const ::std::os::raw::c_void,
    ) -> capi_return_t;

    pub fn tiledb_subarray_add_range(
        ctx: *mut tiledb_ctx_t,
        subarray: *mut tiledb_subarray_t,
        dim_idx: u32,
        start: *const ::std::os::raw::c_void,
        end: *const ::std::os::raw::c_void,
        stride: *const ::std::os::raw::c_void,
    ) -> capi_return_t;

    pub fn tiledb_subarray_add_range_by_name(
        ctx: *mut tiledb_ctx_t,
        subarray: *mut tiledb_subarray_t,
        dim_name: *const ::std::os::raw::c_char,
        start: *const ::std::os::raw::c_void,
        end: *const ::std::os::raw::c_void,
        stride: *const ::std::os::raw::c_void,
    ) -> capi_return_t;

    pub fn tiledb_subarray_add_range_var(
        ctx: *mut tiledb_ctx_t,
        subarray: *mut tiledb_subarray_t,
        dim_idx: u32,
        start: *const ::std::os::raw::c_void,
        start_size: u64,
        end: *const ::std::os::raw::c_void,
        end_size: u64,
    ) -> capi_return_t;

    pub fn tiledb_subarray_add_range_var_by_name(
        ctx: *mut tiledb_ctx_t,
        subarray: *mut tiledb_subarray_t,
        dim_name: *const ::std::os::raw::c_char,
        start: *const ::std::os::raw::c_void,
        start_size: u64,
        end: *const ::std::os::raw::c_void,
        end_size: u64,
    ) -> capi_return_t;

    pub fn tiledb_subarray_add_point_ranges(
        ctx: *mut tiledb_ctx_t,
        subarray: *mut tiledb_subarray_t,
        dim_idx: u32,
        start: *const ::std::os::raw::c_void,
        count: u64,
    ) -> capi_return_t;

    pub fn tiledb_subarray_get_range_num(
        ctx: *mut tiledb_ctx_t,
        subarray: *const tiledb_subarray_t,
        dim_idx: u32,
        range_num: *mut u64,
    ) -> capi_return_t;

    pub fn tiledb_subarray_get_range(
        ctx: *mut tiledb_ctx_t,
        subarray: *const tiledb_subarray_t,
        dim_idx: u32,
        range_idx: u64,
        start: *mut *const ::std::os::raw::c_void,
        end: *mut *const ::std::os::raw::c_void,
        stride: *mut *const ::std::os::raw::c_void,
    ) -> capi_return_t;

    pub fn tiledb_subarray_get_range_var_size(
        ctx: *mut tiledb_ctx_t,
        subarray: *const tiledb_subarray_t,
        dim_idx: u32,
        range_idx: u64,
        start_size: *mut u64,
        end_size: *mut u64,
    ) -> capi_return_t;

    pub fn tiledb_subarray_get_range_var(
        ctx: *mut tiledb_ctx_t,
        subarray: *const tiledb_subarray_t,
        dim_idx: u32,
        range_idx: u64,
        start: *mut ::std::os::raw::c_void,
        end: *mut ::std::os::raw::c_void,
    ) -> capi_return_t;
}
