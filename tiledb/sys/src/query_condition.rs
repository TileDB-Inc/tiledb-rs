use crate::capi_enum::{
    tiledb_query_condition_combination_op_t, tiledb_query_condition_op_t,
};
use crate::types::{capi_return_t, tiledb_ctx_t, tiledb_query_condition_t};

unsafe extern "C" {
    pub fn tiledb_query_condition_alloc(
        ctx: *mut tiledb_ctx_t,
        cond: *mut *mut tiledb_query_condition_t,
    ) -> i32;

    pub fn tiledb_query_condition_alloc_set_membership(
        ctx: *mut tiledb_ctx_t,
        field_name: *const ::std::os::raw::c_char,
        data: *const ::std::os::raw::c_void,
        data_size: u64,
        offsets: *const ::std::os::raw::c_void,
        offests_size: u64,
        op: tiledb_query_condition_op_t,
        cond: *mut *mut tiledb_query_condition_t,
    ) -> capi_return_t;

    pub fn tiledb_query_condition_free(
        cond: *mut *mut tiledb_query_condition_t,
    );

    pub fn tiledb_query_condition_init(
        ctx: *mut tiledb_ctx_t,
        cond: *mut tiledb_query_condition_t,
        attribute_name: *const ::std::os::raw::c_char,
        condition_value: *const ::std::os::raw::c_void,
        condition_value_size: u64,
        op: tiledb_query_condition_op_t,
    ) -> i32;

    pub fn tiledb_query_condition_combine(
        ctx: *mut tiledb_ctx_t,
        left_cond: *const tiledb_query_condition_t,
        right_cond: *const tiledb_query_condition_t,
        combination_op: tiledb_query_condition_combination_op_t,
        combined_cond: *mut *mut tiledb_query_condition_t,
    ) -> i32;

    pub fn tiledb_query_condition_negate(
        ctx: *mut tiledb_ctx_t,
        cond: *const tiledb_query_condition_t,
        negated_cond: *mut *mut tiledb_query_condition_t,
    ) -> i32;
}
