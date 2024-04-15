use crate::capi_enum::tiledb_datatype_t;
use crate::types::{tiledb_attribute_t, tiledb_ctx_t, tiledb_filter_list_t};

extern "C" {
    pub fn tiledb_attribute_alloc(
        ctx: *mut tiledb_ctx_t,
        name: *const ::std::os::raw::c_char,
        type_: tiledb_datatype_t,
        attr: *mut *mut tiledb_attribute_t,
    ) -> i32;

    pub fn tiledb_attribute_free(attr: *mut *mut tiledb_attribute_t);

    pub fn tiledb_attribute_get_name(
        ctx: *mut tiledb_ctx_t,
        attr: *const tiledb_attribute_t,
        name: *mut *const ::std::os::raw::c_char,
    ) -> i32;

    pub fn tiledb_attribute_get_type(
        ctx: *mut tiledb_ctx_t,
        attr: *const tiledb_attribute_t,
        type_: *mut tiledb_datatype_t,
    ) -> i32;

    pub fn tiledb_attribute_set_nullable(
        ctx: *mut tiledb_ctx_t,
        attr: *mut tiledb_attribute_t,
        nullable: u8,
    ) -> i32;

    pub fn tiledb_attribute_get_nullable(
        ctx: *mut tiledb_ctx_t,
        attr: *mut tiledb_attribute_t,
        nullable: *mut u8,
    ) -> i32;

    pub fn tiledb_attribute_set_filter_list(
        ctx: *mut tiledb_ctx_t,
        attr: *mut tiledb_attribute_t,
        filter_list: *mut tiledb_filter_list_t,
    ) -> i32;

    pub fn tiledb_attribute_get_filter_list(
        ctx: *mut tiledb_ctx_t,
        attr: *mut tiledb_attribute_t,
        filter_list: *mut *mut tiledb_filter_list_t,
    ) -> i32;

    pub fn tiledb_attribute_set_cell_val_num(
        ctx: *mut tiledb_ctx_t,
        attr: *mut tiledb_attribute_t,
        cell_val_num: u32,
    ) -> i32;

    pub fn tiledb_attribute_get_cell_val_num(
        ctx: *mut tiledb_ctx_t,
        attr: *const tiledb_attribute_t,
        cell_val_num: *mut u32,
    ) -> i32;

    pub fn tiledb_attribute_get_cell_size(
        ctx: *mut tiledb_ctx_t,
        attr: *const tiledb_attribute_t,
        cell_size: *mut u64,
    ) -> i32;

    pub fn tiledb_attribute_set_fill_value(
        ctx: *mut tiledb_ctx_t,
        attr: *mut tiledb_attribute_t,
        value: *const ::std::os::raw::c_void,
        size: u64,
    ) -> i32;

    pub fn tiledb_attribute_get_fill_value(
        ctx: *mut tiledb_ctx_t,
        attr: *mut tiledb_attribute_t,
        value: *mut *const ::std::os::raw::c_void,
        size: *mut u64,
    ) -> i32;

    pub fn tiledb_attribute_set_fill_value_nullable(
        ctx: *mut tiledb_ctx_t,
        attr: *mut tiledb_attribute_t,
        value: *const ::std::os::raw::c_void,
        size: u64,
        validity: u8,
    ) -> i32;

    pub fn tiledb_attribute_get_fill_value_nullable(
        ctx: *mut tiledb_ctx_t,
        attr: *mut tiledb_attribute_t,
        value: *mut *const ::std::os::raw::c_void,
        size: *mut u64,
        valid: *mut u8,
    ) -> i32;
}
