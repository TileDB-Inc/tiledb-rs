use crate::capi_enum::{
    tiledb_array_type_t, tiledb_encryption_type_t, tiledb_layout_t,
};
use crate::types::{
    tiledb_array_schema_t, tiledb_attribute_t, tiledb_ctx_t, tiledb_domain_t,
    tiledb_filter_list_t,
};

extern "C" {
    pub fn tiledb_array_schema_alloc(
        ctx: *mut tiledb_ctx_t,
        array_type: tiledb_array_type_t,
        array_schema: *mut *mut tiledb_array_schema_t,
    ) -> i32;

    pub fn tiledb_array_schema_free(
        array_schema: *mut *mut tiledb_array_schema_t,
    );

    pub fn tiledb_array_schema_add_attribute(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        attr: *mut tiledb_attribute_t,
    ) -> i32;

    pub fn tiledb_array_schema_set_allows_dups(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        allows_dups: ::std::os::raw::c_int,
    ) -> i32;

    pub fn tiledb_array_schema_get_allows_dups(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        allows_dups: *mut ::std::os::raw::c_int,
    ) -> i32;

    pub fn tiledb_array_schema_get_version(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        version: *mut u32,
    ) -> i32;

    pub fn tiledb_array_schema_set_domain(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        domain: *mut tiledb_domain_t,
    ) -> i32;

    pub fn tiledb_array_schema_set_capacity(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        capacity: u64,
    ) -> i32;

    pub fn tiledb_array_schema_set_cell_order(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        cell_order: tiledb_layout_t,
    ) -> i32;

    pub fn tiledb_array_schema_set_tile_order(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        tile_order: tiledb_layout_t,
    ) -> i32;

    pub fn tiledb_array_schema_set_coords_filter_list(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        filter_list: *mut tiledb_filter_list_t,
    ) -> i32;

    pub fn tiledb_array_schema_set_offsets_filter_list(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        filter_list: *mut tiledb_filter_list_t,
    ) -> i32;

    pub fn tiledb_array_schema_set_validity_filter_list(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        filter_list: *mut tiledb_filter_list_t,
    ) -> i32;

    pub fn tiledb_array_schema_check(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
    ) -> i32;

    pub fn tiledb_array_schema_load(
        ctx: *mut tiledb_ctx_t,
        array_uri: *const ::std::os::raw::c_char,
        array_schema: *mut *mut tiledb_array_schema_t,
    ) -> i32;

    pub fn tiledb_array_schema_load_with_key(
        ctx: *mut tiledb_ctx_t,
        array_uri: *const ::std::os::raw::c_char,
        encryption_type: tiledb_encryption_type_t,
        encryption_key: *const ::std::os::raw::c_void,
        key_length: u32,
        array_schema: *mut *mut tiledb_array_schema_t,
    ) -> i32;

    pub fn tiledb_array_schema_get_array_type(
        ctx: *mut tiledb_ctx_t,
        array_schema: *const tiledb_array_schema_t,
        array_type: *mut tiledb_array_type_t,
    ) -> i32;

    pub fn tiledb_array_schema_get_capacity(
        ctx: *mut tiledb_ctx_t,
        array_schema: *const tiledb_array_schema_t,
        capacity: *mut u64,
    ) -> i32;

    pub fn tiledb_array_schema_get_cell_order(
        ctx: *mut tiledb_ctx_t,
        array_schema: *const tiledb_array_schema_t,
        cell_order: *mut tiledb_layout_t,
    ) -> i32;

    pub fn tiledb_array_schema_get_coords_filter_list(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        filter_list: *mut *mut tiledb_filter_list_t,
    ) -> i32;

    pub fn tiledb_array_schema_get_offsets_filter_list(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        filter_list: *mut *mut tiledb_filter_list_t,
    ) -> i32;

    pub fn tiledb_array_schema_get_validity_filter_list(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        filter_list: *mut *mut tiledb_filter_list_t,
    ) -> i32;

    pub fn tiledb_array_schema_get_domain(
        ctx: *mut tiledb_ctx_t,
        array_schema: *const tiledb_array_schema_t,
        domain: *mut *mut tiledb_domain_t,
    ) -> i32;

    pub fn tiledb_array_schema_get_tile_order(
        ctx: *mut tiledb_ctx_t,
        array_schema: *const tiledb_array_schema_t,
        tile_order: *mut tiledb_layout_t,
    ) -> i32;

    pub fn tiledb_array_schema_get_attribute_num(
        ctx: *mut tiledb_ctx_t,
        array_schema: *const tiledb_array_schema_t,
        attribute_num: *mut u32,
    ) -> i32;

    pub fn tiledb_array_schema_get_attribute_from_index(
        ctx: *mut tiledb_ctx_t,
        array_schema: *const tiledb_array_schema_t,
        index: u32,
        attr: *mut *mut tiledb_attribute_t,
    ) -> i32;

    pub fn tiledb_array_schema_get_attribute_from_name(
        ctx: *mut tiledb_ctx_t,
        array_schema: *const tiledb_array_schema_t,
        name: *const ::std::os::raw::c_char,
        attr: *mut *mut tiledb_attribute_t,
    ) -> i32;

    pub fn tiledb_array_schema_has_attribute(
        ctx: *mut tiledb_ctx_t,
        array_schema: *const tiledb_array_schema_t,
        name: *const ::std::os::raw::c_char,
        has_attr: *mut i32,
    ) -> i32;
}
