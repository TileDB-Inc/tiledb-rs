use crate::capi_enum::{tiledb_object_t, tiledb_query_type_t};
use crate::tiledb_datatype_t;
use crate::types::{
    capi_return_t, tiledb_config_t, tiledb_ctx_t, tiledb_group_t,
    tiledb_string_t,
};

extern "C" {
    pub fn tiledb_group_alloc(
        ctx: *mut tiledb_ctx_t,
        group_uri: *const ::std::ffi::c_char,
        group: *mut *mut tiledb_group_t,
    ) -> capi_return_t;

    pub fn tiledb_group_free(group: *mut *mut tiledb_group_t);

    pub fn tiledb_group_create(
        ctx: *mut tiledb_ctx_t,
        group_uri: *const ::std::ffi::c_char,
    ) -> capi_return_t;

    pub fn tiledb_group_open(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        query_type: tiledb_query_type_t,
    ) -> capi_return_t;

    pub fn tiledb_group_close(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
    ) -> capi_return_t;

    pub fn tiledb_group_set_config(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        config: *mut tiledb_config_t,
    ) -> capi_return_t;

    pub fn tiledb_group_get_config(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        config: *mut *mut tiledb_config_t,
    ) -> capi_return_t;

    pub fn tiledb_group_put_metadata(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        key: *const ::std::ffi::c_char,
        value_type: tiledb_datatype_t,
        value_num: u32,
        value: *const ::std::ffi::c_void,
    ) -> capi_return_t;

    pub fn tiledb_group_delete_group(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        uri: *const ::std::ffi::c_char,
        recursive: u8,
    ) -> i32;

    pub fn tiledb_group_delete_metadata(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        key: *const ::std::ffi::c_char,
    ) -> capi_return_t;

    pub fn tiledb_group_get_metadata(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        key: *const ::std::ffi::c_char,
        value_type: *mut tiledb_datatype_t,
        value_num: *mut u32,
        value: *mut *const ::std::ffi::c_void,
    ) -> capi_return_t;

    pub fn tiledb_group_get_metadata_num(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        num: *mut u64,
    ) -> capi_return_t;

    pub fn tiledb_group_get_metadata_from_index(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        index: u64,
        key: *mut *const ::std::ffi::c_char,
        key_len: *mut u32,
        value_type: *mut tiledb_datatype_t,
        value_num: *mut u32,
        value: *mut *const ::std::ffi::c_void,
    ) -> capi_return_t;

    pub fn tiledb_group_has_metadata_key(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        key: *const ::std::ffi::c_char,
        value_type: *mut tiledb_datatype_t,
        has_key: *mut i32,
    ) -> capi_return_t;

    pub fn tiledb_group_add_member(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        uri: *const ::std::ffi::c_char,
        relative: u8,
        name: *const ::std::ffi::c_char,
    ) -> capi_return_t;

    pub fn tiledb_group_remove_member(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        name_or_uri: *const ::std::ffi::c_char,
    ) -> capi_return_t;

    pub fn tiledb_group_get_member_count(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        count: *mut u64,
    ) -> capi_return_t;

    pub fn tiledb_group_get_member_by_index_v2(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        index: u64,
        uri: *mut *mut tiledb_string_t,
        type_: *mut tiledb_object_t,
        name: *mut *mut tiledb_string_t,
    ) -> capi_return_t;

    pub fn tiledb_group_get_member_by_name_v2(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        name: *const ::std::ffi::c_char,
        uri: *mut *mut tiledb_string_t,
        type_: *mut tiledb_object_t,
    ) -> capi_return_t;

    pub fn tiledb_group_get_is_relative_uri_by_name(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        name: *const ::std::ffi::c_char,
        relative: *mut u8,
    ) -> capi_return_t;

    pub fn tiledb_group_is_open(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        is_open: *mut i32,
    ) -> capi_return_t;

    pub fn tiledb_group_get_uri(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        group_uri: *mut *const ::std::ffi::c_char,
    ) -> capi_return_t;

    pub fn tiledb_group_get_query_type(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        query_type: *mut tiledb_query_type_t,
    ) -> capi_return_t;

    pub fn tiledb_group_dump_str(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        dump_ascii: *mut *mut ::std::ffi::c_char,
        recursive: u8,
    ) -> capi_return_t;

    pub fn tiledb_group_consolidate_metadata(
        ctx: *mut tiledb_ctx_t,
        group_uri: *const ::std::ffi::c_char,
        config: *mut tiledb_config_t,
    ) -> capi_return_t;

    pub fn tiledb_group_vacuum_metadata(
        ctx: *mut tiledb_ctx_t,
        group_uri: *const ::std::ffi::c_char,
        config: *mut tiledb_config_t,
    ) -> capi_return_t;
}
