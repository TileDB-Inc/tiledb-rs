use crate::capi_enum::{tiledb_encryption_type_t, tiledb_query_type_t};
use crate::tiledb_datatype_t;
use crate::types::{
    capi_return_t, tiledb_array_schema_t, tiledb_array_t, tiledb_config_t,
    tiledb_ctx_t, tiledb_enumeration_t,
};

extern "C" {
    pub fn tiledb_array_alloc(
        ctx: *mut tiledb_ctx_t,
        array_uri: *const ::std::os::raw::c_char,
        array: *mut *mut tiledb_array_t,
    ) -> capi_return_t;

    pub fn tiledb_array_free(array: *mut *mut tiledb_array_t);

    pub fn tiledb_array_create(
        ctx: *mut tiledb_ctx_t,
        array_uri: *const ::std::os::raw::c_char,
        array_schema: *const tiledb_array_schema_t,
    ) -> capi_return_t;

    pub fn tiledb_array_open(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        query_type: tiledb_query_type_t,
    ) -> capi_return_t;

    pub fn tiledb_array_is_open(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        is_open: *mut i32,
    ) -> capi_return_t;

    pub fn tiledb_array_reopen(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
    ) -> capi_return_t;

    pub fn tiledb_array_set_config(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        config: *mut tiledb_config_t,
    ) -> capi_return_t;

    pub fn tiledb_array_get_config(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        config: *mut *mut tiledb_config_t,
    ) -> capi_return_t;

    pub fn tiledb_array_close(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
    ) -> capi_return_t;

    pub fn tiledb_array_get_schema(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        array_schema: *mut *mut tiledb_array_schema_t,
    ) -> capi_return_t;

    pub fn tiledb_array_encryption_type(
        ctx: *mut tiledb_ctx_t,
        array_uri: *const ::std::os::raw::c_char,
        encryption_type: *mut tiledb_encryption_type_t,
    ) -> i32;

    pub fn tiledb_array_put_metadata(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        key: *const ::std::os::raw::c_char,
        value_type: tiledb_datatype_t,
        value_num: u32,
        value: *const ::std::os::raw::c_void,
    ) -> capi_return_t;

    pub fn tiledb_array_delete_metadata(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        key: *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_array_get_metadata(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        key: *const ::std::os::raw::c_char,
        value_type: *mut tiledb_datatype_t,
        value_num: *mut u32,
        value: *mut *const ::std::os::raw::c_void,
    ) -> capi_return_t;

    pub fn tiledb_array_get_metadata_num(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        num: *mut u64,
    ) -> capi_return_t;

    pub fn tiledb_array_get_metadata_from_index(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        index: u64,
        key: *mut *const ::std::os::raw::c_char,
        key_len: *mut u32,
        value_type: *mut tiledb_datatype_t,
        value_num: *mut u32,
        value: *mut *const ::std::os::raw::c_void,
    ) -> capi_return_t;

    pub fn tiledb_array_has_metadata_key(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        key: *const ::std::os::raw::c_char,
        value_type: *mut tiledb_datatype_t,
        has_key: *mut i32,
    ) -> capi_return_t;

    pub fn tiledb_array_get_non_empty_domain(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        domain: *mut ::std::os::raw::c_void,
        is_empty: *mut i32,
    ) -> i32;

    pub fn tiledb_array_get_non_empty_domain_from_index(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        idx: u32,
        domain: *mut ::std::os::raw::c_void,
        is_empty: *mut i32,
    ) -> capi_return_t;

    pub fn tiledb_array_get_non_empty_domain_from_name(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        name: *const ::std::os::raw::c_char,
        domain: *mut ::std::os::raw::c_void,
        is_empty: *mut i32,
    ) -> capi_return_t;

    pub fn tiledb_array_get_non_empty_domain_var_size_from_index(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        idx: u32,
        start_size: *mut u64,
        end_size: *mut u64,
        is_empty: *mut i32,
    ) -> capi_return_t;

    pub fn tiledb_array_get_non_empty_domain_var_size_from_name(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        name: *const ::std::os::raw::c_char,
        start_size: *mut u64,
        end_size: *mut u64,
        is_empty: *mut i32,
    ) -> capi_return_t;

    pub fn tiledb_array_get_non_empty_domain_var_from_index(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        idx: u32,
        start: *mut ::std::os::raw::c_void,
        end: *mut ::std::os::raw::c_void,
        is_empty: *mut i32,
    ) -> capi_return_t;

    pub fn tiledb_array_get_non_empty_domain_var_from_name(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        name: *const ::std::os::raw::c_char,
        start: *mut ::std::os::raw::c_void,
        end: *mut ::std::os::raw::c_void,
        is_empty: *mut i32,
    ) -> capi_return_t;

    pub fn tiledb_array_set_open_timestamp_start(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        timestamp_start: u64,
    ) -> capi_return_t;

    pub fn tiledb_array_set_open_timestamp_end(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        timestamp_end: u64,
    ) -> capi_return_t;

    pub fn tiledb_array_get_open_timestamp_start(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        timestamp_start: *mut u64,
    ) -> capi_return_t;

    pub fn tiledb_array_get_open_timestamp_end(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        timestamp_end: *mut u64,
    ) -> capi_return_t;

    pub fn tiledb_array_vacuum(
        ctx: *mut tiledb_ctx_t,
        array_uri: *const ::std::os::raw::c_char,
        config: *mut tiledb_config_t,
    ) -> capi_return_t;

    pub fn tiledb_array_upgrade_version(
        ctx: *mut tiledb_ctx_t,
        array_uri: *const ::std::os::raw::c_char,
        config: *mut tiledb_config_t,
    ) -> capi_return_t;

    pub fn tiledb_array_consolidate(
        ctx: *mut tiledb_ctx_t,
        array_uri: *const ::std::os::raw::c_char,
        config: *mut tiledb_config_t,
    ) -> capi_return_t;

    pub fn tiledb_array_consolidate_fragments(
        ctx: *mut tiledb_ctx_t,
        array_uri: *const ::std::os::raw::c_char,
        fragment_uris: *mut *const ::std::os::raw::c_char,
        num_fragments: u64,
        config: *mut tiledb_config_t,
    ) -> capi_return_t;

    pub fn tiledb_array_delete(
        ctx: *mut tiledb_ctx_t,
        uri: *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_array_get_enumeration(
        ctx: *mut tiledb_ctx_t,
        array: *const tiledb_array_t,
        name: *const ::std::os::raw::c_char,
        enumeration: *mut *mut tiledb_enumeration_t,
    ) -> capi_return_t;
}
