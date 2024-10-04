use crate::types::{
    capi_return_t, tiledb_array_schema_t, tiledb_config_t, tiledb_ctx_t,
    tiledb_fragment_info_t, tiledb_string_t,
};

extern "C" {

    pub fn tiledb_fragment_info_alloc(
        ctx: *mut tiledb_ctx_t,
        array_uri: *const ::std::os::raw::c_char,
        fragment_info: *mut *mut tiledb_fragment_info_t,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_free(
        fragment_info: *mut *mut tiledb_fragment_info_t,
    );

    pub fn tiledb_fragment_info_set_config(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        config: *mut tiledb_config_t,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_get_config(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        config: *mut *mut tiledb_config_t,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_load(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_get_fragment_name_v2(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        fid: u32,
        name: *mut *mut tiledb_string_t,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_get_fragment_num(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        fragment_num: *mut u32,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_get_fragment_uri(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        fid: u32,
        uri: *mut *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_get_fragment_size(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        fid: u32,
        size: *mut u64,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_get_dense(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        fid: u32,
        dense: *mut i32,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_get_timestamp_range(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        fid: u32,
        start: *mut u64,
        end: *mut u64,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_get_non_empty_domain_from_index(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        fid: u32,
        did: u32,
        domain: *mut ::std::os::raw::c_void,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_get_non_empty_domain_var_size_from_index(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        fid: u32,
        did: u32,
        start_size: *mut u64,
        end_size: *mut u64,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_get_non_empty_domain_var_from_index(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        fid: u32,
        did: u32,
        start: *mut ::std::os::raw::c_void,
        end: *mut ::std::os::raw::c_void,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_get_mbr_num(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        fid: u32,
        mbr_num: *mut u64,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_get_mbr_from_index(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        fid: u32,
        mid: u32,
        did: u32,
        mbr: *mut ::std::os::raw::c_void,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_get_mbr_var_size_from_index(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        fid: u32,
        mid: u32,
        did: u32,
        start_size: *mut u64,
        end_size: *mut u64,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_get_mbr_var_from_index(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        fid: u32,
        mid: u32,
        did: u32,
        start: *mut ::std::os::raw::c_void,
        end: *mut ::std::os::raw::c_void,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_get_cell_num(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        fid: u32,
        cell_num: *mut u64,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_get_version(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        fid: u32,
        version: *mut u32,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_has_consolidated_metadata(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        fid: u32,
        has: *mut i32,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_get_unconsolidated_metadata_num(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        unconsolidated: *mut u32,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_get_to_vacuum_num(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        to_vacuum_num: *mut u32,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_get_to_vacuum_uri(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        fid: u32,
        uri: *mut *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_get_array_schema(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        fid: u32,
        array_schema: *mut *mut tiledb_array_schema_t,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_get_array_schema_name(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        fid: u32,
        schema_name: *mut *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_fragment_info_get_total_cell_num(
        ctx: *mut tiledb_ctx_t,
        fragment_info: *mut tiledb_fragment_info_t,
        count: *mut u64,
    ) -> capi_return_t;
}
