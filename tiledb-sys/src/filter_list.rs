use crate::types::capi_return_t;
use crate::types::tiledb_ctx_t;
use crate::types::tiledb_filter_list_t;
use crate::types::tiledb_filter_t;

extern "C" {
    pub fn tiledb_filter_list_alloc(
        ctx: *mut tiledb_ctx_t,
        filter_list: *mut *mut tiledb_filter_list_t,
    ) -> capi_return_t;

    pub fn tiledb_filter_list_free(filter_list: *mut *mut tiledb_filter_list_t);

    pub fn tiledb_filter_list_add_filter(
        ctx: *mut tiledb_ctx_t,
        filter_list: *mut tiledb_filter_list_t,
        filter: *mut tiledb_filter_t,
    ) -> capi_return_t;

    pub fn tiledb_filter_list_set_max_chunk_size(
        ctx: *mut tiledb_ctx_t,
        filter_list: *mut tiledb_filter_list_t,
        max_chunk_size: u32,
    ) -> capi_return_t;

    pub fn tiledb_filter_list_get_nfilters(
        ctx: *mut tiledb_ctx_t,
        filter_list: *const tiledb_filter_list_t,
        nfilters: *mut u32,
    ) -> capi_return_t;

    pub fn tiledb_filter_list_get_filter_from_index(
        ctx: *mut tiledb_ctx_t,
        filter_list: *const tiledb_filter_list_t,
        index: u32,
        filter: *mut *mut tiledb_filter_t,
    ) -> capi_return_t;

    pub fn tiledb_filter_list_get_max_chunk_size(
        ctx: *mut tiledb_ctx_t,
        filter_list: *const tiledb_filter_list_t,
        max_chunk_size: *mut u32,
    ) -> capi_return_t;
}
