use crate::{
    tiledb_ctx_t, tiledb_datatype_t, tiledb_dimension_t, tiledb_filter_list_t,
};

extern "C" {
    pub fn tiledb_dimension_alloc(
        ctx: *mut tiledb_ctx_t,
        name: *const ::std::os::raw::c_char,
        type_: tiledb_datatype_t,
        dim_domain: *const ::std::os::raw::c_void,
        tile_extent: *const ::std::os::raw::c_void,
        dim: *mut *mut tiledb_dimension_t,
    ) -> i32;

    pub fn tiledb_dimension_free(dim: *mut *mut tiledb_dimension_t);

    pub fn tiledb_dimension_set_filter_list(
        ctx: *mut tiledb_ctx_t,
        dim: *mut tiledb_dimension_t,
        filter_list: *mut tiledb_filter_list_t,
    ) -> i32;

    pub fn tiledb_dimension_set_cell_val_num(
        ctx: *mut tiledb_ctx_t,
        dim: *mut tiledb_dimension_t,
        cell_val_num: u32,
    ) -> i32;

    pub fn tiledb_dimension_get_filter_list(
        ctx: *mut tiledb_ctx_t,
        dim: *mut tiledb_dimension_t,
        filter_list: *mut *mut tiledb_filter_list_t,
    ) -> i32;

    pub fn tiledb_dimension_get_cell_val_num(
        ctx: *mut tiledb_ctx_t,
        dim: *const tiledb_dimension_t,
        cell_val_num: *mut u32,
    ) -> i32;

    pub fn tiledb_dimension_get_name(
        ctx: *mut tiledb_ctx_t,
        dim: *const tiledb_dimension_t,
        name: *mut *const ::std::os::raw::c_char,
    ) -> i32;

    pub fn tiledb_dimension_get_type(
        ctx: *mut tiledb_ctx_t,
        dim: *const tiledb_dimension_t,
        type_: *mut tiledb_datatype_t,
    ) -> i32;

    pub fn tiledb_dimension_get_domain(
        ctx: *mut tiledb_ctx_t,
        dim: *const tiledb_dimension_t,
        domain: *mut *const ::std::os::raw::c_void,
    ) -> i32;

    pub fn tiledb_dimension_get_tile_extent(
        ctx: *mut tiledb_ctx_t,
        dim: *const tiledb_dimension_t,
        tile_extent: *mut *const ::std::os::raw::c_void,
    ) -> i32;
}
