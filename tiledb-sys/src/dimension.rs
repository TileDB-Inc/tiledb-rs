use crate::{
    tiledb_ctx_t, tiledb_datatype_t, tiledb_dimension_t, tiledb_filter_list_t,
};

extern "C" {
    #[doc = " Creates a dimension.\n\n **Example:**\n\n @code{.c}\n tiledb_dimension_t* dim;\n int64_t dim_domain[] = {1, 10};\n int64_t tile_extent = 5;\n tiledb_dimension_alloc(\n     ctx, \"dim_0\", TILEDB_INT64, dim_domain, &tile_extent, &dim);\n @endcode\n\n Note: as laid out in the Storage Format,\n the following Datatypes are not valid for Dimension:\n TILEDB_CHAR, TILEDB_BLOB, TILEDB_GEOM_WKB, TILEDB_GEOM_WKT, TILEDB_BOOL,\n TILEDB_STRING_UTF8, TILEDB_STRING_UTF16, TILEDB_STRING_UTF32,\n TILEDB_STRING_UCS2, TILEDB_STRING_UCS4, TILEDB_ANY\n\n @param ctx The TileDB context.\n @param name The dimension name.\n @param type The dimension type.\n @param dim_domain The dimension domain.\n @param tile_extent The dimension tile extent.\n @param dim The dimension to be created.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_dimension_alloc(
        ctx: *mut tiledb_ctx_t,
        name: *const ::std::os::raw::c_char,
        type_: tiledb_datatype_t,
        dim_domain: *const ::std::os::raw::c_void,
        tile_extent: *const ::std::os::raw::c_void,
        dim: *mut *mut tiledb_dimension_t,
    ) -> i32;
    #[doc = " Destroys a TileDB dimension, freeing associated memory.\n\n **Example:**\n\n @code{.c}\n tiledb_dimension_free(&dim);\n @endcode\n\n @param dim The dimension to be destroyed."]
    pub fn tiledb_dimension_free(dim: *mut *mut tiledb_dimension_t);
    #[doc = " Sets the filter list for a dimension.\n\n **Example:**\n\n @code{.c}\n tiledb_filter_list_t* filter_list;\n tiledb_filter_list_alloc(ctx, &filter_list);\n tiledb_filter_list_add_filter(ctx, filter_list, filter);\n tiledb_dimension_set_filter_list(ctx, dim, filter_list);\n @endcode\n\n @param ctx The TileDB context.\n @param dim The target dimension.\n @param filter_list The filter_list to be set.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_dimension_set_filter_list(
        ctx: *mut tiledb_ctx_t,
        dim: *mut tiledb_dimension_t,
        filter_list: *mut tiledb_filter_list_t,
    ) -> i32;
    #[doc = " Sets the number of values per cell for a dimension. If this is not\n used, the default is `1`.\n\n **Examples:**\n\n For a fixed-sized dimension:\n\n @code{.c}\n tiledb_dimension_set_cell_val_num(ctx, dim, 3);\n @endcode\n\n For a variable-sized dimension:\n\n @code{.c}\n tiledb_dimension_set_cell_val_num(ctx, dim, TILEDB_VAR_NUM);\n @endcode\n\n @param ctx The TileDB context.\n @param dim The target dimension.\n @param cell_val_num The number of values per cell.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_dimension_set_cell_val_num(
        ctx: *mut tiledb_ctx_t,
        dim: *mut tiledb_dimension_t,
        cell_val_num: u32,
    ) -> i32;
    #[doc = " Retrieves the filter list for a dimension.\n\n **Example:**\n\n @code{.c}\n tiledb_filter_list_t* filter_list;\n tiledb_dimension_get_filter_list(ctx, dim, &filter_list);\n tiledb_filter_list_free(&filter_list);\n @endcode\n\n @param ctx The TileDB context.\n @param dim The target dimension.\n @param filter_list The filter list to be retrieved.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_dimension_get_filter_list(
        ctx: *mut tiledb_ctx_t,
        dim: *mut tiledb_dimension_t,
        filter_list: *mut *mut tiledb_filter_list_t,
    ) -> i32;
    #[doc = " Retrieves the number of values per cell for a dimension. For variable-sized\n dimensions the result is TILEDB_VAR_NUM.\n\n **Example:**\n\n @code{.c}\n uint32_t num;\n tiledb_dimension_get_cell_val_num(ctx, dim, &num);\n @endcode\n\n @param ctx The TileDB context.\n @param dim The dimension.\n @param cell_val_num The number of values per cell to be retrieved.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_dimension_get_cell_val_num(
        ctx: *mut tiledb_ctx_t,
        dim: *const tiledb_dimension_t,
        cell_val_num: *mut u32,
    ) -> i32;
    #[doc = " Retrieves the dimension name.\n\n **Example:**\n\n @code{.c}\n const char* dim_name;\n tiledb_dimension_get_name(ctx, dim, &dim_name);\n @endcode\n\n @param ctx The TileDB context.\n @param dim The dimension.\n @param name The name to be retrieved.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_dimension_get_name(
        ctx: *mut tiledb_ctx_t,
        dim: *const tiledb_dimension_t,
        name: *mut *const ::std::os::raw::c_char,
    ) -> i32;
    #[doc = " Retrieves the dimension type.\n\n **Example:**\n\n @code{.c}\n tiledb_datatype_t dim_type;\n tiledb_dimension_get_type(ctx, dim, &dim_type);\n @endcode\n\n @param ctx The TileDB context.\n @param dim The dimension.\n @param type The type to be retrieved.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_dimension_get_type(
        ctx: *mut tiledb_ctx_t,
        dim: *const tiledb_dimension_t,
        type_: *mut tiledb_datatype_t,
    ) -> i32;
    #[doc = " Retrieves the domain of the dimension.\n\n **Example:**\n\n @code{.c}\n uint64_t* domain;\n tiledb_dimension_get_domain(ctx, dim, &domain);\n @endcode\n\n @param ctx The TileDB context.\n @param dim The dimension.\n @param domain The domain to be retrieved. Note that the defined type of\n     input `domain` must be the same as the dimension type, otherwise the\n     behavior is unpredictable (it will probably segfault).\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_dimension_get_domain(
        ctx: *mut tiledb_ctx_t,
        dim: *const tiledb_dimension_t,
        domain: *mut *const ::std::os::raw::c_void,
    ) -> i32;
    #[doc = " Retrieves the tile extent of the dimension.\n\n **Example:**\n\n @code{.c}\n uint64_t* tile_extent;\n tiledb_dimension_get_tile_extent(ctx, dim, &tile_extent);\n @endcode\n\n @param ctx The TileDB context.\n @param dim The dimension.\n @param tile_extent The tile extent (pointer) to be retrieved.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_dimension_get_tile_extent(
        ctx: *mut tiledb_ctx_t,
        dim: *const tiledb_dimension_t,
        tile_extent: *mut *const ::std::os::raw::c_void,
    ) -> i32;
}
