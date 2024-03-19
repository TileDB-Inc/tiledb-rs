use crate::capi_enum::tiledb_query_type_t;
use crate::types::{
    tiledb_array_schema_t, tiledb_array_t, tiledb_config_t, tiledb_ctx_t,
};

extern "C" {
    #[doc = " Creates a new TileDB array given an input schema.\n\n **Example:**\n\n @code{.c}\n tiledb_array_create(ctx, \"hdfs:///tiledb_arrays/my_array\", array_schema);\n @endcode\n\n @param ctx The TileDB context.\n @param array_uri The array name.\n @param array_schema The array schema.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_create(
        ctx: *mut tiledb_ctx_t,
        array_uri: *const ::std::os::raw::c_char,
        array_schema: *const tiledb_array_schema_t,
    ) -> i32;

    #[doc = " Allocates a TileDB array object.\n\n **Example:**\n\n @code{.c}\n tiledb_array_t* array;\n tiledb_array_alloc(ctx, \"hdfs:///tiledb_arrays/my_array\", &array);\n @endcode\n\n @param ctx The TileDB context.\n @param array_uri The array URI.\n @param array The array object to be created.\n @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error."]
    pub fn tiledb_array_alloc(
        ctx: *mut tiledb_ctx_t,
        array_uri: *const ::std::os::raw::c_char,
        array: *mut *mut tiledb_array_t,
    ) -> i32;

    pub fn tiledb_array_open(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        query_type: tiledb_query_type_t,
    ) -> i32;

    pub fn tiledb_array_is_open(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        is_open: *mut i32,
    ) -> i32;

    pub fn tiledb_array_reopen(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
    ) -> i32;

    pub fn tiledb_array_set_config(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        config: *mut tiledb_config_t,
    ) -> i32;

    pub fn tiledb_array_get_config(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        config: *mut *mut tiledb_config_t,
    ) -> i32;

    pub fn tiledb_array_close(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
    ) -> i32;

    pub fn tiledb_array_free(array: *mut *mut tiledb_array_t);
}
