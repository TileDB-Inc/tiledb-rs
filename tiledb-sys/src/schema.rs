use crate::capi_enum::{
    tiledb_array_type_t, tiledb_encryption_type_t, tiledb_layout_t,
};
use crate::types::{
    tiledb_array_schema_t, tiledb_attribute_t, tiledb_ctx_t, tiledb_domain_t,
    tiledb_filter_list_t,
};

extern "C" {
    #[doc = " Creates a TileDB array schema object.\n\n **Example:**\n\n @code{.c}\n tiledb_array_schema_t* array_schema;\n tiledb_array_schema_alloc(ctx, TILEDB_DENSE, &array_schema);\n @endcode\n\n @param ctx The TileDB context.\n @param array_type The array type.\n @param array_schema The TileDB array schema to be created.\n @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_alloc(
        ctx: *mut tiledb_ctx_t,
        array_type: tiledb_array_type_t,
        array_schema: *mut *mut tiledb_array_schema_t,
    ) -> i32;

    #[doc = " Adds an attribute to an array schema.\n\n **Example:**\n\n @code{.c}\n tiledb_attribute_t* attr;\n tiledb_attribute_alloc(ctx, \"my_attr\", TILEDB_INT32, &attr);\n tiledb_array_schema_add_attribute(ctx, array_schema, attr);\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @param attr The attribute to be added.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_add_attribute(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        attr: *mut tiledb_attribute_t,
    ) -> i32;

    #[doc = " Sets whether the array can allow coordinate duplicates or not.\n Applicable only to sparse arrays (it errors out if set to `1` for dense\n arrays).\n\n **Example:**\n\n @code{.c}\n int allows_dups = 1;\n tiledb_array_schema_set_allows_dups(ctx, array_schema, allows_dups);\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @param allows_dups Whether or not the array allows coordinate duplicates.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_set_allows_dups(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        allows_dups: ::std::os::raw::c_int,
    ) -> i32;

    #[doc = " Gets whether the array can allow coordinate duplicates or not.\n It should always be `0` for dense arrays.\n\n **Example:**\n\n @code{.c}\n int allows_dups;\n tiledb_array_schema_get_allows_dups(ctx, array_schema, &allows_dups);\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @param allows_dups Whether or not the array allows coordinate duplicates.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_get_allows_dups(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        allows_dups: *mut ::std::os::raw::c_int,
    ) -> i32;

    #[doc = " Returns the array schema version.\n\n **Example:**\n\n @code{.c}\n uint32_t version;\n tiledb_array_schema_get_version(ctx, array_schema, &version);\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @param version The version.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_get_version(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        version: *mut u32,
    ) -> i32;

    #[doc = " Sets a domain for the array schema.\n\n **Example:**\n\n @code{.c}\n tiledb_domain_t* domain;\n tiledb_domain_alloc(ctx, &domain);\n // -- Add dimensions to the domain here -- //\n tiledb_array_schema_set_domain(ctx, array_schema, domain);\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @param domain The domain to be set.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_set_domain(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        domain: *mut tiledb_domain_t,
    ) -> i32;

    #[doc = " Sets the tile capacity. Applies to sparse arrays only.\n\n **Example:**\n\n @code{.c}\n tiledb_array_schema_set_capacity(ctx, array_schema, 10000);\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @param capacity The capacity of a sparse data tile. Note that\n sparse data tiles exist in sparse fragments, which can be created\n in sparse arrays only. For more details,\n see [tutorials/tiling-sparse.html](tutorials/tiling-sparse.html).\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_set_capacity(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        capacity: u64,
    ) -> i32;

    #[doc = " Sets the cell order.\n\n **Example:**\n\n @code{.c}\n tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR);\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @param cell_order The cell order to be set.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_set_cell_order(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        cell_order: tiledb_layout_t,
    ) -> i32;

    #[doc = " Sets the tile order.\n\n **Example:**\n\n @code{.c}\n tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_COL_MAJOR);\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @param tile_order The tile order to be set.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_set_tile_order(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        tile_order: tiledb_layout_t,
    ) -> i32;

    #[doc = " Sets the filter list to use for the coordinates.\n\n **Example:**\n\n @code{.c}\n tiledb_filter_list_t* filter_list;\n tiledb_filter_list_alloc(ctx, &filter_list);\n tiledb_filter_list_add_filter(ctx, filter_list, filter);\n tiledb_array_schema_set_coords_filter_list(ctx, array_schema, filter_list);\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @param filter_list The filter list to be set.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_set_coords_filter_list(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        filter_list: *mut tiledb_filter_list_t,
    ) -> i32;

    #[doc = " Sets the filter list to use for the offsets of variable-sized attribute\n values.\n\n **Example:**\n\n @code{.c}\n tiledb_filter_list_t* filter_list;\n tiledb_filter_list_alloc(ctx, &filter_list);\n tiledb_filter_list_add_filter(ctx, filter_list, filter);\n tiledb_array_schema_set_offsets_filter_list(ctx, array_schema, filter_list);\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @param filter_list The filter list to be set.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_set_offsets_filter_list(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        filter_list: *mut tiledb_filter_list_t,
    ) -> i32;

    #[doc = " Sets the filter list to use for the validity array of nullable attribute\n values.\n\n **Example:**\n\n @code{.c}\n tiledb_filter_list_t* filter_list;\n tiledb_filter_list_alloc(ctx, &filter_list);\n tiledb_filter_list_add_filter(ctx, filter_list, filter);\n tiledb_array_schema_set_validity_filter_list(ctx, array_schema, filter_list);\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @param filter_list The filter list to be set.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_set_validity_filter_list(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        filter_list: *mut tiledb_filter_list_t,
    ) -> i32;

    #[doc = " Checks the correctness of the array schema.\n\n **Example:**\n\n @code{.c}\n tiledb_array_schema_check(ctx, array_schema);\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @return `TILEDB_OK` if the array schema is correct and `TILEDB_ERR` upon any\n     error."]
    pub fn tiledb_array_schema_check(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
    ) -> i32;

    #[doc = " Retrieves the schema of an array from the disk, creating an array schema\n struct.\n\n **Example:**\n\n @code{.c}\n tiledb_array_schema_t* array_schema;\n tiledb_array_schema_load(ctx, \"s3://tiledb_bucket/my_array\", &array_schema);\n // Make sure to free the array schema in the end\n @endcode\n\n @param ctx The TileDB context.\n @param array_uri The array whose schema will be retrieved.\n @param array_schema The array schema to be retrieved, or `NULL` upon error.\n @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_load(
        ctx: *mut tiledb_ctx_t,
        array_uri: *const ::std::os::raw::c_char,
        array_schema: *mut *mut tiledb_array_schema_t,
    ) -> i32;

    #[doc = " Retrieves the schema of an encrypted array from the disk, creating an array\n schema struct.\n\n **Example:**\n\n @code{.c}\n // Load AES-256 key from disk, environment variable, etc.\n uint8_t key[32] = ...;\n tiledb_array_schema_t* array_schema;\n tiledb_array_schema_load_with_key(\n     ctx, \"s3://tiledb_bucket/my_array\", TILEDB_AES_256_GCM,\n     key, sizeof(key), &array_schema);\n // Make sure to free the array schema in the end\n @endcode\n\n @param ctx The TileDB context.\n @param array_uri The array whose schema will be retrieved.\n @param encryption_type The encryption type to use.\n @param encryption_key The encryption key to use.\n @param key_length Length in bytes of the encryption key.\n @param array_schema The array schema to be retrieved, or `NULL` upon error.\n @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_load_with_key(
        ctx: *mut tiledb_ctx_t,
        array_uri: *const ::std::os::raw::c_char,
        encryption_type: tiledb_encryption_type_t,
        encryption_key: *const ::std::os::raw::c_void,
        key_length: u32,
        array_schema: *mut *mut tiledb_array_schema_t,
    ) -> i32;

    #[doc = " Retrieves the array type.\n\n **Example:**\n\n @code{.c}\n tiledb_array_schema_t* array_schema;\n tiledb_array_schema_load(ctx, \"s3://tiledb_bucket/my_array\", array_schema);\n tiledb_array_type_t* array_type;\n tiledb_array_schema_get_array_type(ctx, array_schema, &array_type);\n // Make sure to free the array schema in the end\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @param array_type The array type to be retrieved.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_get_array_type(
        ctx: *mut tiledb_ctx_t,
        array_schema: *const tiledb_array_schema_t,
        array_type: *mut tiledb_array_type_t,
    ) -> i32;

    #[doc = " Retrieves the capacity.\n\n **Example:**\n\n @code{.c}\n uint64_t capacity;\n tiledb_array_schema_get_capacity(ctx, array_schema, &capacity);\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @param capacity The capacity to be retrieved.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_get_capacity(
        ctx: *mut tiledb_ctx_t,
        array_schema: *const tiledb_array_schema_t,
        capacity: *mut u64,
    ) -> i32;

    #[doc = " Retrieves the cell order.\n\n **Example:**\n\n @code{.c}\n tiledb_layout_t cell_order;\n tiledb_array_schema_get_cell_order(ctx, array_schema, &cell_order);\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @param cell_order The cell order to be retrieved.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_get_cell_order(
        ctx: *mut tiledb_ctx_t,
        array_schema: *const tiledb_array_schema_t,
        cell_order: *mut tiledb_layout_t,
    ) -> i32;

    #[doc = " Retrieves the filter list used for the coordinates.\n\n **Example:**\n\n @code{.c}\n tiledb_filter_list_t* filter_list;\n tiledb_array_schema_get_coords_filter_list(ctx, array_schema, &filter_list);\n tiledb_filter_list_free(ctx, &filter_list);\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @param filter_list The filter list to be retrieved.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_get_coords_filter_list(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        filter_list: *mut *mut tiledb_filter_list_t,
    ) -> i32;

    #[doc = " Retrieves the filter list used for the offsets.\n\n **Example:**\n\n @code{.c}\n tiledb_filter_list_t* filter_list;\n tiledb_array_schema_get_offsets_filter_list(ctx, array_schema, &filter_list);\n tiledb_filter_list_free(ctx, &filter_list);\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @param filter_list The filter list to be retrieved.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_get_offsets_filter_list(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        filter_list: *mut *mut tiledb_filter_list_t,
    ) -> i32;

    #[doc = " Retrieves the filter list used for validity maps.\n\n **Example:**\n\n @code{.c}\n tiledb_filter_list_t* filter_list;\n tiledb_array_schema_get_validity_filter_list(ctx, array_schema,\n &filter_list); tiledb_filter_list_free(ctx, &filter_list);\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @param filter_list The filter list to be retrieved.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_get_validity_filter_list(
        ctx: *mut tiledb_ctx_t,
        array_schema: *mut tiledb_array_schema_t,
        filter_list: *mut *mut tiledb_filter_list_t,
    ) -> i32;

    #[doc = " Retrieves the array domain.\n\n **Example:**\n\n @code{.c}\n tiledb_domain_t* domain;\n tiledb_array_schema_get_domain(ctx, array_schema, &domain);\n // Make sure to delete domain in the end\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @param domain The array domain to be retrieved.\n @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_get_domain(
        ctx: *mut tiledb_ctx_t,
        array_schema: *const tiledb_array_schema_t,
        domain: *mut *mut tiledb_domain_t,
    ) -> i32;

    #[doc = " Retrieves the tile order.\n\n **Example:**\n\n @code{.c}\n tiledb_layout_t tile_order;\n tiledb_array_schema_get_tile_order(ctx, array_schema, &tile_order);\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @param tile_order The tile order to be retrieved.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_get_tile_order(
        ctx: *mut tiledb_ctx_t,
        array_schema: *const tiledb_array_schema_t,
        tile_order: *mut tiledb_layout_t,
    ) -> i32;

    #[doc = " Retrieves the number of array attributes.\n\n **Example:**\n\n @code{.c}\n uint32_t attr_num;\n tiledb_array_schema_get_attribute_num(ctx, array_schema, &attr_num);\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @param attribute_num The number of attributes to be retrieved.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_get_attribute_num(
        ctx: *mut tiledb_ctx_t,
        array_schema: *const tiledb_array_schema_t,
        attribute_num: *mut u32,
    ) -> i32;

    #[doc = " Retrieves an attribute given its index.\n\n Attributes are ordered the same way they were defined\n when constructing the array schema.\n\n **Example:**\n\n The following retrieves the first attribute in the schema.\n\n @code{.c}\n tiledb_attribute_t* attr;\n tiledb_array_schema_get_attribute_from_index(ctx, array_schema, 0, &attr);\n // Make sure to delete the retrieved attribute in the end.\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @param index The index of the attribute to retrieve.\n @param attr The attribute object to retrieve.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_get_attribute_from_index(
        ctx: *mut tiledb_ctx_t,
        array_schema: *const tiledb_array_schema_t,
        index: u32,
        attr: *mut *mut tiledb_attribute_t,
    ) -> i32;

    #[doc = " Retrieves an attribute given its name (key).\n\n **Example:**\n\n The following retrieves the first attribute in the schema.\n\n @code{.c}\n tiledb_attribute_t* attr;\n tiledb_array_schema_get_attribute_from_name(\n     ctx, array_schema, \"attr_0\", &attr);\n // Make sure to delete the retrieved attribute in the end.\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @param name The name (key) of the attribute to retrieve.\n @param attr THe attribute object to retrieve.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_get_attribute_from_name(
        ctx: *mut tiledb_ctx_t,
        array_schema: *const tiledb_array_schema_t,
        name: *const ::std::os::raw::c_char,
        attr: *mut *mut tiledb_attribute_t,
    ) -> i32;

    #[doc = " Checks whether the array schema has an attribute of the given name.\n\n **Example:**\n\n @code{.c}\n int32_t has_attr;\n tiledb_array_schema_has_attribute(ctx, array_schema, \"attr_0\", &has_attr);\n @endcode\n\n @param ctx The TileDB context.\n @param array_schema The array schema.\n @param name The name of the attribute to check for.\n @param has_attr Set to `1` if the array schema has an attribute of the\n      given name, else `0`.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_schema_has_attribute(
        ctx: *mut tiledb_ctx_t,
        array_schema: *const tiledb_array_schema_t,
        name: *const ::std::os::raw::c_char,
        has_attr: *mut i32,
    ) -> i32;

    #[doc = " Destroys an array schema, freeing associated memory.\n\n **Example:**\n\n @code{.c}\n tiledb_array_schema_free(&array_schema);\n @endcode\n\n @param array_schema The array schema to be destroyed."]
    pub fn tiledb_array_schema_free(
        array_schema: *mut *mut tiledb_array_schema_t,
    );
}
