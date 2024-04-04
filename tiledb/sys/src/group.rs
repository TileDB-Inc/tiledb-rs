use crate::capi_enum::{tiledb_object_t, tiledb_query_type_t};
use crate::tiledb_datatype_t;
use crate::types::{
    capi_return_t, tiledb_config_t, tiledb_ctx_t, tiledb_group_t,
    tiledb_string_t,
};

extern "C" {
    #[doc = " Creates a new TileDB group.\n\n **Example:**\n\n @code{.c}\n tiledb_group_create(ctx, \"my_group\");\n @endcode\n\n @param ctx The TileDB context.\n @param group_uri The group URI.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_group_create(
        ctx: *mut tiledb_ctx_t,
        group_uri: *const ::std::ffi::c_char,
    ) -> capi_return_t;

    #[doc = " Creates a new TileDB group.\n\n **Example:**\n\n @code{.c}\n tiledb_group_t* group;\n tiledb_group_alloc(ctx, \"my_group\", &group);\n @endcode\n\n @param ctx The TileDB context.\n @param group_uri The group URI.\n @param group The TileDB group to be allocated\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_group_alloc(
        ctx: *mut tiledb_ctx_t,
        group_uri: *const ::std::ffi::c_char,
        group: *mut *mut tiledb_group_t,
    ) -> capi_return_t;

    #[doc = " Destroys a TileDB group, freeing associated memory.\n\n **Example:**\n\n @code{.c}\n tiledb_group_t* group;\n tiledb_group_alloc(ctx, \"my_group\", &group);\n tiledb_group_free(&group);\n @endcode\n\n @param group The TileDB group to be freed"]
    pub fn tiledb_group_free(group: *mut *mut tiledb_group_t);

    #[doc = " Opens a TileDB group. The group is opened using a query type as input.\n This is to indicate that queries created for this `tiledb_group_t`\n object will inherit the query type. In other words, `tiledb_group_t`\n objects are opened to receive only one type of queries.\n They can always be closed and be re-opened with another query type.\n Also there may be many different `tiledb_group_t`\n objects created and opened with different query types.\n\n **Example:**\n\n @code{.c}\n tiledb_group_t* group;\n tiledb_group_alloc(ctx, \"hdfs:///tiledb_groups/my_group\", &group);\n tiledb_group_open(ctx, group, TILEDB_READ);\n @endcode\n\n @param ctx The TileDB context.\n @param group The group object to be opened.\n @param query_type The type of queries the group object will be receiving.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error.\n\n @note If the same group object is opened again without being closed,\n     an error will be set and TILEDB_ERR returned.\n @note The config should be set before opening an group.\n @note If the group is to be opened at a specfic time interval, the\n      `timestamp{start, end}` values should be set to a config that's set to\n       the group object before opening the group."]
    pub fn tiledb_group_open(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        query_type: tiledb_query_type_t,
    ) -> capi_return_t;

    #[doc = " Closes a TileDB group.\n\n **Example:**\n\n @code{.c}\n tiledb_group_t* group;\n tiledb_group_alloc(ctx, \"hdfs:///tiledb_groups/my_group\", &group);\n tiledb_group_open(ctx, group, TILEDB_READ);\n tiledb_group_close(ctx, group);\n @endcode\n\n @param ctx The TileDB context.\n @param group The group object to be closed.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error.\n\n @note If the group object has already been closed, the function has\n     no effect."]
    pub fn tiledb_group_close(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
    ) -> capi_return_t;

    #[doc = " Sets the group config.\n\n **Example:**\n\n @code{.c}\n tiledb_group_t* group;\n tiledb_group_alloc(ctx, \"s3://tiledb_bucket/my_group\", &group);\n // Set the config for the given group.\n tiledb_config_t* config;\n tiledb_group_set_config(ctx, group, config);\n tiledb_group_open(ctx, group, TILEDB_READ);\n @endcode\n\n @param ctx The TileDB context.\n @param group The group to set the config for.\n @param config The config to be set.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error.\n\n @pre The config must be set on a closed group."]
    pub fn tiledb_group_set_config(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        config: *mut tiledb_config_t,
    ) -> capi_return_t;

    #[doc = " Gets the group config.\n\n **Example:**\n\n @code{.c}\n // Retrieve the config for the given group.\n tiledb_config_t* config;\n tiledb_group_get_config(ctx, group, config);\n @endcode\n\n @param ctx The TileDB context.\n @param group The group to set the config for.\n @param config Set to the retrieved config.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_group_get_config(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        config: *mut *mut tiledb_config_t,
    ) -> capi_return_t;

    #[doc = " It puts a metadata key-value item to an open group. The group must\n be opened in WRITE mode, otherwise the function will error out.\n\n @param ctx The TileDB context.\n @param group An group opened in WRITE mode.\n @param key The key of the metadata item to be added. UTF-8 encodings\n     are acceptable.\n @param value_type The datatype of the value.\n @param value_num The value may consist of more than one items of the\n     same datatype. This argument indicates the number of items in the\n     value component of the metadata.\n @param value The metadata value in binary form.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error.\n\n @note The writes will take effect only upon closing the group."]
    pub fn tiledb_group_put_metadata(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        key: *const ::std::ffi::c_char,
        value_type: tiledb_datatype_t,
        value_num: u32,
        value: *const ::std::ffi::c_void,
    ) -> capi_return_t;

    #[doc = " Deletes written data from an open group. The group must\n be opened in MODIFY_EXCLSUIVE mode, otherwise the function will error out.\n\n @param ctx The TileDB context.\n @param group An group opened in MODIFY_EXCLUSIVE mode.\n @param uri The address of the group item to be deleted.\n @param recursive True if all data inside the group is to be deleted.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error.\n\n @note if recursive == false, data added to the group will be left as-is."]
    pub fn tiledb_group_delete_group(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        uri: *const ::std::ffi::c_char,
        recursive: u8,
    ) -> i32;

    #[doc = " Deletes a metadata key-value item from an open group. The group must\n be opened in WRITE mode, otherwise the function will error out.\n\n @param ctx The TileDB context.\n @param group An group opened in WRITE mode.\n @param key The key of the metadata item to be deleted.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error.\n\n @note The writes will take effect only upon closing the group.\n\n @note If the key does not exist, this will take no effect\n     (i.e., the function will not error out)."]
    pub fn tiledb_group_delete_metadata(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        key: *const ::std::ffi::c_char,
    ) -> capi_return_t;

    #[doc = " It gets a metadata key-value item from an open group. The group must\n be opened in READ mode, otherwise the function will error out.\n\n @param ctx The TileDB context.\n @param group An group opened in READ mode.\n @param key The key of the metadata item to be retrieved. UTF-8 encodings\n     are acceptable.\n @param value_type The datatype of the value.\n @param value_num The value may consist of more than one items of the\n     same datatype. This argument indicates the number of items in the\n     value component of the metadata. Keys with empty values are indicated\n     by value_num == 1 and value == NULL.\n @param value The metadata value in binary form.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error.\n\n @note If the key does not exist, then `value` will be NULL."]
    pub fn tiledb_group_get_metadata(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        key: *const ::std::ffi::c_char,
        value_type: *mut tiledb_datatype_t,
        value_num: *mut u32,
        value: *mut *const ::std::ffi::c_void,
    ) -> capi_return_t;

    #[doc = " It gets then number of metadata items in an open group. The group must\n be opened in READ mode, otherwise the function will error out.\n\n @param ctx The TileDB context.\n @param group An group opened in READ mode.\n @param num The number of metadata items to be retrieved.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_group_get_metadata_num(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        num: *mut u64,
    ) -> capi_return_t;

    #[doc = " It gets a metadata item from an open group using an index.\n The group must be opened in READ mode, otherwise the function will\n error out.\n\n @param ctx The TileDB context.\n @param group An group opened in READ mode.\n @param index The index used to get the metadata.\n @param key The metadata key.\n @param key_len The metadata key length.\n @param value_type The datatype of the value.\n @param value_num The value may consist of more than one items of the\n     same datatype. This argument indicates the number of items in the\n     value component of the metadata.\n @param value The metadata value in binary form.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
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

    #[doc = " Checks whether a key exists in metadata from an open group. The group must\n be opened in READ mode, otherwise the function will error out.\n\n @param ctx The TileDB context.\n @param group An group opened in READ mode.\n @param key The key to be checked. UTF-8 encoding are acceptable.\n @param value_type The datatype of the value, if any.\n @param has_key Set to `1` if the metadata with given key exists, else `0`.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error.\n\n @note If the key does not exist, then `value` will be NULL."]
    pub fn tiledb_group_has_metadata_key(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        key: *const ::std::ffi::c_char,
        value_type: *mut tiledb_datatype_t,
        has_key: *mut i32,
    ) -> capi_return_t;

    #[doc = " Add a member to a group\n\n **Example:**\n\n @code{.c}\n tiledb_group_t* group;\n tiledb_group_alloc(ctx, \"s3://tiledb_bucket/my_group\", &group);\n tiledb_group_open(ctx, group, TILEDB_WRITE);\n tiledb_group_add_member(ctx, group, \"s3://tiledb_bucket/my_array\");\n tiledb_group_add_member(ctx, group, \"s3://tiledb_bucket/my_group_2\");\n @endcode\n\n @param ctx The TileDB context.\n @param group An group opened in WRITE mode.\n @param uri URI of member to add\n @param relative is the URI relative to the group\n @param name optional name group member can be given to be looked up by.\n Can be set to NULL.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_group_add_member(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        uri: *const ::std::ffi::c_char,
        relative: u8,
        name: *const ::std::ffi::c_char,
    ) -> capi_return_t;

    #[doc = " Remove a member from a group\n\n * @code{.c}\n tiledb_group_t* group;\n tiledb_group_alloc(ctx, \"s3://tiledb_bucket/my_group\", &group);\n tiledb_group_open(ctx, group, TILEDB_WRITE);\n tiledb_group_remove_member(ctx, group, \"s3://tiledb_bucket/my_array\");\n @endcode\n\n @param ctx The TileDB context.\n @param group An group opened in WRITE mode.\n @param name_or_uri Name or URI of member to remove. If the URI is\n registered multiple times in the group, the name needs to be specified so\n that the correct one can be removed. Note that if a URI is registered as\n both a named and unnamed member, the unnamed member will be removed\n successfully using the URI.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_group_remove_member(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        name_or_uri: *const ::std::ffi::c_char,
    ) -> capi_return_t;

    #[doc = " Get the count of members in a group\n\n * @code{.c}\n tiledb_group_t* group;\n tiledb_group_alloc(ctx, \"s3://tiledb_bucket/my_group\", &group);\n tiledb_group_open(ctx, group, TILEDB_WRITE);\n tiledb_group_add_member(ctx, group, \"s3://tiledb_bucket/my_array\");\n tiledb_group_add_member(ctx, group, \"s3://tiledb_bucket/my_group_2\");\n\n tiledb_group_close(ctx, group);\n tiledb_group_open(ctx, group, TILEDB_READ);\n uint64_t count = 0;\n tiledb_group_get_member_count(ctx, group, &count);\n\n @endcode\n\n @param ctx The TileDB context.\n @param group An group opened in READ mode.\n @param count number of members in group\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_group_get_member_count(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        count: *mut u64,
    ) -> capi_return_t;

    #[doc = " Get a member of a group by index and details of group\n\n **Example:**\n\n @code{.c}\n tiledb_group_t* group;\n tiledb_group_alloc(ctx, \"s3://tiledb_bucket/my_group\", &group);\n tiledb_group_open(ctx, group, TILEDB_WRITE);\n tiledb_group_add_member(ctx, group, \"s3://tiledb_bucket/my_array\");\n tiledb_group_add_member(ctx, group, \"s3://tiledb_bucket/my_group_2\");\n\n tiledb_group_close(ctx, group);\n tiledb_group_open(ctx, group, TILEDB_READ);\n tiledb_string_t *uri, *name;\n tiledb_object_t type;\n tiledb_group_get_member_by_index_v2(ctx, group, 0, &uri, &type, &name);\n\n tiledb_string_free(uri);\n tiledb_string_free(name);\n\n @endcode\n\n @param ctx The TileDB context.\n @param group An group opened in READ mode.\n @param index index of member to fetch\n @param uri Handle to the URI of the member.\n @param type type of member\n @param name Handle to the name of the member. NULL if name was not set\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_group_get_member_by_index_v2(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        index: u64,
        uri: *mut *mut tiledb_string_t,
        type_: *mut tiledb_object_t,
        name: *mut *mut tiledb_string_t,
    ) -> capi_return_t;

    #[doc = " Get a member of a group by name and details of group.\n\n **Example:**\n\n @code{.c}\n tiledb_group_t* group;\n tiledb_group_alloc(ctx, \"s3://tiledb_bucket/my_group\", &group);\n tiledb_group_open(ctx, group, TILEDB_WRITE);\n tiledb_group_add_member(ctx, group, \"s3://tiledb_bucket/my_array\", \"array1\");\n tiledb_group_add_member(ctx, group, \"s3://tiledb_bucket/my_group_2\",\n \"group2\");\n\n tiledb_group_close(ctx, group);\n tiledb_group_open(ctx, group, TILEDB_READ);\n tilledb_string_t *uri;\n tiledb_object_t type;\n tiledb_group_get_member_by_name(ctx, group, \"array1\", &uri, &type);\n\n tiledb_string_free(uri);\n\n @endcode\n\n @param ctx The TileDB context.\n @param group An group opened in READ mode.\n @param name name of member to fetch\n @param uri Handle to the URI of the member.\n @param type type of member\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_group_get_member_by_name_v2(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        name: *const ::std::ffi::c_char,
        uri: *mut *mut tiledb_string_t,
        type_: *mut tiledb_object_t,
    ) -> capi_return_t;

    #[doc = " Get a member of a group by name and relative characteristic of that name\n\n **Example:**\n\n @code{.c}\n tiledb_group_t* group;\n tiledb_group_alloc(ctx, \"s3://tiledb_bucket/my_group\", &group);\n tiledb_group_open(ctx, group, TILEDB_WRITE);\n tiledb_group_add_member(ctx, group, \"s3://tiledb_bucket/my_array\", true,\n     \"array1\");\n tiledb_group_add_member(ctx, group, \"s3://tiledb_bucket/my_group_2\",\n     false, \"group2\");\n\n tiledb_group_close(ctx, group);\n tiledb_group_open(ctx, group, TILEDB_READ);\n uint8_t is_relative;\n tiledb_group_get_is_relative_uri_by_name(ctx, group, \"array1\", &is_relative);\n\n @endcode\n\n @param[in] ctx The TileDB context.\n @param[in] group An group opened in READ mode.\n @param[in] name name of member to fetch\n @param[out] is_relative to receive relative characteristic of named member\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_group_get_is_relative_uri_by_name(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        name: *const ::std::ffi::c_char,
        relative: *mut u8,
    ) -> capi_return_t;

    #[doc = " Checks if the group is open.\n\n @param ctx The TileDB context.\n @param group The group to be checked.\n @param is_open `1` if the group is open and `0` otherwise.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_group_is_open(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        is_open: *mut i32,
    ) -> capi_return_t;

    #[doc = " Retrieves the URI the group was opened with. It outputs an error\n if the group is not open.\n\n @param ctx The TileDB context.\n @param group The input group.\n @param group_uri The group URI to be retrieved.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_group_get_uri(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        group_uri: *mut *const ::std::ffi::c_char,
    ) -> capi_return_t;

    #[doc = " Retrieves the query type with which the group was opened.\n\n **Example:**\n\n @code{.c}\n tiledb_group_t* group;\n tiledb_group_alloc(ctx, \"s3://tiledb_groups/my_group\", &group);\n tiledb_group_open(ctx, group, TILEDB_READ);\n tiledb_query_type_t query_type;\n tiledb_group_get_type(ctx, group, &query_type);\n @endcode\n\n @param ctx The TileDB context.\n @param group The group.\n @param query_type The query type to be retrieved.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_group_get_query_type(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        query_type: *mut tiledb_query_type_t,
    ) -> capi_return_t;

    #[doc = " Dump a string representation of a group\n\n @param ctx The TileDB context.\n @param group The group.\n @param dump_ascii The output string. The caller takes ownership\n   of the c-string.\n @param recursive should we recurse into sub-groups\n @return  `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_group_dump_str(
        ctx: *mut tiledb_ctx_t,
        group: *mut tiledb_group_t,
        dump_ascii: *mut *mut ::std::ffi::c_char,
        recursive: u8,
    ) -> capi_return_t;

    #[doc = " Consolidates the group metadata into a single group metadata file.\n\n **Example:**\n\n @code{.c}\n tiledb_group_consolidate_metadata(\n     ctx, \"tiledb:///groups/mygroup\", nullptr);\n @endcode\n\n @param ctx The TileDB context.\n @param group_uri The name of the TileDB group whose metadata will\n     be consolidated.\n @param config Configuration parameters for the consolidation\n     (`nullptr` means default, which will use the config from `ctx`).\n @return `TILEDB_OK` on success, and `TILEDB_ERR` on error."]
    pub fn tiledb_group_consolidate_metadata(
        ctx: *mut tiledb_ctx_t,
        group_uri: *const ::std::ffi::c_char,
        config: *mut tiledb_config_t,
    ) -> capi_return_t;

    #[doc = " Cleans up the group metadata\n Note that this will coarsen the granularity of time traveling (see docs\n for more information).\n\n **Example:**\n\n @code{.c}\n tiledb_group_vacuum_metadata(\n     ctx, \"tiledb:///groups/mygroup\", nullptr);\n @endcode\n\n @param ctx The TileDB context.\n @param group_uri The name of the TileDB group to vacuum.\n @param config Configuration parameters for the vacuuming\n     (`nullptr` means default, which will use the config from `ctx`).\n @return `TILEDB_OK` on success, and `TILEDB_ERR` on error."]
    pub fn tiledb_group_vacuum_metadata(
        ctx: *mut tiledb_ctx_t,
        group_uri: *const ::std::ffi::c_char,
        config: *mut tiledb_config_t,
    ) -> capi_return_t;
}
