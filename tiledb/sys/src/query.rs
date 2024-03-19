use crate::capi_enum::{
    tiledb_layout_t, tiledb_query_status_t, tiledb_query_type_t,
};
use crate::types::{
    tiledb_array_t, tiledb_config_t, tiledb_ctx_t, tiledb_query_condition_t,
    tiledb_query_t, tiledb_subarray_t,
};

extern "C" {
    #[doc = " Creates a TileDB query object. Note that the query object is associated\n with a specific array object. The query type (read or write) is inferred\n from the array object, which was opened with a specific query type.\n\n **Example:**\n\n @code{.c}\n tiledb_array_t* array;\n tiledb_array_alloc(ctx, \"file:///my_array\", &array);\n tiledb_array_open(ctx, array, TILEDB_WRITE);\n tiledb_query_t* query;\n tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);\n @endcode\n\n @param ctx The TileDB context.\n @param query The query object to be created.\n @param array An opened array object.\n @param query_type The query type. This must comply with the query type\n     `array` was opened.\n @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error."]
    pub fn tiledb_query_alloc(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        query_type: tiledb_query_type_t,
        query: *mut *mut tiledb_query_t,
    ) -> i32;

    #[doc = " Retrieves the stats from a Query.\n\n **Example:**\n\n @code{.c}\n char* stats_json;\n tiledb_query_get_stats(ctx, query, &stats_json);\n // Use the string\n tiledb_stats_free_str(&stats_json);\n @endcode\n\n @param ctx The TileDB context.\n @param query The query object.\n @param stats_json The output json. The caller takes ownership\n   of the c-string and must free it using tiledb_stats_free_str().\n @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error."]
    pub fn tiledb_query_get_stats(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        stats_json: *mut *mut ::std::os::raw::c_char,
    ) -> i32;

    #[doc = " Set the query config\n\n Setting the query config will also set the subarray configuration in order to\n maintain existing behavior. If you wish the subarray to have a different\n configuration than the query, set it after calling tiledb_query_set_config.\n\n Setting the configuration with this function overrides the following\n Query-level parameters only:\n\n - `sm.memory_budget`\n - `sm.memory_budget_var`\n - `sm.var_offsets.mode`\n - `sm.var_offsets.extra_element`\n - `sm.var_offsets.bitsize`\n - `sm.check_coord_dups`\n - `sm.check_coord_oob`\n - `sm.check_global_order`\n - `sm.dedup_coords`"]
    pub fn tiledb_query_set_config(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        config: *mut tiledb_config_t,
    ) -> i32;

    #[doc = " Retrieves the config from a Query.\n\n **Example:**\n\n @code{.c}\n tiledb_config_t* config;\n tiledb_query_get_config(ctx, vfs, &config);\n // Make sure to free the retrieved config\n @endcode\n\n @param ctx The TileDB context.\n @param query The query object.\n @param config The config to be retrieved.\n @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error."]
    pub fn tiledb_query_get_config(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        config: *mut *mut tiledb_config_t,
    ) -> i32;

    #[doc = " Indicates that the query will write or read a subarray, and provides\n the appropriate information.\n\n **Example:**\n\n The following sets a 2D subarray [0,10], [20, 30] to the query.\n\n @code{.c}\n uint64_t subarray[] = { 0, 10, 20, 30};\n tiledb_query_set_subarray(ctx, query, subarray);\n @endcode\n\n @param ctx The TileDB context.\n @param query The TileDB query.\n @param subarray The subarray in which the array read/write will be\n     constrained on. It should be a sequence of [low, high] pairs (one\n     pair per dimension). For the case of writes, this is meaningful only\n     for dense arrays. Note that `subarray` must have the same type as the\n     domain.\n @return `TILEDB_OK` for success or `TILEDB_ERR` for error.\n\n @note This will error if the query is already initialized.\n\n @note This function will error for writes to sparse arrays."]
    pub fn tiledb_query_set_subarray(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        subarray: *const ::std::os::raw::c_void,
    ) -> i32;

    #[doc = " Indicates that the query will write or read a subarray, and provides\n the appropriate information.\n\n **Example:**\n\n The following sets a 2D subarray [0,10], [20, 30] to the query.\n\n @code{.c}\n tiledb_subarray_t *subarray;\n tiledb_subarray_alloc(ctx, array, &subarray);\n uint64_t subarray_v[] = { 0, 10, 20, 30};\n tiledb_subarray_set_subarray(ctx, subarray, subarray_v);\n tiledb_query_set_subarray_t(ctx, query, subarray);\n @endcode\n\n @param ctx The TileDB context.\n @param query The TileDB query.\n @param subarray The subarray by which the array read/write will be\n     constrained. For the case of writes, this is meaningful only\n     for dense arrays.\n @return `TILEDB_OK` for success or `TILEDB_ERR` for error.\n\n @note This will error if the query is already initialized.\n\n @note This will error for writes to sparse arrays."]
    pub fn tiledb_query_set_subarray_t(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        subarray: *const tiledb_subarray_t,
    ) -> i32;

    #[doc = " Sets the buffer for an attribute/dimension to a query, which will\n either hold the values to be written (if it is a write query), or will hold\n the results from a read query.\n\n The caller owns the `buffer` provided and is responsible for freeing the\n memory associated with it. For writes, the buffer holds values to be written\n which can be freed at any time after query completion. For reads, the buffer\n is allocated by the caller and will contain data read by the query after\n completion. The freeing of this memory is up to the caller once they are done\n referencing the read data.\n\n **Example:**\n\n @code{.c}\n int32_t a1[100];\n uint64_t a1_size = sizeof(a1);\n tiledb_query_set_data_buffer(ctx, query, \"a1\", a1, &a1_size);\n @endcode\n\n @param ctx The TileDB context.\n @param query The TileDB query.\n @param name The attribute/dimension to set the buffer for. Note that\n     zipped coordinates have special name `TILEDB_COORDS`.\n @param buffer The buffer that either have the input data to be written,\n     or will hold the data to be read.\n @param buffer_size In the case of writes, this is the size of `buffer`\n     in bytes. In the case of reads, this initially contains the allocated\n     size of `buffer`, but after the termination of the query\n     it will contain the size of the useful (read) data in `buffer`.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_set_data_buffer(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        name: *const ::std::os::raw::c_char,
        buffer: *mut ::std::os::raw::c_void,
        buffer_size: *mut u64,
    ) -> i32;

    #[doc = " Sets the starting offsets of each cell value in the data buffer.\n\n The caller owns the `buffer` provided and is responsible for freeing the\n memory associated with it. For writes, the buffer holds offsets to be written\n which can be freed at any time after query completion. For reads, the buffer\n is allocated by the caller and will contain offset data read by the query\n after completion. The freeing of this memory is up to the caller once they\n are done referencing the read data.\n\n **Example:**\n\n @code{.c}\n uint64_t a1[100];\n uint64_t a1_size = sizeof(a1);\n tiledb_query_set_offsets_buffer(ctx, query, \"a1\", a1, &a1_size);\n @endcode\n\n @param ctx The TileDB context.\n @param query The TileDB query.\n @param name The attribute/dimension to set the buffer for. Note that\n     zipped coordinates have special name `TILEDB_COORDS`.\n @param buffer This buffer holds the starting offsets\n     of each cell value in `buffer_val`.\n @param buffer_size In the case of writes, it is the size of `buffer_off`\n     in bytes. In the case of reads, this initially contains the allocated\n     size of `buffer_off`, but after the *end of the query*\n     (`tiledb_query_submit`) it will contain the size of the useful (read)\n     data in `buffer_off`.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_set_offsets_buffer(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        name: *const ::std::os::raw::c_char,
        buffer: *mut u64,
        buffer_size: *mut u64,
    ) -> i32;

    #[doc = " Sets the validity byte map that has exactly one value for each value in the\n data buffer.\n\n The caller owns the `buffer` provided and is responsible for freeing the\n memory associated with it. For writes, the buffer holds validity values to be\n written which can be freed at any time after query completion. For reads, the\n buffer is allocated by the caller and will contain the validity map read by\n the query after completion. The freeing of this memory is up to the caller\n once they are done referencing the read data.\n\n **Example:**\n\n @code{.c}\n uint8_t a1[100];\n uint64_t a1_size = sizeof(a1);\n tiledb_query_set_validity_buffer(ctx, query, \"a1\", a1, &a1_size);\n @endcode\n\n @param ctx The TileDB context.\n @param query The TileDB query.\n @param name The attribute/dimension to set the buffer for. Note that\n     zipped coordinates have special name `TILEDB_COORDS`.\n @param buffer The validity byte map that has exactly\n     one value for each value in `buffer`.\n @param buffer_size In the case of writes, this is the\n     size of `buffer_validity_bytemap` in bytes. In the case of reads,\n     this initially contains the allocated size of `buffer_validity_bytemap`,\n     but after the termination of the query it will contain the size of the\n     useful (read) data in `buffer_validity_bytemap`.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_set_validity_buffer(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        name: *const ::std::os::raw::c_char,
        buffer: *mut u8,
        buffer_size: *mut u64,
    ) -> i32;

    #[doc = " Gets the buffer of a fixed-sized attribute/dimension from a query. If the\n buffer has not been set, then `buffer` is set to `nullptr`.\n\n **Example:**\n\n @code{.c}\n int* a1;\n uint64_t* a1_size;\n tiledb_query_get_data_buffer(ctx, query, \"a1\", &a1, &a1_size);\n @endcode\n\n @param ctx The TileDB context.\n @param query The TileDB query.\n @param name The attribute/dimension to get the buffer for. Note that the\n     zipped coordinates have special name `TILEDB_COORDS`.\n @param buffer The buffer to retrieve.\n @param buffer_size A pointer to the size of the buffer. Note that this is\n     a double pointer and returns the original variable address from\n     `set_buffer`.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_get_data_buffer(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        name: *const ::std::os::raw::c_char,
        buffer: *mut *mut ::std::os::raw::c_void,
        buffer_size: *mut *mut u64,
    ) -> i32;

    #[doc = " Gets the starting offsets of each cell value in the data buffer.\n\n **Example:**\n\n @code{.c}\n int* a1;\n uint64_t* a1_size;\n tiledb_query_get_offsets_buffer(ctx, query, \"a1\", &a1, &a1_size);\n @endcode\n\n @param ctx The TileDB context.\n @param query The TileDB query.\n @param name The attribute/dimension to get the buffer for. Note that the\n     zipped coordinates have special name `TILEDB_COORDS`.\n @param buffer The buffer to retrieve.\n @param buffer_size A pointer to the size of the buffer. Note that this is\n     a double pointer and returns the original variable address from\n     `set_buffer`.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_get_offsets_buffer(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        name: *const ::std::os::raw::c_char,
        buffer: *mut *mut u64,
        buffer_size: *mut *mut u64,
    ) -> i32;

    #[doc = " Gets the validity byte map that has exactly one value for each value in the\n data buffer.\n\n **Example:**\n\n @code{.c}\n int* a1;\n uint64_t* a1_size;\n tiledb_query_get_validity_buffer(ctx, query, \"a1\", &a1, &a1_size);\n @endcode\n\n @param ctx The TileDB context.\n @param query The TileDB query.\n @param name The attribute/dimension to get the buffer for. Note that the\n     zipped coordinates have special name `TILEDB_COORDS`.\n @param buffer The buffer to retrieve.\n @param buffer_size A pointer to the size of the buffer. Note that this is\n     a double pointer and returns the original variable address from\n     `set_buffer`.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_get_validity_buffer(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        name: *const ::std::os::raw::c_char,
        buffer: *mut *mut u8,
        buffer_size: *mut *mut u64,
    ) -> i32;

    #[doc = " Sets the layout of the cells to be written or read.\n\n **Example:**\n\n @code{.c}\n tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR);\n @endcode\n\n @param ctx The TileDB context.\n @param query The TileDB query.\n @param layout For a write query, this specifies the order of the cells\n     provided by the user in the buffers. For a read query, this specifies\n     the order of the cells that will be retrieved as results and stored\n     in the user buffers. The layout can be one of the following:\n    - `TILEDB_COL_MAJOR`:\n      This means column-major order with respect to the subarray.\n    - `TILEDB_ROW_MAJOR`:\n      This means row-major order with respect to the subarray.\n    - `TILEDB_GLOBAL_ORDER`:\n      This means that cells are stored or retrieved in the array global\n      cell order.\n    - `TILEDB_UNORDERED`:\n      This is applicable only to reads and writes for sparse arrays, or for\n      sparse writes to dense arrays. For writes, it specifies that the cells\n      are unordered and, hence, TileDB must sort the cells in the global cell\n      order prior to writing. For reads, TileDB will return the cells without\n      any particular order, which will often lead to better performance.\n * @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_set_layout(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        layout: tiledb_layout_t,
    ) -> i32;

    #[doc = " Sets the query condition to be applied on a read.\n\n **Example:**\n\n @code{.c}\n tiledb_query_condition_t* query_condition;\n tiledb_query_condition_alloc(ctx, &query_condition);\n uint32_t value = 5;\n tiledb_query_condition_init(\n   ctx, query_condition, \"longitude\", &value, sizeof(value), TILEDB_LT);\n tiledb_query_set_condition(ctx, query, query_condition);\n @endcode\n\n @param ctx The TileDB context.\n @param query The TileDB query.\n @param cond The TileDB query condition."]
    pub fn tiledb_query_set_condition(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        cond: *const tiledb_query_condition_t,
    ) -> i32;

    #[doc = " Flushes all internal state of a query object and finalizes the query.\n This is applicable only to global layout writes. It has no effect for\n any other query type.\n\n **Example:**\n\n @code{.c}\n tiledb_query_t* query;\n // ... Your code here ... //\n tiledb_query_finalize(ctx, query);\n @endcode\n\n @param ctx The TileDB context.\n @param query The query object to be flushed.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_finalize(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
    ) -> i32;

    #[doc = " Submits and finalizes the query.\n This is applicable only to global layout writes. The function will\n error out if called on a query with non global layout.\n Its purpose is to submit the final chunk (partial or full tile) in\n a global order write query.\n `tiledb_query_submit_and_finalize` drops the tile alignment restriction\n of the buffers (i.e. compared to the regular global layout submit call)\n given the last chunk of a global order write is most frequently smaller\n in size than a tile.\n\n **Example:**\n\n @code{.c}\n tiledb_query_t* query;\n while (stop_condition) {\n   tiledb_query_set_buffer(ctx, query, attr, tile_aligned_buffer, &size);\n   tiledb_query_submit(ctx, query);\n }\n tiledb_query_set_buffer(ctx, query, attr, final_chunk, &size);\n tiledb_query_submit_and_finalize(ctx, query);\n @endcode\n\n @param ctx The TileDB context.\n @param query The query object to be flushed.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_submit_and_finalize(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
    ) -> i32;

    #[doc = " Frees a TileDB query object.\n\n **Example:**\n\n @code{.c}\n tiledb_query_free(&query);\n @endcode\n\n @param query The query object to be deleted."]
    pub fn tiledb_query_free(query: *mut *mut tiledb_query_t);

    #[doc = " Submits a TileDB query.\n\n **Example:**\n\n @code{.c}\n tiledb_query_submit(ctx, query);\n @endcode\n\n @param ctx The TileDB context.\n @param query The query to be submitted.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error.\n\n @note `tiledb_query_finalize` must be invoked after finish writing in\n     global layout (via repeated invocations of `tiledb_query_submit`),\n     in order to flush any internal state.\n\n @note For the case of reads, if the returned status is `TILEDB_INCOMPLETE`,\n    TileDB could not fit the entire result in the user's buffers. In this\n    case, the user should consume the read results (if any), optionally\n    reset the buffers with `tiledb_query_set_buffer`, and then resubmit the\n    query until the status becomes `TILEDB_COMPLETED`. If all buffer sizes\n    after the termination of this function become 0, then this means that\n    **no** useful data was read into the buffers, implying that larger\n    buffers are needed for the query to proceed. In this case, the users\n    must reallocate their buffers (increasing their size), reset the buffers\n    with `tiledb_query_set_buffer`, and resubmit the query."]
    pub fn tiledb_query_submit(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
    ) -> i32;

    #[doc = " Submits a TileDB query in asynchronous mode.\n\n **Examples:**\n\n Submit without a callback.\n\n @code{.c}\n tiledb_query_submit_async(ctx, query, NULL, NULL);\n @endcode\n\n Submit with a callback function `print` that takes as input message\n `msg` and prints it upon completion of the query.\n\n @code{.c}\n const char* msg = \"Query completed\";\n tiledb_query_submit_async(ctx, &query, foo, msg);\n @endcode\n\n @param ctx The TileDB context.\n @param query The query to be submitted.\n @param callback The function to be called when the query completes.\n @param callback_data The data to be passed to the \\p callback function.\n @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.\n\n @note `tiledb_query_finalize` must be invoked after finish writing in\n     global layout (via repeated invocations of `tiledb_query_submit`),\n     in order to flush any internal state.\n\n @note For the case of reads, if the returned status is `TILEDB_INCOMPLETE`,\n    TileDB could not fit the entire result in the user's buffers. In this\n    case, the user should consume the read results (if any), optionally\n    reset the buffers with `tiledb_query_set_buffer`, and then resubmit the\n    query until the status becomes `TILEDB_COMPLETED`. If all buffer sizes\n    after the termination of this function become 0, then this means that\n    **no** useful data was read into the buffers, implying that larger\n    buffers are needed for the query to proceed. In this case, the users\n    must reallocate their buffers (increasing their size), reset the buffers\n    with `tiledb_query_set_buffer`, and resubmit the query.\n\n @note \\p callback will be executed in a thread managed by TileDB's internal\n    thread pool. To allow TileDB to reuse the thread and avoid starving the\n    thread pool, long-running callbacks should be dispatched to another\n    thread."]
    pub fn tiledb_query_submit_async(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        callback: ::std::option::Option<
            unsafe extern "C" fn(arg1: *mut ::std::os::raw::c_void),
        >,
        callback_data: *mut ::std::os::raw::c_void,
    ) -> i32;

    #[doc = " Checks if the query has returned any results. Applicable only to\n read queries; it sets `has_results` to `0 in the case of writes.\n\n **Example:**\n\n @code{.c}\n int32_t has_results;\n tiledb_query_has_results(ctx, query, &has_results);\n @endcode\n\n @param ctx The TileDB context.\n @param query The query.\n @param has_results Set to `1` if the query returned results and `0`\n     otherwise.\n @return `TILEDB_OK` upon success, and `TILEDB_ERR` upon error."]
    pub fn tiledb_query_has_results(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        has_results: *mut i32,
    ) -> i32;

    #[doc = " Retrieves the status of a query.\n\n **Example:**\n\n @code{.c}\n tiledb_query_status_t status;\n tiledb_query_get_status(ctx, query, &status);\n @endcode\n\n @param ctx The TileDB context.\n @param query The query.\n @param status The query status to be retrieved.\n @return `TILEDB_OK` upon success, and `TILEDB_ERR` upon error."]
    pub fn tiledb_query_get_status(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        status: *mut tiledb_query_status_t,
    ) -> i32;

    #[doc = " Retrieves the query type.\n\n **Example:**\n\n @code{.c}\n tiledb_query_type_t query_type;\n tiledb_query_get_status(ctx, query, &query_type);\n @endcode\n\n @param ctx The TileDB context.\n @param query The query.\n @param query_type The query type to be retrieved.\n @return `TILEDB_OK` upon success, and `TILEDB_ERR` upon error."]
    pub fn tiledb_query_get_type(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        query_type: *mut tiledb_query_type_t,
    ) -> i32;

    #[doc = " Retrieves the query layout.\n\n **Example:**\n\n @code{.c}\n tiledb_layout_t query_layout;\n tiledb_query_get_layout(ctx, query, &query_layout);\n @endcode\n\n @param ctx The TileDB context.\n @param query The query.\n @param query_layout The query layout to be retrieved.\n @return `TILEDB_OK` upon success, and `TILEDB_ERR` upon error."]
    pub fn tiledb_query_get_layout(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        query_layout: *mut tiledb_layout_t,
    ) -> i32;

    #[doc = " Retrieves the query array.\n\n **Example:**\n\n @code{.c}\n tiledb_array_t* array;\n tiledb_query_get_array(ctx, query, &array);\n @endcode\n\n @param ctx The TileDB context.\n @param query The query.\n @param array The query array to be retrieved.\n @return `TILEDB_OK` upon success, and `TILEDB_ERR` upon error."]
    pub fn tiledb_query_get_array(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        array: *mut *mut tiledb_array_t,
    ) -> i32;

    #[doc = " Adds a 1D range along a subarray dimension index, which is in the form\n (start, end, stride). The datatype of the range components\n must be the same as the type of the domain of the array in the query.\n\n **Example:**\n\n @code{.c}\n uint32_t dim_idx = 2;\n int64_t start = 10;\n int64_t end = 20;\n tiledb_query_add_range(ctx, query, dim_idx, &start, &end, nullptr);\n @endcode\n\n @param ctx The TileDB context.\n @param query The query to add the range to.\n @param dim_idx The index of the dimension to add the range to.\n @param start The range start.\n @param end The range end.\n @param stride The range stride.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error.\n\n @note The stride is currently unsupported. Use `nullptr` as the\n     stride argument."]
    pub fn tiledb_query_add_range(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        dim_idx: u32,
        start: *const ::std::os::raw::c_void,
        end: *const ::std::os::raw::c_void,
        stride: *const ::std::os::raw::c_void,
    ) -> i32;

    #[doc = " Adds a 1D range along a subarray dimension name, which is in the form\n (start, end, stride). The datatype of the range components\n must be the same as the type of the domain of the array in the query.\n\n **Example:**\n\n @code{.c}\n char* dim_name = \"rows\";\n int64_t start = 10;\n int64_t end = 20;\n tiledb_query_add_range_by_name(ctx, query, dim_name, &start, &end, nullptr);\n @endcode\n\n @param ctx The TileDB context.\n @param query The query to add the range to.\n @param dim_name The name of the dimension to add the range to.\n @param start The range start.\n @param end The range end.\n @param stride The range stride.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error.\n\n @note The stride is currently unsupported. Use `nullptr` as the\n     stride argument."]
    pub fn tiledb_query_add_range_by_name(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        dim_name: *const ::std::os::raw::c_char,
        start: *const ::std::os::raw::c_void,
        end: *const ::std::os::raw::c_void,
        stride: *const ::std::os::raw::c_void,
    ) -> i32;

    #[doc = " Adds a 1D variable-sized range along a subarray dimension index, which is in\n the form (start, end). Applicable only to variable-sized dimensions.\n\n **Example:**\n\n @code{.c}\n uint32_t dim_idx = 2;\n char start[] = \"a\";\n char end[] = \"bb\";\n tiledb_query_add_range_var(ctx, query, dim_idx, start, 1, end, 2);\n @endcode\n\n @param ctx The TileDB context.\n @param query The query to add the range to.\n @param dim_idx The index of the dimension to add the range to.\n @param start The range start.\n @param start_size The size of the range start in bytes.\n @param end The range end.\n @param end_size The size of the range end in bytes.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_add_range_var(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        dim_idx: u32,
        start: *const ::std::os::raw::c_void,
        start_size: u64,
        end: *const ::std::os::raw::c_void,
        end_size: u64,
    ) -> i32;

    #[doc = " Adds a 1D variable-sized range along a subarray dimension name, which is in\n the form (start, end). Applicable only to variable-sized dimensions.\n\n **Example:**\n\n @code{.c}\n char* dim_name = \"rows\";\n char start[] = \"a\";\n char end[] = \"bb\";\n tiledb_query_add_range_var_by_name(ctx, query, dim_name, start, 1, end, 2);\n @endcode\n\n @param ctx The TileDB context.\n @param query The query to add the range to.\n @param dim_name The name of the dimension to add the range to.\n @param start The range start.\n @param start_size The size of the range start in bytes.\n @param end The range end.\n @param end_size The size of the range end in bytes.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_add_range_var_by_name(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        dim_name: *const ::std::os::raw::c_char,
        start: *const ::std::os::raw::c_void,
        start_size: u64,
        end: *const ::std::os::raw::c_void,
        end_size: u64,
    ) -> i32;

    #[doc = " Retrieves the number of ranges of the query subarray along a given dimension\n index.\n\n **Example:**\n\n @code{.c}\n uint64_t range_num;\n tiledb_query_get_range_num(ctx, query, dim_idx, &range_num);\n @endcode\n\n @param ctx The TileDB context\n @param query The query.\n @param dim_idx The index of the dimension whose range number to retrieve.\n @param range_num The number of ranges to retrieve.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_get_range_num(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        dim_idx: u32,
        range_num: *mut u64,
    ) -> i32;

    #[doc = " Retrieves the number of ranges of the query subarray along a given dimension\n name.\n\n **Example:**\n\n @code{.c}\n uint64_t range_num;\n tiledb_query_get_range_num_from_name(ctx, query, dim_name, &range_num);\n @endcode\n\n @param ctx The TileDB context\n @param query The query.\n @param dim_name The name of the dimension whose range number to retrieve.\n @param range_num The number of ranges to retrieve.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_get_range_num_from_name(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        dim_name: *const ::std::os::raw::c_char,
        range_num: *mut u64,
    ) -> i32;

    #[doc = " Retrieves a specific range of the query subarray along a given dimension\n index.\n\n **Example:**\n\n @code{.c}\n const void* start;\n const void* end;\n const void* stride;\n tiledb_query_get_range(\n     ctx, query, dim_idx, range_idx, &start, &end, &stride);\n @endcode\n\n @param ctx The TileDB context\n @param query The query.\n @param dim_idx The index of the dimension to retrieve the range from.\n @param range_idx The index of the range to retrieve.\n @param start The range start to retrieve.\n @param end The range end to retrieve.\n @param stride The range stride to retrieve.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_get_range(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        dim_idx: u32,
        range_idx: u64,
        start: *mut *const ::std::os::raw::c_void,
        end: *mut *const ::std::os::raw::c_void,
        stride: *mut *const ::std::os::raw::c_void,
    ) -> i32;

    #[doc = " Retrieves a specific range of the query subarray along a given dimension\n name.\n\n **Example:**\n\n @code{.c}\n const void* start;\n const void* end;\n const void* stride;\n tiledb_query_get_range_from_name(\n     ctx, query, dim_name, range_idx, &start, &end, &stride);\n @endcode\n\n @param ctx The TileDB context\n @param query The query.\n @param dim_name The name of the dimension to retrieve the range from.\n @param range_idx The index of the range to retrieve.\n @param start The range start to retrieve.\n @param end The range end to retrieve.\n @param stride The range stride to retrieve.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_get_range_from_name(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        dim_name: *const ::std::os::raw::c_char,
        range_idx: u64,
        start: *mut *const ::std::os::raw::c_void,
        end: *mut *const ::std::os::raw::c_void,
        stride: *mut *const ::std::os::raw::c_void,
    ) -> i32;

    #[doc = " Retrieves a range's start and end size for a given variable-length\n dimension index at a given range index.\n\n **Example:**\n\n @code{.c}\n uint64_t start_size;\n uint64_t end_size;\n tiledb_query_get_range_var_size(\n     ctx, query, dim_idx, range_idx, &start_size, &end_size);\n @endcode\n\n @param ctx The TileDB context\n @param query The query.\n @param dim_idx The index of the dimension to retrieve the range from.\n @param range_idx The index of the range to retrieve.\n @param start_size range start size in bytes\n @param end_size range end size in bytes\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_get_range_var_size(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        dim_idx: u32,
        range_idx: u64,
        start_size: *mut u64,
        end_size: *mut u64,
    ) -> i32;

    #[doc = " Retrieves a range's start and end size for a given variable-length\n dimension name at a given range index.\n\n **Example:**\n\n @code{.c}\n uint64_t start_size;\n uint64_t end_size;\n tiledb_query_get_range_var_size_from_name(\n     ctx, query, dim_name, range_idx, &start_size, &end_size);\n @endcode\n\n @param ctx The TileDB context\n @param query The query.\n @param dim_name The name of the dimension to retrieve the range from.\n @param range_idx The index of the range to retrieve.\n @param start_size range start size in bytes\n @param end_size range end size in bytes\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_get_range_var_size_from_name(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        dim_name: *const ::std::os::raw::c_char,
        range_idx: u64,
        start_size: *mut u64,
        end_size: *mut u64,
    ) -> i32;

    #[doc = " Retrieves a specific range of the query subarray along a given\n variable-length dimension index.\n\n **Example:**\n\n @code{.c}\n const void* start;\n const void* end;\n tiledb_query_get_range_var(\n     ctx, query, dim_idx, range_idx, &start, &end);\n @endcode\n\n @param ctx The TileDB context\n @param query The query.\n @param dim_idx The index of the dimension to retrieve the range from.\n @param range_idx The index of the range to retrieve.\n @param start The range start to retrieve.\n @param end The range end to retrieve.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_get_range_var(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        dim_idx: u32,
        range_idx: u64,
        start: *mut ::std::os::raw::c_void,
        end: *mut ::std::os::raw::c_void,
    ) -> i32;

    #[doc = " Retrieves a specific range of the query subarray along a given\n variable-length dimension name.\n\n **Example:**\n\n @code{.c}\n const void* start;\n const void* end;\n tiledb_query_get_range_var_from_name(\n     ctx, query, dim_name, range_idx, &start, &end);\n @endcode\n\n @param ctx The TileDB context\n @param query The query.\n @param dim_name The name of the dimension to retrieve the range from.\n @param range_idx The index of the range to retrieve.\n @param start The range start to retrieve.\n @param end The range end to retrieve.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_get_range_var_from_name(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        dim_name: *const ::std::os::raw::c_char,
        range_idx: u64,
        start: *mut ::std::os::raw::c_void,
        end: *mut ::std::os::raw::c_void,
    ) -> i32;

    #[doc = " Retrieves the estimated result size for a fixed-sized attribute/dimension.\n This is an estimate and may not be sufficient to read all results for the\n requested range, in particular for sparse arrays or array with\n var-length attributes.\n Query status must be checked and resubmitted if not complete.\n\n **Example:**\n\n @code{.c}\n uint64_t size;\n tiledb_query_get_est_result_size(ctx, query, \"a\", &size);\n @endcode\n\n @param ctx The TileDB context\n @param query The query.\n @param name The attribute/dimension name.\n @param size The size (in bytes) to be retrieved.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_get_est_result_size(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        name: *const ::std::os::raw::c_char,
        size: *mut u64,
    ) -> i32;

    #[doc = " Retrieves the estimated result size for a var-sized attribute/dimension.\n This is an estimate and may not be sufficient to read all results for the\n requested range, for sparse arrays or any array with\n var-length attributes.\n Query status must be checked and resubmitted if not complete.\n\n **Example:**\n\n @code{.c}\n uint64_t size_off, size_val;\n tiledb_query_get_est_result_size_var(\n     ctx, query, \"a\", &size_off, &size_val);\n @endcode\n\n @param ctx The TileDB context\n @param query The query.\n @param name The attribute/dimension name.\n @param size_off The size of the offsets (in bytes) to be retrieved.\n @param size_val The size of the values (in bytes) to be retrieved.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_get_est_result_size_var(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        name: *const ::std::os::raw::c_char,
        size_off: *mut u64,
        size_val: *mut u64,
    ) -> i32;

    #[doc = " Retrieves the estimated result size for a fixed-sized, nullable attribute.\n This is an estimate and may not be sufficient to read all results for the\n requested range, for sparse arrays or any array with\n var-length attributes.\n Query status must be checked and resubmitted if not complete.\n\n **Example:**\n\n @code{.c}\n uint64_t size_val;\n uint64_t size_validity;\n tiledb_query_get_est_result_size_nullable(ctx, query, \"a\", &size_val,\n &size_validity);\n @endcode\n\n @param ctx The TileDB context\n @param query The query.\n @param name The attribute name.\n @param size_val The size of the values (in bytes) to be retrieved.\n @param size_validity The size of the validity values (in bytes) to be\n retrieved.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_get_est_result_size_nullable(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        name: *const ::std::os::raw::c_char,
        size_val: *mut u64,
        size_validity: *mut u64,
    ) -> i32;

    #[doc = " Retrieves the estimated result size for a var-sized, nullable attribute.\n\n **Example:**\n\n @code{.c}\n uint64_t size_off, size_val, size_validity;\n tiledb_query_get_est_result_size_var_nullable(\n     ctx, query, \"a\", &size_off, &size_val, &size_validity);\n @endcode\n\n @param ctx The TileDB context\n @param query The query.\n @param name The attribute name.\n @param size_off The size of the offsets (in bytes) to be retrieved.\n @param size_val The size of the values (in bytes) to be retrieved.\n @param size_validity The size of the validity values (in bytes) to be\n retrieved.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_get_est_result_size_var_nullable(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        name: *const ::std::os::raw::c_char,
        size_off: *mut u64,
        size_val: *mut u64,
        size_validity: *mut u64,
    ) -> i32;

    #[doc = " Retrieves the number of written fragments. Applicable only to WRITE\n queries.\n\n **Example:**\n\n @code{.c}\n uint32_t num;\n tiledb_query_get_fragment_num(ctx, query, &num);\n @endcode\n\n @param ctx The TileDB context\n @param query The query.\n @param num The number of written fragments to be retrieved.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_get_fragment_num(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        num: *mut u32,
    ) -> i32;

    #[doc = " Retrieves the URI of the written fragment with the input index. Applicable\n only to WRITE queries.\n\n **Example:**\n\n @code{.c}\n const char* uri;\n tiledb_query_get_fragment_uri(\n     ctx, query, 0, &uri);\n @endcode\n\n @param ctx The TileDB context\n @param query The query.\n @param idx The index of the written fragment.\n @param uri The URI of the written fragment to be returned.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error.\n\n @note Make sure to make a copy of `uri` after its retrieval, as the\n     constant pointer may be updated internally as new fragments\n     are being written."]
    pub fn tiledb_query_get_fragment_uri(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        idx: u64,
        uri: *mut *const ::std::os::raw::c_char,
    ) -> i32;

    #[doc = " Retrieves the timestamp range of the written fragment with the input index.\n Applicable only to WRITE queries.\n\n **Example:**\n\n @code{.c}\n uint64_t t1, t2;\n tiledb_query_get_fragment_timestamp_range(\n     ctx, query, 0, &t1, &t2);\n @endcode\n\n @param ctx The TileDB context\n @param query The query.\n @param idx The index of the written fragment.\n @param t1 The start value of the timestamp range of the\n     written fragment to be returned.\n @param t2 The end value of the timestamp range of the\n     written fragment to be returned.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_query_get_fragment_timestamp_range(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        idx: u64,
        t1: *mut u64,
        t2: *mut u64,
    ) -> i32;

    #[doc = " Return a TileDB subarray object from the given query.\n\n **Example:**\n\n @code{.c}\n tiledb_subarray_t* subarray;\n tiledb_query_get_subarray_t(array, &subarray);\n @endcode\n\n @param ctx The TileDB context.\n @param query An open Query object.\n @param subarray The retrieved subarray object if available.\n @return `TILEDB_OK` for success or `TILEDB_OOM` or `TILEDB_ERR` for error."]
    pub fn tiledb_query_get_subarray_t(
        ctx: *mut tiledb_ctx_t,
        query: *const tiledb_query_t,
        subarray: *mut *mut tiledb_subarray_t,
    ) -> i32;
}
