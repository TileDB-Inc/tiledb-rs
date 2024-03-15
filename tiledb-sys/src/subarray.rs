use crate::types::{
    tiledb_array_t, tiledb_config_t, tiledb_ctx_t, tiledb_subarray_t,
};

extern "C" {
    #[doc = " Allocates a TileDB subarray object.\n\n **Example:**\n\n @code{.c}\n tiledb_subarray_t* subarray;\n tiledb_subarray_alloc(ctx, array, &subarray);\n @endcode\n\n @param ctx The TileDB context.\n @param array An open array object.\n @param subarray The subarray object to be created.\n @return `TILEDB_OK` for success or `TILEDB_OOM` or `TILEDB_ERR` for error.\n\n @note The allocated subarray initially has internal coalesce_ranges == true."]
    pub fn tiledb_subarray_alloc(
        ctx: *mut tiledb_ctx_t,
        array: *const tiledb_array_t,
        subarray: *mut *mut tiledb_subarray_t,
    ) -> i32;

    #[doc = " Set the subarray config.\n\n Setting the configuration with this function overrides the following\n Subarray-level parameters only:\n\n - `sm.read_range_oob`"]
    pub fn tiledb_subarray_set_config(
        ctx: *mut tiledb_ctx_t,
        subarray: *mut tiledb_subarray_t,
        config: *mut tiledb_config_t,
    ) -> i32;

    #[doc = " Frees a TileDB subarray object.\n\n **Example:**\n\n @code{.c}\n tiledb_subarray_t* subarray;\n tiledb_array_open(ctx, array, TILEDB_READ);\n tiledb_subarray_alloc(ctx, array, &subarray);\n tiledb_array_close(ctx, array);\n tiledb_subarray_free(&subarray);\n @endcode\n\n @param subarray The subarray object to be freed."]
    pub fn tiledb_subarray_free(subarray: *mut *mut tiledb_subarray_t);

    #[doc = " Set coalesce_ranges property on a TileDB subarray object.\n Intended to be used just after tiledb_subarray_alloc() to replace\n the initial coalesce_ranges == true\n with coalesce_ranges = false if\n needed.\n\n **Example:**\n\n @code{.c}\n tiledb_subarray_t* subarray;\n //tiledb_subarray_alloc internally defaults to 'coalesce_ranges == true'\n tiledb_subarray_alloc(ctx, array, &subarray);\n // so manually set to 'false' to match earlier behaviour with older\n // tiledb_query_ subarray actions.\n bool coalesce_ranges = false;\n tiledb_subarray_set_coalesce_ranges(ctx, subarray, coalesce_ranges);\n @endcode\n\n @param ctx The TileDB context.\n @param subarray The subarray object to change.\n @param coalesce_ranges The true/false value to be set\n @return `TILEDB_OK` for success or `TILEDB_ERR` for error."]
    pub fn tiledb_subarray_set_coalesce_ranges(
        ctx: *mut tiledb_ctx_t,
        subarray: *mut tiledb_subarray_t,
        coalesce_ranges: ::std::os::raw::c_int,
    ) -> i32;

    #[doc = " Populates a subarray with specific indicies.\n\n **Example:**\n\n The following sets a 2D subarray [0,10], [20, 30] to the subarray.\n\n @code{.c}\n tiledb_subarray_t *subarray;\n uint64_t subarray_v[] = { 0, 10, 20, 30};\n tiledb_subarray_set_subarray(ctx, subarray, subarray_v);\n @endcode\n\n @param ctx The TileDB context.\n @param subarray The TileDB subarray object.\n @param subarray_v The subarray values which can be used to limit the subarray\n read/write.\n     It should be a sequence of [low, high] pairs (one pair per dimension).\n     When the subarray is used for writes, this is meaningful only\n     for dense arrays, and specifically dense writes. Note that `subarray_a`\n     must have the same type as the domain of the subarray's associated\n     array.\n @return `TILEDB_OK` for success or `TILEDB_ERR` for error."]
    pub fn tiledb_subarray_set_subarray(
        ctx: *mut tiledb_ctx_t,
        subarray: *mut tiledb_subarray_t,
        subarray_v: *const ::std::os::raw::c_void,
    ) -> i32;

    #[doc = " Adds a 1D range along a subarray dimension index, which is in the form\n (start, end, stride). The datatype of the range components\n must be the same as the type of the domain of the array in the query.\n\n **Example:**\n\n @code{.c}\n uint32_t dim_idx = 2;\n int64_t start = 10;\n int64_t end = 20;\n tiledb_subarray_add_range(ctx, subarray, dim_idx, &start, &end, nullptr);\n @endcode\n\n @param ctx The TileDB context.\n @param subarray The subarray to add the range to.\n @param dim_idx The index of the dimension to add the range to.\n @param start The range start.\n @param end The range end.\n @param stride The range stride.\n @return `TILEDB_OK` for success or `TILEDB_ERR` for error.\n\n @note The stride is currently unsupported. Use 0/NULL/nullptr as the\n     stride argument."]
    pub fn tiledb_subarray_add_range(
        ctx: *mut tiledb_ctx_t,
        subarray: *mut tiledb_subarray_t,
        dim_idx: u32,
        start: *const ::std::os::raw::c_void,
        end: *const ::std::os::raw::c_void,
        stride: *const ::std::os::raw::c_void,
    ) -> i32;

    #[doc = " Adds a 1D range along a subarray dimension name, which is in the form\n (start, end, stride). The datatype of the range components\n must be the same as the type of the domain of the array in the query.\n\n **Example:**\n\n @code{.c}\n char* dim_name = \"rows\";\n int64_t start = 10;\n int64_t end = 20;\n tiledb_subarray_add_range_by_name(\n     ctx, subarray, dim_name, &start, &end, nullptr);\n @endcode\n\n @param ctx The TileDB context.\n @param subarray The subarray to add the range to.\n @param dim_name The name of the dimension to add the range to.\n @param start The range start.\n @param end The range end.\n @param stride The range stride.\n @return `TILEDB_OK` for success or `TILEDB_ERR` for error.\n\n @note The stride is currently unsupported. Use 0/NULL/nullptr as the\n     stride argument."]
    pub fn tiledb_subarray_add_range_by_name(
        ctx: *mut tiledb_ctx_t,
        subarray: *mut tiledb_subarray_t,
        dim_name: *const ::std::os::raw::c_char,
        start: *const ::std::os::raw::c_void,
        end: *const ::std::os::raw::c_void,
        stride: *const ::std::os::raw::c_void,
    ) -> i32;

    #[doc = " Adds a 1D variable-sized range along a subarray dimension index, which is in\n the form (start, end). Applicable only to variable-sized dimensions.\n\n **Example:**\n\n @code{.c}\n uint32_t dim_idx = 2;\n char start[] = \"a\";\n char end[] = \"bb\";\n tiledb_subarray_add_range_var(ctx, subarray, dim_idx, start, 1, end, 2);\n @endcode\n\n @param ctx The TileDB context.\n @param subarray The subarray to add the range to.\n @param dim_idx The index of the dimension to add the range to.\n @param start The range start.\n @param start_size The size of the range start in bytes.\n @param end The range end.\n @param end_size The size of the range end in bytes.\n @return `TILEDB_OK` for success or `TILEDB_ERR` for error."]
    pub fn tiledb_subarray_add_range_var(
        ctx: *mut tiledb_ctx_t,
        subarray: *mut tiledb_subarray_t,
        dim_idx: u32,
        start: *const ::std::os::raw::c_void,
        start_size: u64,
        end: *const ::std::os::raw::c_void,
        end_size: u64,
    ) -> i32;

    #[doc = " Adds a 1D variable-sized range along a subarray dimension name, which is in\n the form (start, end). Applicable only to variable-sized dimensions.\n\n **Example:**\n\n @code{.c}\n char* dim_name = \"rows\";\n char start[] = \"a\";\n char end[] = \"bb\";\n tiledb_subarray_add_range_var_by_name(\n     ctx, subarray, dim_name, start, 1, end, 2);\n @endcode\n\n @param ctx The TileDB context.\n @param subarray The subarray to add the range to.\n @param dim_name The name of the dimension to add the range to.\n @param start The range start.\n @param start_size The size of the range start in bytes.\n @param end The range end.\n @param end_size The size of the range end in bytes.\n @return `TILEDB_OK` for success or `TILEDB_ERR` for error."]
    pub fn tiledb_subarray_add_range_var_by_name(
        ctx: *mut tiledb_ctx_t,
        subarray: *mut tiledb_subarray_t,
        dim_name: *const ::std::os::raw::c_char,
        start: *const ::std::os::raw::c_void,
        start_size: u64,
        end: *const ::std::os::raw::c_void,
        end_size: u64,
    ) -> i32;

    #[doc = " Retrieves the number of ranges of the query subarray along a given dimension\n index.\n\n **Example:**\n\n @code{.c}\n uint64_t range_num;\n tiledb_subarray_get_range_num(ctx, subarray, dim_idx, &range_num);\n @endcode\n\n @param ctx The TileDB context\n @param subarray The subarray.\n @param dim_idx The index of the dimension for which to retrieve number of\n ranges.\n @param range_num Receives the retrieved number of ranges.\n @return `TILEDB_OK` for success or `TILEDB_ERR` for error."]
    pub fn tiledb_subarray_get_range_num(
        ctx: *mut tiledb_ctx_t,
        subarray: *const tiledb_subarray_t,
        dim_idx: u32,
        range_num: *mut u64,
    ) -> i32;

    #[doc = " Retrieves the number of ranges of the subarray along a given dimension\n name.\n\n **Example:**\n\n @code{.c}\n uint64_t range_num;\n tiledb_subarray_get_range_num_from_name(ctx, subarray, dim_name, &range_num);\n @endcode\n\n @param ctx The TileDB context\n @param subarray The subarray.\n @param dim_name The name of the dimension whose range number to retrieve.\n @param range_num Receives the retrieved number of ranges.\n @return `TILEDB_OK` for success or `TILEDB_ERR` for error."]
    pub fn tiledb_subarray_get_range_num_from_name(
        ctx: *mut tiledb_ctx_t,
        subarray: *const tiledb_subarray_t,
        dim_name: *const ::std::os::raw::c_char,
        range_num: *mut u64,
    ) -> i32;

    #[doc = " Retrieves a specific range of the subarray along a given dimension\n index.\n\n **Example:**\n\n @code{.c}\n const void* start;\n const void* end;\n const void* stride;\n tiledb_subarray_get_range(\n     ctx, subarray, dim_idx, range_idx, &start, &end, &stride);\n @endcode\n\n @param ctx The TileDB context\n @param subarray The subarray.\n @param dim_idx The index of the dimension to retrieve the range from.\n @param range_idx The index of the range to retrieve.\n @param start Receives the retrieved range start.\n @param end Receives the received range end.\n @param stride Receives the retrieved range stride.\n @return `TILEDB_OK` for success or `TILEDB_ERR` for error."]
    pub fn tiledb_subarray_get_range(
        ctx: *mut tiledb_ctx_t,
        subarray: *const tiledb_subarray_t,
        dim_idx: u32,
        range_idx: u64,
        start: *mut *const ::std::os::raw::c_void,
        end: *mut *const ::std::os::raw::c_void,
        stride: *mut *const ::std::os::raw::c_void,
    ) -> i32;

    #[doc = " Retrieves a specific range of the subarray along a given dimension\n name.\n\n **Example:**\n\n @code{.c}\n const void* start;\n const void* end;\n const void* stride;\n tiledb_subarray_get_range_from_name(\n     ctx, query, dim_name, range_idx, &start, &end, &stride);\n @endcode\n\n @param ctx The TileDB context\n @param subarray The subarray.\n @param dim_name The name of the dimension to retrieve the range from.\n @param range_idx The index of the range to retrieve.\n @param start Receives the retrieved range start.\n @param end Receives the retrieved range end.\n @param stride Receives the retrieved range stride.\n @return `TILEDB_OK` for success or `TILEDB_ERR` for error."]
    pub fn tiledb_subarray_get_range_from_name(
        ctx: *mut tiledb_ctx_t,
        subarray: *const tiledb_subarray_t,
        dim_name: *const ::std::os::raw::c_char,
        range_idx: u64,
        start: *mut *const ::std::os::raw::c_void,
        end: *mut *const ::std::os::raw::c_void,
        stride: *mut *const ::std::os::raw::c_void,
    ) -> i32;

    #[doc = " Retrieves a range's start and end size for a given variable-length\n dimension index at a given range index.\n\n **Example:**\n\n @code{.c}\n uint64_t start_size;\n uint64_t end_size;\n tiledb_subarray_get_range_var_size(\n     ctx, subarray, dim_idx, range_idx, &start_size, &end_size);\n @endcode\n\n @param ctx The TileDB context\n @param subarray The subarray.\n @param dim_idx The index of the dimension to retrieve the range from.\n @param range_idx The index of the range to retrieve.\n @param start_size Receives the retrieved range start size in bytes\n @param end_size Receives the retrieved range end size in bytes\n @return `TILEDB_OK` for success or `TILEDB_ERR` for error."]
    pub fn tiledb_subarray_get_range_var_size(
        ctx: *mut tiledb_ctx_t,
        subarray: *const tiledb_subarray_t,
        dim_idx: u32,
        range_idx: u64,
        start_size: *mut u64,
        end_size: *mut u64,
    ) -> i32;

    #[doc = " Retrieves a range's start and end size for a given variable-length\n dimension name at a given range index.\n\n **Example:**\n\n @code{.c}\n uint64_t start_size;\n uint64_t end_size;\n tiledb_subarray_get_range_var_size_from_name(\n     ctx, subarray, dim_name, range_idx, &start_size, &end_size);\n @endcode\n\n @param ctx The TileDB context\n @param subarray The subarray.\n @param dim_name The name of the dimension to retrieve the range from.\n @param range_idx The index of the range to retrieve.\n @param start_size Receives the retrieved range start size in bytes\n @param end_size Receives the retrieved range end size in bytes\n @return `TILEDB_OK` for success or `TILEDB_ERR` for error."]
    pub fn tiledb_subarray_get_range_var_size_from_name(
        ctx: *mut tiledb_ctx_t,
        subarray: *const tiledb_subarray_t,
        dim_name: *const ::std::os::raw::c_char,
        range_idx: u64,
        start_size: *mut u64,
        end_size: *mut u64,
    ) -> i32;

    #[doc = " Retrieves a specific range of the subarray along a given\n variable-length dimension index.\n\n **Example:**\n\n @code{.c}\n const void* start;\n const void* end;\n tiledb_subarray_get_range_var(\n     ctx, subarray, dim_idx, range_idx, &start, &end);\n @endcode\n\n @param ctx The TileDB context\n @param subarray The subarray.\n @param dim_idx The index of the dimension to retrieve the range from.\n @param range_idx The index of the range to retrieve.\n @param start Receives the retrieved range start.\n @param end Receives the retrieved range end.\n @return `TILEDB_OK` for success or `TILEDB_ERR` for error."]
    pub fn tiledb_subarray_get_range_var(
        ctx: *mut tiledb_ctx_t,
        subarray: *const tiledb_subarray_t,
        dim_idx: u32,
        range_idx: u64,
        start: *mut ::std::os::raw::c_void,
        end: *mut ::std::os::raw::c_void,
    ) -> i32;

    #[doc = " Retrieves a specific range of the subarray along a given\n variable-length dimension name.\n\n **Example:**\n\n @code{.c}\n const void* start;\n const void* end;\n tiledb_subarray_get_range_var_from_name(\n     ctx, subarray, dim_name, range_idx, &start, &end);\n @endcode\n\n @param ctx The TileDB context\n @param subarray The subarray.\n @param dim_name The name of the dimension to retrieve the range from.\n @param range_idx The index of the range to retrieve.\n @param start Receives the retrieved range start.\n @param end Receives the retrieved range end.\n @return `TILEDB_OK` for success or `TILEDB_ERR` for error."]
    pub fn tiledb_subarray_get_range_var_from_name(
        ctx: *mut tiledb_ctx_t,
        subarray: *const tiledb_subarray_t,
        dim_name: *const ::std::os::raw::c_char,
        range_idx: u64,
        start: *mut ::std::os::raw::c_void,
        end: *mut ::std::os::raw::c_void,
    ) -> i32;
}
