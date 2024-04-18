// N.B., This file is not processed by cargo/rustc and only exists so that we
// can eventually assert in CI that all of the functions that bindgen generates
// are covered by our bindings.

// This is a list of constants that we are ignoring.

// We use the tiledb_version function instead.
pub const TILEDB_VERSION_MAJOR: u32 = 2;
pub const TILEDB_VERSION_MINOR: u32 = 21;
pub const TILEDB_VERSION_PATCH: u32 = 0;

// This is a list of functions that we are currently planning on not wrapping.

extern "C" {

    // The dump functions aren't being wrapped because Rust makes it really easy
    // to write Debug traits that will dump everything as a JSON string. The dump
    // functions just write free form ASCII to a file handle which isn't nearly
    // as useful.

    pub fn tiledb_attribute_dump(
        ctx: *mut tiledb_ctx_t,
        attr: *const tiledb_attribute_t,
        out: *mut FILE,
    ) -> i32;

    pub fn tiledb_array_schema_dump(
        ctx: *mut tiledb_ctx_t,
        array_schema: *const tiledb_array_schema_t,
        out: *mut FILE,
    ) -> i32;

    pub fn tiledb_as_built_dump(
        out: *mut *mut tiledb_string_t,
    ) -> capi_return_t;

    pub fn tiledb_dimension_dump(
        ctx: *mut tiledb_ctx_t,
        dim: *const tiledb_dimension_t,
        out: *mut FILE,
    ) -> i32;

    pub fn tiledb_domain_dump(
        ctx: *mut tiledb_ctx_t,
        domain: *const tiledb_domain_t,
        out: *mut FILE,
    ) -> i32;

    pub fn tiledb_enumeration_dump(
        ctx: *mut tiledb_ctx_t,
        enumeration: *mut tiledb_enumeration_t,
        out: *mut FILE,
    ) -> capi_return_t;

    pub fn tiledb_stats_dump(out: *mut FILE) -> i32;
    pub fn tiledb_stats_raw_dump(out: *mut FILE) -> i32;

    // This is an exact duplicate of tiledb_stats_dump_str
    pub fn tiledb_stats_raw_dump_str(
        out: *mut *mut ::std::os::raw::c_char,
    ) -> i32;

    // The tiledb_handle_* functions are for internal use. They should probably be
    // part of a library separate from libtiledb.{dylib,so,dll} but for now they're
    // just lumped in.

    pub fn tiledb_handle_array_delete_fragments_timestamps_request(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        serialization_type: tiledb_serialization_type_t,
        request: *const tiledb_buffer_t,
    ) -> capi_return_t;

    pub fn tiledb_handle_array_delete_fragments_list_request(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        serialization_type: tiledb_serialization_type_t,
        request: *const tiledb_buffer_t,
    ) -> capi_return_t;

    pub fn tiledb_handle_consolidation_plan_request(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        serialization_type: tiledb_serialization_type_t,
        request: *const tiledb_buffer_t,
        response: *mut tiledb_buffer_t,
    ) -> capi_return_t;

    pub fn tiledb_handle_load_array_schema_request(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        serialization_type: tiledb_serialization_type_t,
        request: *const tiledb_buffer_t,
        response: *mut tiledb_buffer_t,
    ) -> capi_return_t;

    pub fn tiledb_handle_load_enumerations_request(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        serialization_type: tiledb_serialization_type_t,
        request: *const tiledb_buffer_t,
        response: *mut tiledb_buffer_t,
    ) -> capi_return_t;

    pub fn tiledb_handle_query_plan_request(
        ctx: *mut tiledb_ctx_t,
        array: *mut tiledb_array_t,
        serialization_type: tiledb_serialization_type_t,
        request: *const tiledb_buffer_t,
        response: *mut tiledb_buffer_t,
    ) -> capi_return_t;

    pub fn tiledb_heap_profiler_enable(
        file_name_prefix: *const ::std::os::raw::c_char,
        dump_interval_ms: u64,
        dump_interval_bytes: u64,
        dump_threshold_bytes: u64,
    ) -> i32;

    // Resetting iterators doesn't really work given Rust's iterator APIs. If we
    // ever do need this we can always just wrap it when we get to that point.

    pub fn tiledb_config_iter_reset(
        config: *mut tiledb_config_t,
        config_iter: *mut tiledb_config_iter_t,
        prefix: *const ::std::os::raw::c_char,
        error: *mut *mut tiledb_error_t,
    ) -> capi_return_t;

    // Ignoring the async tasks as those likely won't be useful in Rust land given
    // they don't at all map to async Rust.

    pub fn tiledb_ctx_cancel_tasks(ctx: *mut tiledb_ctx_t) -> capi_return_t;

    pub fn tiledb_query_submit_async(
        ctx: *mut tiledb_ctx_t,
        query: *mut tiledb_query_t,
        callback: ::std::option::Option<
            unsafe extern "C" fn(arg1: *mut ::std::os::raw::c_void),
        >,
        callback_data: *mut ::std::os::raw::c_void,
    ) -> i32;

    // Filter types are not part of the public Rust API and the filter API's types
    // already have their Debug traits implemented.

    pub fn tiledb_filter_option_to_str(
        filter_option: tiledb_filter_option_t,
        str_: *mut *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_filter_option_from_str(
        str_: *const ::std::os::raw::c_char,
        filter_option: *mut tiledb_filter_option_t,
    ) -> capi_return_t;

}
