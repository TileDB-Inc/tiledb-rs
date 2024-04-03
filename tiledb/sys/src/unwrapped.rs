// N.B., This file is not processed by cargo/rustc and only exists so that we
// can eventually assert in CI that all of the functions that bindgen generates
// are covered by our bindings. Eventually we'll also want to look into
// including constants as well.

// This is a list of functions that we are currently planning on not wrapping.

// The dump functions aren't being wrapped because Rust makes it really easy
// to write Debug traits that will dump everything as a JSON string. The dump
// functions just write free form ASCII to a file handle which isn't nearly
// as useful.
//
// fn tiledb_attribute_dump
// fn tiledb_array_schema_dump
// fn tiledb_as_built_dump
// fn tiledb_dimension_dump
// fn tiledb_domain_dump
// fn tiledb_enumeration_dump
// fn tiledb_stats_dump
// fn tiledb_stats_raw_dump
// fn tiledb_stats_raw_dump_str - This is a duplicate of tildb_stats_dump_str

// The tiledb_handle_* functions are for internal use. They should probably be
// part of a library separate from libtiledb.{dylib,so,dll} but for now they're
// just lumped in.
//
// fn tiledb_handle_array_delete_fragments_list_request
// fn tiledb_handle_array_delete_fragments_timestamps_request
// fn tiledb_handle_consolidation_plan_request
// fn tiledb_handle_load_array_schema_request
// fn tiledb_handle_load_enumerations_request
// fn tiledb_handle_query_plan_request
// fn tiledb_heap_profiler_enable

// Resetting iterators doesn't really work given Rust's iterator APIs. If we
// ever do need this we can always just wrap it when we get to that point.
//
// fn tiledb_config_iter_reset

// Ignoring the async tasks as those likely won't be useful in Rust land given
// they don't at all map to async Rust.
//
// fn tiledb_ctx_cancel_tasks
// fn tiledb_query_submit_async

// Filter types are not part of the public Rust API and the filter API's types
// already have their Debug traits implemented.
//
// fn tiledb_filter_option_to_str
// tiledb_filter_option_from_str
