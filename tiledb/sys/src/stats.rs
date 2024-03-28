extern "C" {
    pub fn tiledb_stats_enable() -> i32;

    pub fn tiledb_stats_disable() -> i32;

    pub fn tiledb_stats_reset() -> i32;

    //pub fn tiledb_stats_dump(out: *mut libc::FILE) -> i32;

    pub fn tiledb_stats_dump_str(out: *mut *mut ::std::ffi::c_char) -> i32;

    //pub fn tiledb_stats_raw_dump(out: *mut libc::FILE) -> i32;

    pub fn tiledb_stats_raw_dump_str(out: *mut *mut ::std::ffi::c_char) -> i32;

    pub fn tiledb_stats_free_str(out: *mut *mut ::std::ffi::c_char) -> i32;
}
