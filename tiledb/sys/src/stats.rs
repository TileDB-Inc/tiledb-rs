unsafe extern "C" {
    pub fn tiledb_stats_enable() -> i32;
    pub fn tiledb_stats_disable() -> i32;
    pub fn tiledb_stats_is_enabled(enabled: *mut u8) -> i32;
    pub fn tiledb_stats_reset() -> i32;
    pub fn tiledb_stats_dump_str(out: *mut *mut ::std::os::raw::c_char) -> i32;
    pub fn tiledb_stats_free_str(out: *mut *mut ::std::os::raw::c_char) -> i32;
}
