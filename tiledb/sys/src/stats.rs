extern "C" {
    #[doc = "Enable internal statistics gathering. \n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_stats_enable() -> i32;

    #[doc = "Disable internal statistics gathering. \n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_stats_disable() -> i32;

    #[doc = "Reset all internal statistics counters to 0. \n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_stats_reset() -> i32;

    //#[doc = "Dump all internal statistics counters to some output (e.g., file or stdout). \n @param out The output. \n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    //pub fn tiledb_stats_dump(out: *mut libc::FILE) -> i32;

    #[doc = "Dump all internal statistics counters to an output string. The caller is responsible for freeing the resulting string. \n Example: \n @code{.c} \n char *stats_str; \n tiledb_stats_dump_str(&stats_str); \n // ... \n tiledb_stats_free_str(&stats_str);\n @endcode \n @param out Will be set to point to an allocated string containing the stats. \n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_stats_dump_str(out : *mut *mut ::std::os::raw::c_char) -> i32;

    // #[doc = "Dump all raw internal statistics counters to some output (e.g., file or stdout) as a JSON. \n @param out The output. \n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    //pub fn tiledb_stats_raw_dump(out: *mut libc::FILE) -> i32;

    #[doc = "Dump all raw internal statistics counters to a JSON-formatted output string. The caller is responsible for freeing the resulting string. \n Example: \n @code{.c} \n char *stats_str; \n tiledb_stats_raw_dump_str(&stats_str); \n // ... \n tiledb_stats_raw_free_str(&stats_str); \n @endcode \n @param out Will be set to point to an allocated string containing the stats. \n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_stats_raw_dump_str(out : *mut *mut ::std::os::raw::c_char) -> i32;
    
    #[doc = "Free the memory associated with a previously dumped stats string. \n @param out Pointer to a previously allocated stats string. \n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_stats_free_str(out : *mut *mut ::std::os::raw::c_char) -> i32;
}