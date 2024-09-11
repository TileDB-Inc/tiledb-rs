fn main() {
    let libdir = pkg_config::get_variable("tiledb", "libdir")
        .expect("Build-time TileDB library missing.");
    tiledb_utils::build::set_linker_rpath(&libdir);
}
