fn main() {
    pkg_config::Config::new()
        .atleast_version("2.20.0")
        .probe("tiledb")
        .expect("Build-time TileDB library missing, version >= 2.4 not found.");
    println!("cargo:rustc-link-lib=tiledb");

    let libdir = pkg_config::get_variable("tiledb", "libdir")
        .expect("Missing tiledb dependency.");
    tiledb_utils::build::set_linker_rpath(&libdir);
}
