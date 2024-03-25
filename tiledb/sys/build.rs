fn main() {
    // Hard coded for now
    println!("cargo:rustc-link-lib=tiledb");

    // Use the system's tiledb library
    // Cargo metadata will be printed to stdout if the search was successful
    pkg_config::Config::new()
        .atleast_version("2.4.0")
        .probe("tiledb")
        .expect("Build-time TileDB library missing, version >= 2.4 not found.");
}
