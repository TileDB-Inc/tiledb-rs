fn main() {
    // Hard coded for now
    println!("cargo:rustc-link-lib=tiledb");
    let libdir = pkg_config::get_variable("tiledb", "libdir")
        .expect("Missing tiledb dependency.");
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", libdir);
    pkg_config::Config::new()
        .atleast_version("2.20.0")
        .probe("tiledb")
        .expect("Build-time TileDB library missing, version >= 2.4 not found.");
}
