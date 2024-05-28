fn main() {
    let libdir = pkg_config::get_variable("tiledb", "libdir")
        .expect("Build-time TileDB library missing.");
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", libdir);
}
