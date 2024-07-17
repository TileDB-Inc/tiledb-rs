#[cfg(feature = "static")]
fn link_tiledb() {
    println!("LINKING STATIC LIBRARIES");
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .expect("Missing CARGO_MANIFEST_DIR");
    let linker_file = std::path::Path::new(&manifest_dir)
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("rustc_linker_args.txt");
    let linker_args = std::fs::read_to_string(&linker_file)
        .expect("Error reading linker arguments.");
    println!("{}", linker_args);
}

#[cfg(not(feature = "static"))]
fn link_tiledb() {
    println!("LINKING DYNAMIC LIBRARY");
    let libdir = pkg_config::get_variable("tiledb", "libdir")
        .expect("Build-time TileDB library missing.");
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", libdir);
}

fn main() {
    link_tiledb();
}
