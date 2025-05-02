pub fn include_dir() -> String {
    let prefix = get_prefix();
    get_incdir(&prefix)
}

pub fn configure() {
    let prefix = get_prefix();
    let libdir = get_libdir(&prefix);

    println!("cargo::rustc-link-search=native={libdir}");
    println!("cargo::rustc-link-lib=tiledb");
}

/// Configure rpath for crates that depend on tiledb-sys
///
/// Any crate that depends on tiledb-sys should call this function in its
/// build.rs so that it rustc is correctly configured. Note that for anyone
/// building static binaries, this is a no-op when tiledb-sys was built
/// statically.
pub fn rpath() {
    let prefix = get_prefix();

    let libdir = get_libdir(&prefix);
    if libdir.is_empty() {
        return;
    }

    let parts = [
        "cargo::rustc-link-arg=",
        "-Wl,",
        "-rpath,@loader_path,",
        "-rpath,$ORIGIN,",
        "-rpath,",
        &libdir,
    ];

    println!("{}", parts.join(""));
}

fn get_prefix() -> String {
    pkg_config::Config::new()
        .atleast_version("2.27.0")
        .cargo_metadata(false)
        .probe("tiledb")
        .expect("TileDB >= 2.27 not found.");

    let prefix = pkg_config::get_variable("tiledb", "prefix")
        .expect("Missing TileDB 'libdir' variable.");

    prefix.trim_matches('"').to_string()
}

fn get_incdir(prefix: &str) -> String {
    std::path::Path::new(prefix)
        .join("include")
        .display()
        .to_string()
}

fn get_libdir(prefix: &str) -> String {
    std::path::Path::new(prefix)
        .join("lib")
        .display()
        .to_string()
}
