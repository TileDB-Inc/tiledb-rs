/// Configure rpath for crates that depend on tiledb-sys
///
/// Any crate that depends on tiledb-sys should call this function in its
/// build.rs so that it rustc is correctly configured. Note that for anyone
/// building static binaries, this is a no-op when tiledb-sys was built
/// statically.
pub fn rpath() {
    if cfg!(windows) {
        return;
    }

    let libdir = env!("TILEDB_RPATH");
    if libdir.is_empty() {
        return;
    }

    let parts = [
        "cargo::rustc-link-arg=",
        "-Wl,",
        "-rpath,@loader_path,",
        "-rpath,$ORIGIN,",
        "-rpath,",
        libdir,
    ];

    println!("{}", parts.join(""));
}
