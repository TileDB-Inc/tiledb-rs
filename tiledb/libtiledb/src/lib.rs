//! Build dependency for crates using tiledb.
//!
//! Provides functions which can be used in a crate's build script
//! to add `tiledb` as a dynamically linked and loaded library.

/// Emits the cargo build command `cargo:rustc-link-lib=tiledb`.
///
/// This should be called only from the lowest-level crate which depdends
/// on symbols from `libtiledb`. Crates which depend on the `tiledb-api`
/// do not need to call this.
pub fn link() {
    pkg_config::Config::new()
        .atleast_version("2.20.0")
        .probe("tiledb")
        .expect("Build-time TileDB library missing, version >= 2.4 not found.");
    println!("cargo:rustc-link-lib=tiledb");
}

/// Emits cargo build commands to add `libtiledb.so` to a compiled executable's rpath.
///
/// This should be called from the build script of any library or executable which
/// depends on `tiledb-api`, whether directly or indirectly.
pub fn rpath() {
    let libdir = pkg_config::get_variable("tiledb", "libdir")
        .expect("Missing tiledb dependency.");
    println!(
        "cargo:rustc-link-arg=-Wl,-rpath,@loader_path,-rpath,$ORIGIN,-rpath,{}",
        libdir
    );
}
