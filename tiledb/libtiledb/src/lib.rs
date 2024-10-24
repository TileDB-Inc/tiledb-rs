//! Build dependency for crates using tiledb.
//!
//! Provides functions which can be used in a crate's build script
//! to add `tiledb` as a dynamically linked and loaded library.

/// Configure rustc for crates that depend on tiledb-sys
///
/// Any crate that depends on tiledb-sys should call this function in its
/// build.rs so that it rustc is correctly configured. This will work correctly
/// regardless of whether tiledb-sys was built with a static or dynamic
/// libtiledb.
pub fn configure() {
    let libdir = if let Ok(libdir) = std::env::var("DEP_TILEDB_LIBDIR") {
        libdir
    } else {
        return;
    };

    println!(
        "cargo:rustc-link-arg=-Wl,-rpath,@loader_path,-rpath,$ORIGIN,-rpath,{}",
        libdir
    );
}
