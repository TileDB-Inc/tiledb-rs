mod compile;
mod error;
mod repo;
mod utils;

fn configure_static() -> error::Result<()> {
    repo::update()?;
    compile::libtiledb()?;
    Ok(())
}

fn configure_dynamic() -> error::Result<()> {
    pkg_config::Config::new()
        .atleast_version("2.20.0")
        .probe("tiledb")
        .expect("Build-time TileDB library missing, version >= 2.4 not found.");
    println!("cargo:rustc-link-lib=tiledb");

    let libdir = pkg_config::get_variable("tiledb", "libdir")
        .expect("Missing tiledb dependency.");

    println!("cargo::metadata=DYNAMIC=true");
    println!("cargo::metadata=LIBDIR={}", libdir);

    Ok(())
}

fn main() {
    // Ensure that we rebuild things if either of our environment vairables
    // have changed.
    println!("cargo::rerun-if-env-changed=TILEDB_SYS_STATIC");
    println!("cargo::rerun-if-env-changed=TILEDB_SYS_OUT_DIR");

    if std::env::var("TILEDB_SYS_STATIC").is_ok() {
        configure_static().unwrap();
    } else {
        configure_dynamic().unwrap();
    }
}
