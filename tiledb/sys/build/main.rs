mod compile;
mod current_os;
mod error;
mod repo;
mod utils;

fn configure_static() -> error::Result<()> {
    repo::update()?;
    let libdir = compile::libtiledb()?;

    // Configure linking
    println!("cargo::metadata=LINKAGE=static");
    println!("cargo::rustc-link-search=native={}", libdir);
    println!("cargo::rustc-link-lib=static=tiledb_bundled");

    // Add any extra OS specific config
    current_os::configure_rustc(&libdir).expect("Error configuring rustc");

    Ok(())
}

fn configure_dynamic() -> error::Result<()> {
    pkg_config::Config::new()
        .atleast_version("2.20.0")
        .probe("tiledb")
        .expect("Build-time TileDB library missing, version >= 2.4 not found.");

    let libdir = pkg_config::get_variable("tiledb", "libdir")
        .expect("Missing tiledb dependency.");

    println!("cargo::metadata=LINKAGE=dynamic");
    println!("cargo::rustc-link-lib=tiledb");
    println!("cargo::metadata=LIBDIR={libdir}");

    Ok(())
}

fn main() {
    // Ensure that we rebuild things if either of our environment vairables
    // have changed.
    println!("cargo::rerun-if-env-changed=TILEDB_SYS_STATIC");

    if std::env::var("TILEDB_SYS_STATIC").is_ok() {
        configure_static().unwrap();
    } else {
        configure_dynamic().unwrap();
    }
}
