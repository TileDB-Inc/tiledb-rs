#[cfg(target_os = "linux")]
fn configure_rustc(_libdir: String) {
    println!("cargo::rustc-link-lib=dylib=stdc++");
}

#[cfg(target_os = "macos")]
fn configure_rustc(_libdir: String) {
    println!("cargo::rustc-link-lib=dylib=c++");
    println!("cargo::rustc-link-lib=framework=CoreFoundation");
    println!("cargo::rustc-link-lib=framework=CoreServices");
    println!("cargo::rustc-link-lib=framework=Security");
    println!("cargo::rustc-link-lib=framework=SystemConfiguration");
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn configure_rustc(_libdir: String) {
    panic!("This operating system is not supported.")
}

fn configure_static(libdir: String) {
    // Configure linking
    println!("cargo::metadata=LINKAGE=static");
    println!("cargo::rustc-link-search=native={libdir}");
    println!("cargo::rustc-link-lib=static=tiledb_static");

    // Add any extra OS specific config
    configure_rustc(libdir);
}

fn configure_dynamic(libdir: String) {
    println!("cargo::metadata=LINKAGE=dynamic");
    println!("cargo::rustc-link-search=native={libdir}");
    println!("cargo::rustc-link-lib=tiledb");
    println!("cargo::metadata=LIBDIR={libdir}");
}

fn main() {
    pkg_config::Config::new()
        .atleast_version("2.28.0")
        .cargo_metadata(false)
        .probe("tiledb")
        .expect("TileDB >= 2.28 not found.");

    let prefix = pkg_config::get_variable("tiledb", "prefix")
        .expect("Missing TileDB 'libdir' variable.");
    let prefix = prefix.trim_matches('"');
    let libdir = std::path::Path::new(prefix)
        .join("lib")
        .display()
        .to_string();

    // If we find a libtiledb_static.a, link statically, otherwise assume
    // we want to link dynamically.
    let mut path = std::path::PathBuf::from(&libdir);
    path.push("libtiledb_static.a");

    if path.exists() {
        configure_static(libdir);
    } else {
        configure_dynamic(libdir);
    }
}
