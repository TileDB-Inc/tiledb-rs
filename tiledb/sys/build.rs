#[cfg(target_os = "linux")]
fn configure_rustc(_libdir: &str) {
    println!("cargo::rustc-link-lib=dylib=stdc++");
}

#[cfg(target_os = "macos")]
fn configure_rustc(_libdir: &str) {
    println!("cargo::rustc-link-lib=dylib=c++");
    println!("cargo::rustc-link-lib=framework=CoreFoundation");
    println!("cargo::rustc-link-lib=framework=CoreServices");
    println!("cargo::rustc-link-lib=framework=Security");
    println!("cargo::rustc-link-lib=framework=SystemConfiguration");
    println!("cargo::rustc-link-lib=framework=Network");
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn configure_rustc(_libdir: &str) {}

fn configure_static(libdir: &str) {
    // Configure linking
    println!("cargo::metadata=LINKAGE=static");
    println!("cargo::rustc-link-search=native={libdir}");
    println!("cargo::rustc-link-lib=static=tiledb_static");

    // Add any extra OS specific config
    configure_rustc(libdir);
}

fn configure_dynamic(libdir: &str) {
    println!("cargo::metadata=LINKAGE=dynamic");
    println!("cargo::rustc-link-search=native={libdir}");
    println!("cargo::rustc-link-lib=tiledb");
    println!("cargo::metadata=LIBDIR={libdir}");
}

#[cfg(windows)]
fn maybe_set_probe_cflags(config: &mut pkg_config::Config) {
    // TODO: Do on all platforms once this PR hits a release:
    // https://github.com/rust-lang/pkg-config-rs/pull/183
    // We currently do it only on Windows, to avoid downstream components patching pkg-config themselves.
    config.probe_cflags(false);
}

#[cfg(not(windows))]
fn maybe_set_probe_cflags(_: &mut pkg_config::Config) {}

fn main() {
    if std::env::var("TILEDB_SYS_DISABLE_LINKING").is_ok() {
        println!("cargo::metadata=LINKAGE=disabled");
        return;
    }

    let mut config = pkg_config::Config::new();
    config.atleast_version("2.30.0").cargo_metadata(false);
    // Not needed for our use case, and skips resolving private transitive requirements.
    maybe_set_probe_cflags(&mut config);
    let lib = config.probe("tiledb").expect("TileDB >= 2.30 not found.");

    if let Some(libdir) = lib.link_paths.first() {
        let is_static = lib.libs.iter().any(|x| x.eq("tiledb_static"));
        let libdir: String = libdir.to_string_lossy().into();
        if is_static {
            configure_static(&libdir);
        } else {
            configure_dynamic(&libdir);
        }
    }
}
