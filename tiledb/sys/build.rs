use std::env;

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

fn main() {
    if std::env::var("TILEDB_SYS_DISABLE_LINKING").is_ok() {
        println!("cargo::metadata=LINKAGE=disabled");
        return;
    }

    let lib = pkg_config::Config::new()
        .atleast_version("2.30.0")
        .cargo_metadata(false)
        .probe_cflags(env::consts::OS != "windows")
        .probe("tiledb")
        .expect("TileDB >= 2.30 not found.");

    if let Some(libdir) = lib.link_paths.get(0) {
        let is_static = lib.libs.iter().any(|x| x.eq("tiledb_static"));
        let libdir: String = libdir.to_string_lossy().into();
        if is_static {
            configure_static(&libdir);
        } else {
            configure_dynamic(&libdir);
        }
    }
}
