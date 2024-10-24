use crate::error::{Error, Result};
use crate::utils;

fn configure_rustc_base(out: &std::path::Path) {
    // Configure linking
    println!("cargo::rustc-link-search=native={}", out.display());
    println!("cargo::rustc-link-lib=static=tiledb_bundled");

    // Let dependents know about the situation
    println!("cargo::metadata=STATIC=true");
}

#[cfg(target_os = "linux")]
fn configure_rustc(out: &std::path::Path) {
    configure_rustc_base(out);
    println!("cargo::rustc-link-lib=dylib=c++");
}

#[cfg(target_os = "macos")]
fn configure_rustc(out: &std::path::Path) {
    configure_rustc_base(out);
    println!("cargo::rustc-link-lib=dylib=c++");
    println!("cargo::rustc-link-lib=framework=CoreFoundation");
    println!("cargo::rustc-link-lib=framework=Security");
    println!("cargo::rustc-link-lib=framework=SystemConfiguration");
}

#[cfg(target_os = "windows")]
fn configure_rustc(out: &std::path::Path) {
    configure_rustc_base(out);
    todo!("Windows not yet supported.");
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "windows"
)))]
fn configure_rustc(_out: &std::path::PathBuf) {
    panic!("Unsupported target os");
}

pub fn libtiledb() -> Result<String> {
    println!("Compiling libtiledb");
    let build_dir = utils::build_dir();
    if build_dir.is_dir() {
        let mut bundled = build_dir.clone();
        bundled.push("libtiledb_bundled.a");
        if bundled.is_file() {
            configure_rustc(&build_dir);
            return Ok(build_dir.display().to_string());
        }
    }

    // N.B., you might think this should be `build_dir()`, but the cmake crate
    // appends `build` unconditionally so we have to go one directory up.
    println!("Starting cmake builder");
    let out_dir = utils::out_dir();
    let git_dir = utils::git_dir();
    let mut builder = cmake::Config::new(&git_dir);
    builder
        .out_dir(out_dir)
        .build_target("all")
        .define("BUILD_SHARED_LIBS", "OFF")
        .define("TILEDB_WERROR", "OFF")
        .define("TILEDB_CCACHE", "ON")
        .define("TILEDB_S3", "ON")
        .define("TILEDB_SERIALIZATION", "ON");

    println!("Maybe checking in parallel");
    if let Ok(num_jobs) = std::env::var("TILEDB_SYS_JOBS") {
        builder.build_arg(format!("-j{}", num_jobs));
    }

    println!("Building!");
    let mut dst = builder.build();
    dst.push("build");

    println!("Merging libs");
    merge_libs(&dst)?;
    configure_rustc(&dst);
    Ok(dst.display().to_string())
}

#[cfg(any(target_os = "linux", target_os = "macos",))]
fn merge_libs(build_dir: &std::path::Path) -> Result<()> {
    let mut tdb = std::path::PathBuf::from(build_dir);
    tdb.extend(["tiledb", "libtiledb.a"]);
    if !tdb.is_file() {
        tdb.pop();
        tdb.extend(["tiledb", "libtiledb.a"]);
        if !tdb.is_file() {
            panic!("Missing libtiled: {}", tdb.display());
        }
    }

    let mut vcpkg_installed = std::path::PathBuf::from(build_dir);
    vcpkg_installed.push("vcpkg_installed");
    if !vcpkg_installed.is_dir() {
        panic!("Missing vcpkg_installed directory.");
    }

    let paths = std::fs::read_dir(vcpkg_installed)
        .expect("Error reading vcpkg_installed");

    // Filter out the `vpckg/` subdirectory and hopefully only one directory
    // remains for us to care about.
    let mut not_vcpkg_paths = Vec::new();
    for path in paths.flatten() {
        if !path.path().is_dir() {
            continue;
        }

        let path = path.path();
        if path.file_name() == Some(std::ffi::OsStr::new("vcpkg")) {
            continue;
        }

        not_vcpkg_paths.push(path.display().to_string());
    }

    if not_vcpkg_paths.len() > 1 {
        let paths = not_vcpkg_paths.join(", ");
        panic!(
            "Too many target triplet directories to choose from: {}",
            paths
        );
    }

    let path = if let Some(path) = not_vcpkg_paths.first() {
        path.to_string()
    } else {
        panic!("Error locating `vcpkg_installed/${{triplet}}` directory.");
    };

    let mut lib_dir = std::path::PathBuf::from(build_dir);
    assert!(lib_dir.is_dir());
    lib_dir.extend(["vcpkg_installed", &path, "lib"]);
    if !lib_dir.is_dir() {
        panic!(
            "Missing directory vcpkg_installed/${{triplet}}/lib: {}",
            lib_dir.display()
        );
    }

    let paths =
        std::fs::read_dir(lib_dir).expect("Error reading vcpkg lib directory.");
    let mut libs = vec![tdb.display().to_string()];
    for path in paths.flatten() {
        let path = path.path().display().to_string();
        if !path.ends_with(".a") {
            continue;
        }
        libs.push(path);
    }

    let mut output = std::path::PathBuf::from(build_dir);
    output.extend(["libtiledb_bundled.a"]);

    let merge = armerge::ArMerger::new_from_paths(&libs, &output)
        .map_err(|e| Error::Merge(Box::new(e)))?;
    merge
        .merge_simple()
        .map_err(|e| Error::Merge(Box::new(e)))?;

    Ok(())
}

#[cfg(target_os = "windows")]
fn merge_libs(out_dir: &std::path::Path) -> Result<()> {
    panic!("Need to do the same as armerge, but using lib.exe")
}
