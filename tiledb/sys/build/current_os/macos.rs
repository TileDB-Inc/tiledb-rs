#![cfg(target_os = "macos")]

use crate::error::Result;

pub fn configure_rustc(_out: &str) -> Result<()> {
    println!("cargo::rustc-link-lib=dylib=c++");
    println!("cargo::rustc-link-lib=framework=CoreFoundation");
    println!("cargo::rustc-link-lib=framework=Security");
    println!("cargo::rustc-link-lib=framework=SystemConfiguration");

    Ok(())
}

pub fn merge_libraries(build_dir: &std::path::Path) -> Result<()> {
    let mut tdb = std::path::PathBuf::from(build_dir);
    tdb.extend(["tiledb", "libtiledb.a"]);
    if !tdb.is_file() {
        panic!("Missing libtiled: {}", tdb.display());
    }

    let mut vcpkg_installed = std::path::PathBuf::from(build_dir);
    vcpkg_installed.push("vcpkg_installed");
    if !vcpkg_installed.is_dir() {
        panic!("Missing vcpkg_installed directory.");
    }

    let paths = std::fs::read_dir(vcpkg_installed)
        .expect("Error reading vcpkg_installed");

    // Filter out the `vcpkg/` subdirectory and hopefully only one directory
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
        .expect("Error creating library merger.");
    merge
        .merge_simple()
        .expect("Error merging static libraries.");

    Ok(())
}
