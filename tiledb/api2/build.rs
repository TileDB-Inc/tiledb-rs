fn main() {
    let incdir = tiledb_sys_cfg::include_dir();

    cxx_build::bridge("src/config.rs")
        .warnings_into_errors(true)
        .std("c++20")
        .flag("-mmacosx-version-min=11.0")
        .include(incdir)
        .file("cpp/config.cc")
        .compile("tiledb-api2");

    tiledb_sys_cfg::configure();
    tiledb_sys_cfg::rpath();
}
