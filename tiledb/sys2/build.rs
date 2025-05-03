fn main() {
    // ToDo: Auto discover these lists via walkdir or similar
    let bridges = vec!["src/config.rs", "src/context.rs"];
    let files = vec!["cpp/config.cc", "cpp/context.cc"];

    let incdir = tiledb_sys_cfg::include_dir();

    cxx_build::bridges(bridges)
        .warnings_into_errors(true)
        .std("c++20")
        .flag("-mmacosx-version-min=11.0")
        .include(incdir)
        .files(files)
        .compile("tiledb-api2");

    tiledb_sys_cfg::configure();
    tiledb_sys_cfg::rpath();
}
