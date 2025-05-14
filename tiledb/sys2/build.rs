use walkdir::WalkDir;

const BRIDGES: &[&str] = &[
    "src/attribute.rs",
    "src/buffer.rs",
    "src/config.rs",
    "src/context.rs",
    "src/datatype.rs",
    "src/dimension.rs",
    "src/domain.rs",
    "src/enumeration.rs",
    "src/filter.rs",
    "src/filter_list.rs",
    "src/filter_type.rs",
    "src/utils.rs",
    "src/webp_format.rs",
];

const CPP_FILES: &[&str] = &[
    "cpp/attribute.cc",
    "cpp/config.cc",
    "cpp/context.cc",
    "cpp/datatype.cc",
    "cpp/dimension.cc",
    "cpp/domain.cc",
    "cpp/enumeration.cc",
    "cpp/filter.cc",
    "cpp/filter_list.cc",
    "cpp/filter_type.cc",
    "cpp/string.cc",
    "cpp/webp_format.cc",
];

fn main() {
    let incdir = tiledb_sys_cfg::include_dir();

    cxx_build::bridges(BRIDGES)
        .warnings_into_errors(true)
        .std("c++20")
        .flag("-mmacosx-version-min=11.0")
        .include(incdir)
        .files(CPP_FILES)
        .compile("tiledb-api2");

    tiledb_sys_cfg::configure();
    tiledb_sys_cfg::rpath();

    ensure_rebuild();
}

fn ensure_rebuild() {
    for entry in WalkDir::new(".").into_iter().filter_map(|e| e.ok()) {
        println!("cargo:rerun-if-changed={}", entry.path().display());
    }
}
