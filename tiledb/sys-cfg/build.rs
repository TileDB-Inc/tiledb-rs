use std::env;

fn main() {
    if env::consts::FAMILY != "unix" {
        println!("cargo::rustc-env=TILEDB_RPATH=");
        return;
    }
    let linkage =
        std::env::var("DEP_TILEDB_LINKAGE").expect("Missing DEP_TILEDB_LINKAGE");
    if linkage == "dynamic" {
        let libdir = std::env::var("DEP_TILEDB_LIBDIR")
            .expect("Missing DEP_TILEDB_LIBDIR");
        println!("cargo::rustc-env=TILEDB_RPATH={libdir}");
    } else if linkage == "static" || linkage == "disabled" {
        println!("cargo::rustc-env=TILEDB_RPATH=");
    } else {
        panic!("Unknown linkage of tiledb-sys: {linkage}")
    }
}
