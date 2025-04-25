fn main() {
    let linkage =
        std::env::var("DEP_TILEDB_LINKAGE").expect("Missing DEP_TILEDB_LIKAGE");
    if linkage == "dynamic" {
        let libdir = std::env::var("DEP_TILEDB_LIBDIR")
            .expect("Missing DEP_TILEDB_LIBDIR");
        println!("cargo::rustc-env=TILEDB_RPATH={libdir}");
    } else if linkage == "static" {
        println!("cargo::rustc-env=TILEDB_RPATH=");
    } else {
        panic!("Unknown linkage of tiledb-sys: {linkage}")
    }
}
