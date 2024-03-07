fn main() {
    // Hard coded for now
    println!("cargo:rustc-link-lib=tiledb");
    println!("cargo:rustc-link-search=all=/opt/tiledb/lib");
}
