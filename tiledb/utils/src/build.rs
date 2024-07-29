pub fn set_linker_rpath(libdir: &str) {
    println!(
        "cargo:rustc-link-arg=-Wl,-rpath,@loader_path,-rpath,$ORIGIN,-rpath,{}",
        libdir
    );
}
