pub fn out_dir() -> std::path::PathBuf {
    let out_dir = std::env::var("OUT_DIR").expect("Cargo didn't set OUT_DIR");
    std::path::PathBuf::from(out_dir)
}

pub fn git_dir() -> std::path::PathBuf {
    let mut pbuf = out_dir();
    pbuf.push("git");
    pbuf
}

pub fn build_dir() -> std::path::PathBuf {
    let mut pbuf = out_dir();
    pbuf.push("build");
    pbuf
}
