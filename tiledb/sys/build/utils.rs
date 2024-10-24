pub fn out_dir() -> std::path::PathBuf {
    if let Ok(out_dir) = std::env::var("TILEDB_SYS_OUT_DIR") {
        let pbuf = std::path::PathBuf::from(out_dir);
        if !pbuf.is_dir() {
            panic!("TILEDB_SYS_OUT_DIR is set, but does not exist.");
        }
        pbuf
    } else {
        let out_dir =
            std::env::var("OUT_DIR").expect("Cargo didn't set OUT_DIR");
        std::path::PathBuf::from(out_dir)
    }
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
