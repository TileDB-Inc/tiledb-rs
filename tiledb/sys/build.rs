use std::env;

fn main() {
    if std::env::var("TILEDB_SYS_DISABLE_LINKING").is_ok() {
        println!("cargo::metadata=LINKAGE=disabled");
        return;
    }

    pkg_config::Config::new()
        .atleast_version("2.30.0")
        .cargo_metadata(true)
        .probe_cflags(env::consts::OS != "windows")
        .probe("tiledb")
        .expect("TileDB >= 2.30 not found.");
}
