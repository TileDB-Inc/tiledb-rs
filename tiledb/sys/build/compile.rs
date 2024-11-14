use crate::current_os;
use crate::error::Result;
use crate::utils;

pub fn libtiledb() -> Result<String> {
    let build_dir = utils::build_dir();
    if build_dir.is_dir() {
        let mut bundled = build_dir.clone();
        bundled.push("libtiledb_bundled.a");
        if bundled.is_file() {
            return Ok(build_dir.display().to_string());
        }
    }

    // N.B., you might think this should be `utils::build_dir()`, but the cmake
    // crate appends `build` unconditionally so we have to go one directory up.
    let out_dir = utils::out_dir();
    let git_dir = utils::git_dir();
    let mut builder = cmake::Config::new(&git_dir);
    builder
        .out_dir(out_dir)
        .build_target("all")
        .define("BUILD_SHARED_LIBS", "OFF")
        .define("TILEDB_WERROR", "OFF")
        .define("TILEDB_S3", "ON")
        .define("TILEDB_SERIALIZATION", "ON");

    if std::env::var("TILEDB_SYS_CCACHE").is_ok() {
        builder.define("TILEDB_CCACHE", "ON");
    }

    if let Ok(num_jobs) = std::env::var("TILEDB_SYS_JOBS") {
        builder.build_arg(format!("-j{}", num_jobs));
    }

    let mut dst = builder.build();
    dst.push("build");

    current_os::merge_libraries(&dst)?;
    Ok(dst.display().to_string())
}
