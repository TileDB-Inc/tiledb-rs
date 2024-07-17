#[cfg(feature = "static")]
mod tiledb {
    use std::collections::HashSet;
    use std::io::prelude::*;

    pub fn configure() {
        let tiledb_dir = update_repository();
        copy_link_source(&tiledb_dir);
        build_tiledb(&tiledb_dir);
        write_linker_commands();
    }

    fn update_repository() -> std::path::PathBuf {
        let out_dir =
            std::env::var("OUT_DIR").expect("Error getting cargo OUT_DIR");
        let out_dir = std::path::Path::new(&out_dir);

        let repos = out_dir.join("repos");
        if !repos.exists() {
            std::fs::create_dir_all(&repos)
                .expect("Error creating repos directory");
        }

        let tiledb_dir = repos.clone().join("tiledb");
        let cmd = if tiledb_dir.exists() {
            vec!["git", "-C", tiledb_dir.to_str().unwrap(), "pull"]
        } else {
            vec![
                "git",
                "-C",
                repos.to_str().unwrap(),
                "clone",
                "https://github.com/TileDB-Inc/TileDB",
                "tiledb",
            ]
        };

        let mut p =
            subprocess::Popen::create(&cmd, subprocess::PopenConfig::default())
                .expect("Error creating git subprocess");
        let res = p.wait().expect("Error waiting for git operation");

        if !matches!(res, subprocess::ExitStatus::Exited(0)) {
            panic!("Git operation failed");
        }

        tiledb_dir
    }

    fn copy_link_source(tiledb_dir: &std::path::PathBuf) {
        let curr_dir =
            std::env::current_dir().expect("Error getting current directory");
        let curr_dir = std::path::Path::new(&curr_dir);
        let source = curr_dir.join("static_build").join("link_info.cc");
        let dest = tiledb_dir.join("link_info.cc");
        std::fs::copy(source, dest)
            .expect("Error copying link_info.cc to TileDB source tree");
    }

    fn build_tiledb(tiledb_dir: &std::path::PathBuf) {
        let curr_dir =
            std::env::current_dir().expect("Error getting current directory");

        let link_launcher =
            curr_dir.join("static_build").join("linker_launcher.sh");

        let link_info_cmake =
            curr_dir.join("static_build").join("LinkInfo.cmake");

        cmake::Config::new(tiledb_dir.to_str().unwrap())
            .generator("Ninja")
            .env("MACOSX_DEPLOYMENT_TARGET", "14.0")
            .define(
                "CMAKE_CXX_LINKER_LAUNCHER",
                format!("{}", link_launcher.display()),
            )
            .define("BUILD_SHARED_LIBS", "OFF")
            .define("TILEDB_WERROR", "OFF")
            .define("TILEDB_VCPKG", "ON")
            .define("TILEDB_GCS", "ON")
            .define("TILEDB_S3", "ON")
            .define("TILEDB_AZURE", "ON")
            .define("TILEDB_HDFS", "ON")
            .define("TILEDB_TESTS", "OFF")
            .define("TILEDB_SERIALIZATION", "ON")
            .define("TILEDB_VERBOSE", "ON")
            .define("TILEDB_CCACHE", "ON")
            .define(
                "TILEDB_EXTRA_CMAKE_INCLUDE",
                format!("{}", link_info_cmake.display()),
            )
            .build_target("all")
            .build();
    }

    fn write_linker_commands() {
        let out_dir =
            std::env::var("OUT_DIR").expect("Error getting cargo's OUT_DIR");

        let build_dir = std::path::Path::new(&out_dir).join("build");

        let link_info = build_dir.join("link_info.txt");
        if !link_info.exists() {
            panic!("TileDB build failed to generate a link_info.txt");
        }
        let link_info = std::fs::read_to_string(link_info)
            .expect("Error reading link_info.txt");

        #[derive(Default)]
        struct LibCollector {
            libs: Vec<String>,
            seen: HashSet<String>,
            paths: HashSet<String>,
        }

        let collector = link_info
            .split_whitespace()
            .filter(|arg| arg.ends_with(".a"))
            .fold(LibCollector::default(), |mut acc, arg| {
                let lib = build_dir.join(arg);
                let libname = lib
                    .file_stem()
                    .unwrap()
                    .to_string_lossy()
                    .as_ref()
                    .strip_prefix("lib")
                    .unwrap()
                    .to_string();
                let path = lib.parent().unwrap().to_string_lossy().to_string();

                if acc.seen.insert(libname.clone()) {
                    acc.libs.push(libname);
                }
                acc.paths.insert(path);
                acc
            });

        let crate_dir = std::env::var("CARGO_MANIFEST_DIR")
            .expect("Missing CARGO_MANIFEST_DIR");
        let linker_args = std::path::Path::new(&crate_dir)
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("rustc_linker_args.txt");

        let mut file = std::fs::File::create(&linker_args)
            .expect("Error creating linker args file.");

        for path in collector.paths.iter() {
            file.write_all(
                format!("cargo:rustc-link-search=native={}\n", path).as_ref(),
            )
            .expect("Failed writing rustc linker args.");
        }

        for lib in collector.libs.iter() {
            file.write_all(
                format!("cargo:rustc-link-lib=static={}\n", lib).as_ref(),
            )
            .expect("Failed writing rustc linker args.");
        }

        file.write_all(
            format!("cargo:rustc-link-arg=-Wl,-framework,CoreFoundation\n")
                .as_ref(),
        )
        .expect("Failed writing rustc linker args.");
        file.write_all(
            format!(
                "cargo:rustc-link-arg=-Wl,-framework,SystemConfiguration\n"
            )
            .as_ref(),
        )
        .expect("Failed writing rustc linker args.");
        file.write_all(
            format!("cargo:rustc-link-arg=-Wl,-framework,Security\n").as_ref(),
        )
        .expect("Failed writing rustc linker args.");
        file.write_all(
            format!("cargo:rustc-link-arg=-Wl,-framework,CoreServices\n")
                .as_ref(),
        )
        .expect("Failed writing rustc linker args.");
        file.write_all(format!("cargo:rustc-link-arg=-Wl,-lc++\n").as_ref())
            .expect("Failed writing rustc linker args.");
    }
}

#[cfg(not(feature = "static"))]
mod tiledb {
    pub fn configure() {
        // Hard coded for now
        println!("cargo:rustc-link-lib=tiledb");
        let libdir = pkg_config::get_variable("tiledb", "libdir")
            .expect("Missing tiledb dependency.");
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", libdir);
        pkg_config::Config::new()
            .atleast_version("2.20.0")
            .probe("tiledb")
            .expect(
                "Build-time TileDB library missing, version >= 2.4 not found.",
            );
    }
}

fn main() {
    tiledb::configure()
}
