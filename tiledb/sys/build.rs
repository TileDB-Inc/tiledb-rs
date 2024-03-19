use std::env;
use std::str::FromStr;

const INSTALL_ENVVAR: &str = "CMAKE_INSTALL_PREFIX";
const INSTALL_DEFAULT: &str = "/opt/tiledb/lib";

fn main() {
    // Hard coded for now
    println!("cargo:rustc-link-lib=tiledb");
    println!(
        "cargo:rustc-link-search=all={}",
        match env::var(INSTALL_ENVVAR) {
            Ok(dir) => dir,
            Err(e) =>
                if let env::VarError::NotPresent = e {
                    String::from_str(INSTALL_DEFAULT).expect("&'static str")
                } else {
                    panic!(
                        "Error reading environment variable '{}': {}",
                        INSTALL_ENVVAR, e
                    );
                },
        }
    );
}
