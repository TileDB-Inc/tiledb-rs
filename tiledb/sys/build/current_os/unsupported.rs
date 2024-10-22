#![cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "windows"
)))]

use crate::error::Result;

pub fn configure_rustc(_out: &str) -> Result<()> {
    panic!("This operating system is not supported.");
}

pub fn merge_libraries(build_dir: &std::path::Path) -> Result<()> {
    panic!("This operating system is not supported.");
}
