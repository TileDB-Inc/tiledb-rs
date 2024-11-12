#![cfg(target_os = "windows")]

use crate::error::Result;

pub fn configure_rustc(_out: &str) -> Result<()> {
    todo!("Add windows support.");
}

pub fn merge_libraries(build_dir: &std::path::Path) -> Result<()> {
    todo!("Add windows support.");
}
