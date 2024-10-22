use std::process::{Command, Stdio};

use crate::error::{Error, Result};
use crate::utils;

/// Clone TileDB-Inc/TileDB into `target/repos/tiledb`.
pub fn update() -> Result<()> {
    if utils::git_dir().is_dir() {
        return Ok(());
    }

    let out_dir = utils::out_dir().display().to_string();
    let output = Command::new("git")
        .arg("clone")
        .arg("https://github.com/TileDB-Inc/TileDB")
        .arg("git")
        .current_dir(out_dir)
        .stderr(Stdio::inherit())
        .stdout(Stdio::inherit())
        .output()
        .map_err(|e| Error::IO("Error executing git".to_string(), e))?;

    if !output.status.success() {
        panic!("Error cloning TileDB repository: {}", output.status);
    }

    Ok(())
}
