use std::thread;
use std::time::Duration;

use subprocess as sp;

use crate::error::{Error, Result};
use crate::utils;

/// Clone TileDB-Inc/TileDB into `target/repos/tiledb`.
///
/// Once the first clone is executed, its up to users to manage the state
/// of this repository to avoid automated tooling from constantly trying to
/// update it.
pub fn update() -> Result<()> {
    if utils::git_dir().is_dir() {
        return Ok(());
    }

    let out_dir = utils::out_dir().display().to_string();
    let cmd = [
        "git",
        "-C",
        &out_dir,
        "clone",
        "https://github.com/davisp/TileDB",
        "git",
    ]
    .to_vec();

    // Execute our Git command
    let mut git = sp::Popen::create(
        &cmd,
        sp::PopenConfig {
            stdout: sp::Redirection::Pipe,
            stderr: sp::Redirection::Pipe,
            ..Default::default()
        },
    )
    .map_err(|e| Error::Popen("Executing git".to_string(), e))?;

    // Obtain the output from the standard streams.
    let (out, err) = git
        .communicate(None)
        .map_err(|e| Error::IO("Running git".to_string(), e))?;

    // Wait for git to finish executing
    loop {
        let result = git.poll();
        if result.is_none() {
            thread::sleep(Duration::from_secs(1));
            continue;
        }

        if !matches!(result, Some(sp::ExitStatus::Exited(0))) {
            let msg = format!(
                "Error executing git.\nstdout:\n{}\n\nstderr:\n{}",
                out.unwrap_or("".to_string()),
                err.unwrap_or("".to_string())
            );
            return Err(Error::Git(msg));
        }

        break;
    }

    Ok(())
}
