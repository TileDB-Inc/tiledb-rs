use std::thread;
use std::time::Duration;

use subprocess as sp;

use crate::error::{Error, Result};

pub fn run(cmd: &[&str], input: Option<&str>) -> Result<()> {
    // Execute our Git command
    let stdin = if input.is_some() {
        sp::Redirection::Pipe
    } else {
        sp::Redirection::None
    };

    let mut git = sp::Popen::create(
        cmd,
        sp::PopenConfig {
            stdin,
            stdout: sp::Redirection::Pipe,
            stderr: sp::Redirection::Pipe,
            ..Default::default()
        },
    )
    .map_err(|e| {
        Error::Popen(format!("Spawning command: {}", cmd.join(" ")), e)
    })?;

    // Obtain the output from the standard streams.
    let (out, err) = git.communicate(input).map_err(|e| {
        Error::IO(format!("Executing command: {}", cmd.join(" ")), e)
    })?;

    // Wait for git to finish executing
    loop {
        let result = git.poll();
        if result.is_none() {
            thread::sleep(Duration::from_secs(1));
            continue;
        }

        if !matches!(result, Some(sp::ExitStatus::Exited(0))) {
            let msg = format!(
                "Error executing command.\ncommand: {}\ninput: {:?}\nstdout:\n{}\n\nstderr:\n{}",
                cmd.join(" "),
                input,
                out.unwrap_or("".to_string()),
                err.unwrap_or("".to_string())
            );
            return Err(Error::Git(msg));
        }

        break;
    }

    Ok(())
}
