use subprocess as sp;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error accessing envrionment: {0}")]
    Env(#[from] std::env::VarError),
    #[error("Error executing Git: {0}")]
    Git(String),
    #[error("IO Error: {0} failed due to: {1}")]
    IO(String, std::io::Error),
    #[error("Merge Error: {0}")]
    Merge(Box<dyn std::error::Error>),
    #[error("Popen Error: {0} failed due to: {1}")]
    Popen(String, sp::PopenError),
}

pub type Result<T> = std::result::Result<T, Error>;
