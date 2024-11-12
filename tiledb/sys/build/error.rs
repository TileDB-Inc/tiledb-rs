use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error accessing envrionment: {0}")]
    Env(#[from] std::env::VarError),
    #[error("IO Error: {0} failed due to: {1}")]
    IO(String, std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
