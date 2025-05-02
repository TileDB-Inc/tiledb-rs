#[derive(Debug, thiserror::Error)]
pub enum TileDBError {
    #[error("Internal TileDB Error: {0}")]
    Internal(String),
}

impl From<cxx::Exception> for TileDBError {
    fn from(exc: cxx::Exception) -> Self {
        TileDBError::Internal(exc.what().to_string())
    }
}
