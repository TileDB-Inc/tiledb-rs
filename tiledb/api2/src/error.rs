use tiledb_sys2::datatype::DatatypeError;
use tiledb_sys2::error::TryFromFFIError;

#[derive(Debug, thiserror::Error)]
pub enum TileDBError {
    #[error("Internal TileDB Error: {0}")]
    Internal(String),

    #[error(transparent)]
    Datatype(#[from] DatatypeError),

    #[error(transparent)]
    DatatypeConversion(#[from] TryFromFFIError),

    #[error("TileDB returned invalid UTF-8 data")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),

    #[error("Capacity must be non-zero.")]
    InvalidCapacity,

    #[error("The field '{0}' was not found.")]
    UnknownField(String),

    #[error("The field '{0}' is not variably sized")]
    NonVariable(String),

    #[error("The field '{0}' is not nullable.")]
    NonNullable(String),
}

impl From<cxx::Exception> for TileDBError {
    fn from(exc: cxx::Exception) -> Self {
        TileDBError::Internal(exc.what().to_string())
    }
}
