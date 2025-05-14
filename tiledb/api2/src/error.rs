use tiledb_sys2::buffer::Buffer;
use tiledb_sys2::datatype::{DatatypeError, TryFromFFIError};

#[derive(Debug, thiserror::Error)]
pub enum TileDBError {
    #[error("Internal TileDB Error: {0}")]
    Internal(String),

    #[error("Invalid variant found in enum '{0}'")]
    InvalidEnumVariant(String),

    #[error("Invalid datatype while attempting to convert a Buffer to Vec")]
    FailedBufferConversion(Buffer),

    #[error(transparent)]
    Datatype(#[from] DatatypeError),

    #[error(transparent)]
    DatatypeConversion(#[from] TryFromFFIError),

    #[error("TileDB returned invalid UTF-8 data")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),
}

impl From<cxx::Exception> for TileDBError {
    fn from(exc: cxx::Exception) -> Self {
        TileDBError::Internal(exc.what().to_string())
    }
}

impl From<Buffer> for TileDBError {
    fn from(buf: Buffer) -> Self {
        TileDBError::FailedBufferConversion(buf)
    }
}
