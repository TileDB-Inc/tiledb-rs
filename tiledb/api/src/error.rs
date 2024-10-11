extern crate tiledb_sys as ffi;

use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::ops::Deref;

#[cfg(feature = "serde")]
use serde::{Serialize, Serializer};

use tiledb_common::array::CellValNum;

pub use tiledb_common::datatype::Error as DatatypeError;

pub(crate) enum RawError {
    Owned(*mut ffi::tiledb_error_t),
}

impl Deref for RawError {
    type Target = *mut ffi::tiledb_error_t;
    fn deref(&self) -> &Self::Target {
        let RawError::Owned(ref ffi) = *self;
        ffi
    }
}

impl Drop for RawError {
    fn drop(&mut self) {
        let RawError::Owned(ref mut ffi) = *self;
        unsafe { ffi::tiledb_error_free(ffi) }
    }
}

#[derive(Clone, Debug)]
pub enum ObjectTypeErrorKind {
    InvalidDiscriminant(u64),
}

impl Display for ObjectTypeErrorKind {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            ObjectTypeErrorKind::InvalidDiscriminant(value) => {
                write!(f, "Invalid object type: {}", value)
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Internal error due to bugs in tiledb.
    /// This should be not occur in normal usage of tiledb.
    #[error("Internal error: {0}")]
    Internal(String),
    /// Error locking the context mutex.
    #[error("Error locking context: {0}")]
    LockError(#[source] anyhow::Error),
    /// Error received from the libtiledb backend
    #[error("libtiledb error: {0}")]
    LibTileDB(String),
    /// Error when a function has an invalid argument
    #[error("Invalid argument: {0}")]
    InvalidArgument(#[source] anyhow::Error),
    /// Error when an invalid index is provided as an arguemnt.
    #[error("Invalid index: {0}")]
    InvalidIndex(usize),
    /// Error with datatype handling
    #[error("Datatype error: {0}")]
    Datatype(#[from] DatatypeError),
    /// Error with ObjectType handling
    #[error("Object type error: {0}")]
    ObjectType(ObjectTypeErrorKind),
    #[error("Datatype interface error: {0}")]
    DatatypeFFIError(#[from] tiledb_common::datatype::TryFromFFIError),
    /// Error with Mode handling
    #[error("Mode type error: {0}")]
    ModeType(#[from] tiledb_common::array::ModeError),
    #[error("ArrayType error: {0}")]
    ArrayTypeError(#[from] tiledb_common::array::ArrayTypeError),
    #[error("CellValNum error: {0}")]
    CellValNumError(#[from] tiledb_common::array::CellValNumError),
    #[error("CellOrder error: {0}")]
    CellOrder(#[from] tiledb_common::array::CellOrderError),
    #[error("TileOrder error: {0}")]
    TileOrder(#[from] tiledb_common::array::TileOrderError),
    #[error("FilterType error: {0}")]
    FilterType(#[from] crate::filter::FilterTypeError),
    #[error("FilterOption error: {0}")]
    FilterOption(#[from] crate::filter::FilterOptionError),
    #[error("WebPFilter error: {0}")]
    WebPFilterType(#[from] crate::filter::WebPFilterError),
    #[error("ScaleFloatByteWidth error: {0}")]
    ScaleFloatFilter(#[from] crate::filter::ScaleFloatByteWidthError),
    #[error("Dimension error: {0}")]
    DimensionError(#[from] tiledb_common::array::dimension::Error),
    #[error("Dimension range error: {0}")]
    DimensionRangeError(
        #[from] tiledb_common::range::DimensionCompatibilityError,
    ),
    #[error("FromFillValue error: {0}")]
    FromFillValueError(
        #[from] tiledb_common::array::attribute::FromFillValueError,
    ),
    #[error("Range raw data error: {0}")]
    RangeRawDataError(#[from] tiledb_common::range::RangeFromSlicesError),
    #[error("Multi-value range error: {0}")]
    MultiValueRangeError(#[from] tiledb_common::range::MultiValueRangeError),
    #[error("Unexpected {}: expected {expected}, found {found}", std::any::type_name::<CellValNum>())]
    UnexpectedCellStructure {
        expected: CellValNum,
        found: CellValNum,
    },
    /// Error serializing data
    #[error("Serialization error: {0}: {1}")]
    Serialization(String, #[source] anyhow::Error),
    /// Error deserializing data
    #[error("Deserialization error: {0}: {1}")]
    Deserialization(String, #[source] anyhow::Error),
    /// Error occurred executing a query callback
    #[error("Query callback error for attribute [{}]: {1}",
        .0.iter().map(|s| s.as_ref()).collect::<Vec<&str>>().join(","))]
    QueryCallback(Vec<String>, #[source] anyhow::Error),
    /// Any error which cannot be categorized as any of the above
    #[error("{0}")]
    Other(String),
}

impl From<RawError> for Error {
    fn from(e: RawError) -> Self {
        let mut c_msg: *const std::os::raw::c_char = out_ptr!();
        let c_ret = unsafe {
            ffi::tiledb_error_message(
                *e,
                &mut c_msg as *mut *const std::os::raw::c_char,
            )
        };
        let message = if c_ret == ffi::TILEDB_OK && !c_msg.is_null() {
            let c_message = unsafe { std::ffi::CStr::from_ptr(c_msg) };
            String::from(c_message.to_string_lossy())
        } else {
            String::from("Failed to retrieve an error message from TileDB.")
        };
        Error::LibTileDB(message)
    }
}

#[cfg(feature = "serde")]
impl Serialize for Error {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        format!("<{}>", self).serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// Ensure that Error is Sync, fails to compile if not
    fn is_sync() {
        fn is_sync<T: Sync>() {}
        is_sync::<Error>()
    }

    #[test]
    /// Ensure that Error is Send, fails to compile if not
    fn is_send() {
        fn is_sync<T: Send>() {}
        is_sync::<Error>()
    }
}
