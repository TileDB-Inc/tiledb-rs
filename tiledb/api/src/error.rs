extern crate tiledb_sys as ffi;

use std::fmt::Debug;

#[cfg(feature = "serde")]
use serde::{Serialize, Serializer};

use tiledb_common::array::CellValNum;

pub use tiledb_common::datatype::Error as DatatypeError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Internal error due to bugs in tiledb.
    /// This should be not occur in normal usage of tiledb.
    #[error("Internal error: {0}")]
    Internal(String),
    /// Error locking the context mutex.
    #[error("Error locking context: {0}")]
    LockError(#[source] anyhow::Error),
    #[error("Error creating libtiledb context: {0}")]
    CreateContext(#[from] crate::context::CreateContextError),
    /// Error received from the libtiledb backend
    #[error("libtiledb error: {0}")]
    LibTileDB(#[from] crate::context::CApiError),
    /// Error retrieving a string from libtiledb
    #[error("libtiledb string error: {0}")]
    LibTileDBString(#[from] crate::string::Error),
    #[error("libtiledb stats error: {0}")]
    StatsError(#[from] crate::stats::Error),
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
    ObjectType(#[from] crate::context::ObjectTypeError),
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
    #[error("Unexpected null values")]
    UnexpectedValidity,
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
