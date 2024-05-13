extern crate tiledb_sys as ffi;

use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::ops::Deref;

use crate::array::CellValNum;
use crate::Datatype;
use serde::{Serialize, Serializer};

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
pub enum DatatypeErrorKind {
    InvalidDiscriminant(u64),
    TypeMismatch {
        user_type: &'static str,
        tiledb_type: Datatype,
    },
    UnexpectedCellStructure {
        context: Option<String>,
        found: CellValNum,
        expected: CellValNum,
    },
    InvalidDatatype {
        context: Option<String>,
        found: Datatype,
        expected: Datatype,
    },
}

impl Display for DatatypeErrorKind {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            DatatypeErrorKind::InvalidDiscriminant(value) => {
                write!(f, "Invalid datatype: {}", value)
            }
            DatatypeErrorKind::TypeMismatch {
                user_type,
                tiledb_type,
            } => {
                write!(
                    f,
                    "Type mismatch: requested {}, but found {}",
                    user_type, tiledb_type
                )
            }
            DatatypeErrorKind::UnexpectedCellStructure {
                ref context,
                found,
                expected,
            } => {
                if let Some(context) = context.as_ref() {
                    write!(
                        f,
                        "Unexpected cell val num for {}: expected {}, found {}",
                        context, expected, found
                    )
                } else {
                    write!(
                        f,
                        "Unexpected cell val num: expected {}, found {}",
                        expected, found
                    )
                }
            }
            DatatypeErrorKind::InvalidDatatype {
                ref context,
                found,
                expected,
            } => {
                if let Some(context) = context.as_ref() {
                    write!(
                        f,
                        "Unexpected datatype for {}: expected {}, found {}",
                        context, expected, found
                    )
                } else {
                    write!(
                        f,
                        "Unexpected datatype: expected {}, found {}",
                        expected, found
                    )
                }
            }
        }
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

#[derive(Clone, Debug)]
pub enum ModeErrorKind {
    InvalidDiscriminant(u64),
}

impl Display for ModeErrorKind {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            ModeErrorKind::InvalidDiscriminant(value) => {
                write!(f, "Invalid mode type: {}", value)
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
    Datatype(DatatypeErrorKind),
    /// Error with ObjectType handling
    #[error("Object type error: {0}")]
    ObjectType(ObjectTypeErrorKind),
    /// Error with Mode handling
    #[error("Mode type error: {0}")]
    ModeType(ModeErrorKind),
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
