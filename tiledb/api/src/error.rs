extern crate tiledb_sys as ffi;

use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::ops::Deref;

use serde::{Serialize, Serializer};

use crate::Datatype;

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
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Internal error due to bugs in tiledb.
    /// This should be not occur in normal usage of tiledb.
    #[error("Internal error: {0}")]
    Internal(String),
    /// Error received from the libtiledb backend
    #[error("libtiledb error: {0}")]
    LibTileDB(String),
    /// Error when a function has an invalid argument
    #[error("Invalid argument: {0}")]
    InvalidArgument(#[source] anyhow::Error),
    /// Error with datatype handling
    #[error("Datatype error: {0}")]
    Datatype(DatatypeErrorKind),
    /// Error serializing data
    #[error("Serialization error: {0}: {1}")]
    Serialization(String, #[source] anyhow::Error),
    /// Error deserializing data
    #[error("Deserialization error: {0}: {1}")]
    Deserialization(String, #[source] anyhow::Error),
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
