extern crate tiledb_sys as ffi;

use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::ops::Deref;
use std::str::FromStr;

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

#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    /// Error received from the libtiledb backend
    #[error("libtiledb error: {0}")]
    LibTileDB(String),
    /// Error with datatype handling
    #[error("Datatype error: {0}")]
    Datatype(DatatypeErrorKind),
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

impl<S> From<S> for Error
where
    S: AsRef<str>,
{
    fn from(s: S) -> Error {
        Error::Other(String::from(s.as_ref()))
    }
}

impl FromStr for Error {
    type Err = (); /* always succeeds */
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Error::Other(String::from(s)))
    }
}

impl Serialize for Error {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        format!("<{}>", self.to_string()).serialize(serializer)
    }
}
