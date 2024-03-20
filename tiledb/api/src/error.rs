extern crate tiledb_sys as ffi;

use std::convert::Into;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::str::FromStr;

pub(crate) enum ErrorData {
    Native(*mut ffi::tiledb_error_t),
    Custom(String),
}

pub struct Error {
    data: ErrorData,
}

impl Error {
    pub fn get_message(&self) -> String {
        match &self.data {
            ErrorData::Native(cptr) => {
                let mut msg = std::ptr::null::<std::os::raw::c_char>();
                let res = unsafe {
                    ffi::tiledb_error_message(
                        *cptr,
                        &mut msg as *mut *const std::os::raw::c_char,
                    )
                };
                if res == ffi::TILEDB_OK && !msg.is_null() {
                    let c_msg = unsafe { std::ffi::CStr::from_ptr(msg) };
                    let msg = String::from(c_msg.to_string_lossy());
                    msg
                } else {
                    String::from(
                        "Failed to retrieve an error message from TileDB.",
                    )
                }
            }
            ErrorData::Custom(s) => s.clone(),
        }
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}", self.get_message())
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}", self.get_message())
    }
}

impl From<Error> for String {
    fn from(error: Error) -> String {
        error.get_message()
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Error {
        Error {
            data: ErrorData::Custom(s.into()),
        }
    }
}

impl From<String> for Error {
    fn from(s: String) -> Error {
        Error {
            data: ErrorData::Custom(s),
        }
    }
}

impl FromStr for Error {
    type Err = (); /* always succeeds */
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Error {
            data: ErrorData::Custom(s.into()),
        })
    }
}

impl From<*mut ffi::tiledb_error_t> for Error {
    fn from(cptr: *mut ffi::tiledb_error_t) -> Error {
        Error {
            data: ErrorData::Native(cptr),
        }
    }
}

impl Drop for Error {
    fn drop(&mut self) {
        if let ErrorData::Native(ref mut cptr) = self.data {
            unsafe { ffi::tiledb_error_free(cptr) }
        }
    }
}

impl std::error::Error for Error {}
