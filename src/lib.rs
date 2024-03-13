extern crate tiledb_sys as ffi;

macro_rules! cstring {
    ($arg:ident) => {
        match std::ffi::CString::new($arg) {
            Ok(c_arg) => c_arg,
            Err(nullity) => {
                return Err(crate::error::Error::from(format!("{}", nullity)))
            }
        }
    };
}

macro_rules! out_ptr {
    () => {
        0
    };
    ($T:ty) => {
        0 as $T
    };
}

pub mod array;
pub mod config;
pub mod context;
pub mod error;
pub mod filter;
pub mod filter_list;
pub mod string;

pub use ffi::Datatype;
pub type Result<T> = std::result::Result<T, error::Error>;
