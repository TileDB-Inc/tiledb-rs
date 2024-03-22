extern crate serde;
extern crate serde_json;
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
        unsafe { std::mem::MaybeUninit::zeroed().assume_init() }
    };
    ($T:ty) => {
        $T::default()
    };
}

pub mod array;
pub mod config;
pub mod context;
pub mod convert;
pub mod datatype;
pub mod error;
pub mod filter;
pub mod filter_list;
pub mod query;
pub mod string;
pub mod vfs;

pub fn version() -> (i32, i32, i32) {
    let mut major: i32 = 0;
    let mut minor: i32 = 0;
    let mut patch: i32 = 0;

    unsafe {
        ffi::tiledb_version(&mut major, &mut minor, &mut patch);
    }

    (major, minor, patch)
}

pub use array::Array;
pub use datatype::Datatype;
pub use query::{Builder as QueryBuilder, Query, QueryType};
pub type Result<T> = std::result::Result<T, error::Error>;
