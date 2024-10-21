extern crate anyhow;
extern crate thiserror;
extern crate tiledb_sys as ffi;

#[cfg(test)]
extern crate tiledb_utils as utils;

macro_rules! cstring {
    ($arg:expr) => {
        match std::ffi::CString::new($arg) {
            Ok(c_arg) => c_arg,
            Err(nullity) => {
                return Err(crate::error::Error::InvalidArgument(
                    anyhow::anyhow!(nullity),
                ))
            }
        }
    };
}

macro_rules! eq_helper {
    ($mine:expr, $theirs:expr) => {{
        if !match ($mine, $theirs) {
            (Ok(mine), Ok(theirs)) => mine == theirs,
            _ => false,
        } {
            return false;
        }
    }};
}

macro_rules! out_ptr {
    () => {
        unsafe { std::mem::MaybeUninit::zeroed().assume_init() }
    };
    ($T:ty) => {
        $T::default()
    };
}

pub use tiledb_common::key;
pub use tiledb_common::physical_type_go;
pub use tiledb_common::range;

pub mod array;
pub mod config;
pub mod context;
pub mod datatype;
pub mod error;
pub mod filesystem;
pub mod filter;
pub mod group;
pub mod metadata;
pub mod query;
pub mod stats;
pub mod string;
pub mod vfs;

#[cfg(feature = "arrow")]
pub mod arrow;

#[cfg(test)]
pub mod tests;

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
pub use context::{Context, ContextBound};
pub use datatype::Datatype;
pub type Result<T> = std::result::Result<T, error::Error>;

#[cfg(any(test, feature = "pod"))]
pub trait Factory {
    type Item;

    fn create(&self, context: &context::Context) -> Result<Self::Item>;
}
