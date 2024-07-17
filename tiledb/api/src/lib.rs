extern crate anyhow;
extern crate serde;
extern crate serde_json;
extern crate thiserror;
#[macro_use]
extern crate tiledb_proc_macro;
extern crate tiledb_sys as ffi;
extern crate tiledb_utils as util;

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

pub mod array;
pub mod config;
pub mod context;
pub mod datatype;
pub mod error;
pub mod filesystem;
pub mod filter;
pub mod group;
pub mod key;
pub mod metadata;
pub mod query;
#[macro_use]
pub mod range;
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

pub trait Factory {
    type Item;

    fn create(&self, context: &context::Context) -> Result<Self::Item>;
}

mod private {
    // The "sealed trait" pattern is a way to prevent downstream crates from implementing traits
    // that you don't think they should implement. If you have `trait Foo: Sealed`, then
    // downstream crates cannot `impl Foo` because they cannot `impl Sealed`.
    //
    // Semantic versioning is one reason you might want this.
    // We currently use this as a bound for `datatype::PhysicalType` and `datatype::LogicalType`
    // so that we won't accept something that we don't know about for the C API calls.
    pub trait Sealed {}

    macro_rules! sealed {
        ($($DT:ty),+) => {
            $(
                impl crate::private::Sealed for $DT {}
            )+
        }
    }

    pub(crate) use sealed;
}

#[cfg(any(test, feature = "proptest-strategies"))]
pub(crate) mod tests {
    use std::str::FromStr;

    pub fn env<T>(env: &str) -> Option<T>
    where
        T: FromStr,
    {
        match std::env::var(env) {
            Ok(value) => Some(
                T::from_str(&value)
                    .unwrap_or_else(|_| panic!("Invalid value for {}", env)),
            ),
            Err(_) => None,
        }
    }
}
