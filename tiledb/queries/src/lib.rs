//! Extra query adaptors, functions and macros.
//!
//! Import traits from this crate to extend the
//! various [`tiledb`] query building traits.

mod aggregate;

pub use self::aggregate::*;
