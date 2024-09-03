//! Extra query adaptors, functions and macros.
//!
//! Import traits from this crate to extend the
//! various [`tiledb`] query building traits.

extern crate tiledb;

mod aggregate;

pub use self::aggregate::*;
