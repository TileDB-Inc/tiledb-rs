//! Provides "plain old data" representations of tiledb data structures.
//!
//! "Plain old data" is used to describe types in C++ programming which
//! have no constructors, destructors, or virtual member functions.
//! Values of these types can be duplicated by copying bits.
//!
//! The structures defined in this crate are representations of tiledb
//! logical structures which expose their attributes as public fields.
//! This contrasts with the [tiledb-api] crate where the structures
//! are instead handles to tiledb C API data structures.
//!
//! There is no direct "plain old data" analogue in Rust; the use
//! of the term in this crate intends to capture the spirit rather than the letter
//! of what it means to be plain old data.
//!
//! The structures in this crate can be used to construct, inspect, or manipulate `libtiledb`
//! data structures without invoking tiledb's C API. This allows an
//! application to be built without linking against libtiledb.
//! This might be desirable for embedding a description of a
//! tiledb schema in a remote procedure call, for example.
//!
//! ## Features
//!
//! * `proptest-strategies`: Provides `proptest::arbitrary::Arbitrary` implementations for many of
//!   the structures defined in this crate for use with
//!   [property-based testing](https://proptest-rs.github.io/proptest/intro.html).
//! * `serde`: Provides `serde::Deserialize` and `serde::Serialize` implemenations for many
//!   of the structures defined in this crate.
#[cfg(feature = "option-subset")]
#[macro_use]
extern crate tiledb_proc_macro;
extern crate tiledb_sys as ffi;

pub mod array;
pub mod filter;
pub mod query;
