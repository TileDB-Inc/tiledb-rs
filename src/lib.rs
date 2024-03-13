extern crate tiledb_sys as ffi;

pub mod attribute;
pub mod config;
pub mod context;
pub mod error;
pub mod filter;
pub mod filter_list;
pub mod string;

type Result<T> = std::result::Result<T, error::Error>;
