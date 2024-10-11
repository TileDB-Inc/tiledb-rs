#[cfg(feature = "option-subset")]
#[macro_use]
extern crate tiledb_proc_macro;
extern crate tiledb_sys as ffi;

pub mod array;

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;
