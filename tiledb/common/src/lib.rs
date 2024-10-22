#[cfg(feature = "option-subset")]
#[macro_use]
extern crate tiledb_proc_macro;
extern crate tiledb_sys_defs as ffi;

pub mod array;
pub mod datatype;
pub mod filter;
pub mod key;
pub mod metadata;
pub mod range;
pub mod vfs;

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
