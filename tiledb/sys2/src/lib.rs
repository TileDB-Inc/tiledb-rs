pub mod attribute;
pub mod buffer;
pub mod config;
pub mod context;
pub mod datatype;
pub mod dimension;
pub mod domain;
pub mod enumeration;
pub mod filesystem;
pub mod filter;
pub mod filter_list;
pub mod filter_type;
pub mod types;
pub mod utils;
pub mod webp_format;

mod private {
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
