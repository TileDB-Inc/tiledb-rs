pub mod array;
pub mod array_type;
pub mod attribute;
pub mod buffer;
pub mod config;
pub mod context;
pub mod datatype;
pub mod dimension;
pub mod domain;
pub mod encryption_type;
pub mod enumeration;
pub mod error;
pub mod filesystem;
pub mod filter;
pub mod filter_list;
pub mod filter_type;
pub mod layout;
pub mod mode;
pub mod query;
pub mod query_status;
pub mod schema;
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
