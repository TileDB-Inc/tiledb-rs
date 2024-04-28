use std::ops::Deref;

pub mod buffer;
pub mod conditions;
pub mod read;
pub mod sizeinfo;
pub mod subarray;
pub mod traits;
pub mod write;

pub use self::conditions::{QueryCondition, QueryConditionExpr};
pub use self::read::{ReadQuery, ReadQueryBuilder};
pub use self::subarray::{Builder as SubarrayBuilder, Subarray};
pub use self::write::{WriteQuery, WriteQueryBuilder};

pub type QueryType = crate::array::Mode;
pub type QueryLayout = crate::array::CellOrder;

pub enum RawQuery {
    Owned(*mut ffi::tiledb_query_t),
}

impl Deref for RawQuery {
    type Target = *mut ffi::tiledb_query_t;
    fn deref(&self) -> &Self::Target {
        match *self {
            RawQuery::Owned(ref ffi) => ffi,
        }
    }
}

impl Drop for RawQuery {
    fn drop(&mut self) {
        let RawQuery::Owned(ref mut ffi) = *self;
        unsafe { ffi::tiledb_query_free(ffi) }
    }
}
