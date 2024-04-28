use std::ops::Deref;

use crate::array::Array;
use crate::context::CApiInterface;
use crate::Result as TileDBResult;

pub mod buffer;
pub mod conditions;
pub mod read;
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

pub struct Builder {}

impl Builder {
    fn new_reader(array: Array) -> TileDBResult<ReadQueryBuilder> {
        let c_array = array.capi();
        let c_query_type = QueryType::Read.capi_enum();
        let mut c_query: *mut ffi::tiledb_query_t = out_ptr!();
        array.capi_call(|ctx| unsafe {
            ffi::tiledb_query_alloc(ctx, c_array, c_query_type, &mut c_query)
        })?;
        Ok(ReadQueryBuilder::new(array, RawQuery::Owned(c_query)))
    }
}
