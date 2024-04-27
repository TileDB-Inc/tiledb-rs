use std::ops::Deref;

use crate::array::Array;
use crate::context::{CApiInterface, Context, ContextBound};
use crate::error::Error;
use crate::range::Range;
use crate::Result as TileDBResult;

pub mod buffer;
pub mod conditions;
pub mod read;
pub mod subarray;
pub mod write;

pub use self::conditions::{QueryCondition, QueryConditionExpr};
pub use self::read::{
    ReadBuilder, ReadQuery, ReadQueryBuilder, ReadStepOutput, TypedReadBuilder,
};
pub use self::subarray::{Builder as SubarrayBuilder, Subarray};
pub use self::write::{WriteBuilder, WriteQuery};

use self::subarray::RawSubarray;

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

pub trait Query {
    fn base(&self) -> &QueryBase;

    fn finalize(self) -> TileDBResult<Array>
    where
        Self: Sized;

    fn subarray(&self) -> TileDBResult<Subarray> {
        let ctx = self.base().context();
        let c_query = *self.base().raw;
        let mut c_subarray: *mut ffi::tiledb_subarray_t = out_ptr!();
        ctx.capi_call(|ctx| unsafe {
            ffi::tiledb_query_get_subarray_t(ctx, c_query, &mut c_subarray)
        })?;

        Ok(Subarray::new(ctx, RawSubarray::Owned(c_subarray)))
    }

    fn ranges(&self) -> TileDBResult<Vec<Vec<Range>>> {
        let schema = self.base().array.schema()?;
        let subarray = self.subarray()?;
        subarray.ranges(&schema)
    }
}

pub struct QueryBase {
    array: Array,
    raw: RawQuery,
}

impl ContextBound for QueryBase {
    fn context(&self) -> &Context {
        self.array.context()
    }
}

impl QueryBase {
    fn capi(&self) -> *mut ffi::tiledb_query_t {
        *self.raw
    }

    /// Execute the query
    fn do_submit(&self) -> TileDBResult<()> {
        let c_query = self.capi();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_query_submit(ctx, c_query)
        })?;
        Ok(())
    }

    /// Returns the ffi status of the last submit()
    fn capi_status(&self) -> TileDBResult<ffi::tiledb_query_status_t> {
        let c_query = self.capi();
        let mut c_status: ffi::tiledb_query_status_t = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_query_get_status(ctx, c_query, &mut c_status)
        })
        .map(|_| c_status)
    }

    pub fn array(&self) -> &Array {
        &self.array
    }
}

impl Query for QueryBase {
    fn base(&self) -> &QueryBase {
        self
    }

    fn finalize(self) -> TileDBResult<Array> {
        let c_query = self.base().capi();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_query_finalize(ctx, c_query)
        })?;

        Ok(self.array)
    }
}

pub trait QueryBuilder {
    fn base(&self) -> &Builder;

    fn layout(self, layout: QueryLayout) -> TileDBResult<Self>
    where
        Self: Sized,
    {
        let c_query = **self.base().cquery();
        let c_layout = layout.capi_enum();
        self.base().capi_call(|ctx| unsafe {
            ffi::tiledb_query_set_layout(ctx, c_query, c_layout)
        })?;
        Ok(self)
    }

    fn start_subarray(self) -> TileDBResult<SubarrayBuilder<Self>>
    where
        Self: Sized,
    {
        SubarrayBuilder::for_query(self)
    }

    fn query_condition(self, qc: QueryCondition) -> TileDBResult<Self>
    where
        Self: Sized,
    {
        let c_query = **self.base().cquery();
        let c_cond = qc.capi();
        self.base().capi_call(|ctx| unsafe {
            ffi::tiledb_query_set_condition(ctx, c_query, c_cond)
        })?;
        Ok(self)
    }
}

pub struct Builder {
    query: QueryBase,
}

impl ContextBound for Builder {
    fn context(&self) -> &Context {
        self.query.context()
    }
}

impl Builder {
    fn new(array: Array, query_type: QueryType) -> TileDBResult<Self> {
        let c_array = array.capi();
        let c_query_type = query_type.capi_enum();
        let mut c_query: *mut ffi::tiledb_query_t = out_ptr!();
        array.capi_call(|ctx| unsafe {
            ffi::tiledb_query_alloc(ctx, c_array, c_query_type, &mut c_query)
        })?;
        Ok(Builder {
            query: QueryBase {
                array,
                raw: RawQuery::Owned(c_query),
            },
        })
    }
}
