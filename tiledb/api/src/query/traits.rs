use super::conditions::QueryCondition;
use super::read::ReadQueryBuilder;
use super::subarray::RawSubarray;
use super::write::WriteQueryBuilder;
use super::{QueryLayout, QueryType, RawQuery, Subarray, SubarrayBuilder};

use crate::array::Array;
use crate::context::{CApiInterface, Context, ContextBound};
use crate::range::Range;
use crate::Result as TileDBResult;

pub trait Query: Sized {
    fn context(&self) -> &Context;
    fn array(&self) -> &Array;
    fn capi(&self) -> *mut ffi::tiledb_query_t;

    fn subarray(&self) -> TileDBResult<Subarray> {
        let context = self.context();
        let c_query = self.capi();
        let mut c_subarray: *mut ffi::tiledb_subarray_t = out_ptr!();
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_query_get_subarray_t(ctx, c_query, &mut c_subarray)
        })?;

        Ok(Subarray::new(context, RawSubarray::Owned(c_subarray)))
    }

    fn ranges(&self) -> TileDBResult<Vec<Vec<Range>>> {
        let schema = self.array().schema()?;
        let subarray = self.subarray()?;
        subarray.ranges(&schema)
    }

    /// Execute the query
    fn submit(&self) -> TileDBResult<()> {
        let c_query = self.capi();
        self.context().capi_call(|ctx| unsafe {
            ffi::tiledb_query_submit(ctx, c_query)
        })?;
        Ok(())
    }

    /// Returns the ffi status of the last submit()
    fn capi_status(&self) -> TileDBResult<ffi::tiledb_query_status_t> {
        let c_query = self.capi();
        let mut c_status: ffi::tiledb_query_status_t = out_ptr!();
        self.context()
            .capi_call(|ctx| unsafe {
                ffi::tiledb_query_get_status(ctx, c_query, &mut c_status)
            })
            .map(|_| c_status)
    }
}

pub(crate) trait QueryFinalizer {
    fn context(&self) -> &Context;
    fn capi(&self) -> *mut ffi::tiledb_query_t;

    fn do_finalize(&self) -> TileDBResult<()> {
        let c_query = self.capi();
        self.context().capi_call(|ctx| unsafe {
            ffi::tiledb_query_finalize(ctx, c_query)
        })?;
        Ok(())
    }
}

pub trait QueryBuilder
where
    Self: Sized,
{
    fn context(&self) -> &Context;
    fn array(&self) -> &Array;
    fn capi(&self) -> *mut ffi::tiledb_query_t;

    fn new_reader(array: Array) -> TileDBResult<ReadQueryBuilder> {
        let c_array = array.capi();
        let c_query_type = QueryType::Read.capi_enum();
        let mut c_query: *mut ffi::tiledb_query_t = out_ptr!();
        array.capi_call(|ctx| unsafe {
            ffi::tiledb_query_alloc(ctx, c_array, c_query_type, &mut c_query)
        })?;
        Ok(ReadQueryBuilder::new(array, RawQuery::Owned(c_query)))
    }

    fn layout(self, layout: QueryLayout) -> TileDBResult<Self> {
        let c_query = self.capi();
        let c_layout = layout.capi_enum();
        self.context().capi_call(|ctx| unsafe {
            ffi::tiledb_query_set_layout(ctx, c_query, c_layout)
        })?;
        Ok(self)
    }

    fn start_subarray(self) -> TileDBResult<SubarrayBuilder<Self>> {
        SubarrayBuilder::for_query(self)
    }

    fn query_condition(self, qc: QueryCondition) -> TileDBResult<Self> {
        let c_query = self.capi();
        let c_cond = qc.capi();
        self.context().capi_call(|ctx| unsafe {
            ffi::tiledb_query_set_condition(ctx, c_query, c_cond)
        })?;
        Ok(self)
    }
}
