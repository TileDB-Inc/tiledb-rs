use std::ops::Deref;

use crate::context::{CApiInterface, Context, ContextBound};
use crate::error::Error;
use crate::{Array, Result as TileDBResult, array::RawArray};

pub mod buffer;
pub mod condition;
pub mod read;
pub mod subarray;
pub mod write;

pub use self::condition::QueryConditionExpr;
pub use self::read::{
    ReadBuilder, ReadQuery, ReadQueryBuilder, ReadStepOutput, TypedReadBuilder,
};
pub use self::subarray::{Builder as SubarrayBuilder, Subarray};
pub use self::write::{WriteBuilder, WriteQuery};

use self::condition::QueryConditionBuilder;
use self::subarray::RawSubarray;

pub type QueryType = crate::array::Mode;
pub type QueryLayout = crate::array::CellOrder;

// TODO: this is basically just to patch things over
// to prevent conflicting impl errors (because PhysicalType
// comes from tiledb_common but the traits are defined in this crate)
// we will also split the query adapter stuff out of this crate
// but that will be more complicated
pub trait CellValue: tiledb_common::datatype::PhysicalType {}

impl CellValue for u8 {}
impl CellValue for u16 {}
impl CellValue for u32 {}
impl CellValue for u64 {}
impl CellValue for i8 {}
impl CellValue for i16 {}
impl CellValue for i32 {}
impl CellValue for i64 {}
impl CellValue for f32 {}
impl CellValue for f64 {}

pub enum RawQuery {
    Owned(*mut ffi::tiledb_query_t),
}

impl Deref for RawQuery {
    type Target = *mut ffi::tiledb_query_t;
    fn deref(&self) -> &Self::Target {
        let RawQuery::Owned(ffi) = self;
        ffi
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

    /// Get the subarray for this query.
    ///
    /// The Subarray is tied to the lifetime of the Query.
    ///
    /// ```compile_fail,E0505
    /// # use tiledb_api::query::{Query, QueryBase, Subarray};
    /// fn invalid_use(query: QueryBase) {
    ///     let subarray = query.subarray().unwrap();
    ///     drop(query);
    ///     /// The subarray should not be usable after the query is dropped.
    ///     let _ = subarray.ranges();
    /// }
    /// ```
    fn subarray(&self) -> TileDBResult<Subarray<'_>> {
        let ctx = self.base().context();
        let c_query = *self.base().raw;
        let mut c_subarray: *mut ffi::tiledb_subarray_t = out_ptr!();
        ctx.capi_call(|ctx| unsafe {
            ffi::tiledb_query_get_subarray_t(ctx, c_query, &mut c_subarray)
        })?;

        Ok(Subarray::new(
            self.base().array().schema()?,
            RawSubarray::Owned(c_subarray),
        ))
    }
}

pub struct QueryBase {
    array: Array,
    raw: RawQuery,
}

impl ContextBound for QueryBase {
    fn context(&self) -> Context {
        self.array.context()
    }
}

impl QueryBase {
    fn cquery(&self) -> &RawQuery {
        &self.raw
    }

    /// Executes a single step of the query.
    fn do_submit(&self) -> TileDBResult<()> {
        let c_query = **self.cquery();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_query_submit(ctx, c_query)
        })?;
        Ok(())
    }

    /// Returns the ffi status of the last submit()
    fn capi_status(&self) -> TileDBResult<ffi::tiledb_query_status_t> {
        let c_query = **self.cquery();
        let mut c_status: ffi::tiledb_query_status_t = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_query_get_status(ctx, c_query, &mut c_status)
        })?;
        Ok(c_status)
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
        let c_query = **self.base().cquery();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_query_finalize(ctx, c_query)
        })?;

        Ok(self.array)
    }
}

impl ReadQuery for QueryBase {
    type Intermediate = ();
    type Final = ();

    fn step(
        &mut self,
    ) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>> {
        self.do_submit()?;

        match self.capi_status()? {
            ffi::tiledb_query_status_t_TILEDB_FAILED => {
                Err(Error::from(self.context().get_last_error()
                        .expect("libtiledb context did not have error for failed query status")))
            }
            ffi::tiledb_query_status_t_TILEDB_COMPLETED => {
                Ok(ReadStepOutput::Final(()))
            }
            ffi::tiledb_query_status_t_TILEDB_INPROGRESS => unreachable!(),
            ffi::tiledb_query_status_t_TILEDB_INCOMPLETE => {
                /*
                 * Note: the returned status itself is not enough to distinguish between
                 * "no results, allocate more space plz" and "there are more results after you consume these".
                 * The API tiledb_query_get_status_details exists but is experimental,
                 * so we will worry about it later.
                 * For now: it's a fair assumption that the user requested data, and that is
                 * where we will catch the difference. See RawReadQuery.
                 * We also assume that the same number of records are filled in for all
                 * queried data - if a result is empty for one attribute then it will be so
                 * for all attributes.
                 */
                Ok(ReadStepOutput::Intermediate(()))
            }
            ffi::tiledb_query_status_t_TILEDB_UNINITIALIZED => {
                unreachable!()
            }
            ffi::tiledb_query_status_t_TILEDB_INITIALIZED => unreachable!(),
            unrecognized => Err(Error::Internal(format!(
                "Unrecognized query status: {unrecognized}",
            ))),
        }
    }
}

pub trait QueryBuilder: Sized {
    type Query: Query;

    fn base(&self) -> &BuilderBase;

    fn layout(self, layout: QueryLayout) -> TileDBResult<Self>
    where
        Self: Sized,
    {
        let c_query = **self.base().cquery();
        let c_layout = ffi::tiledb_layout_t::from(layout);
        self.base().capi_call(|ctx| unsafe {
            ffi::tiledb_query_set_layout(ctx, c_query, c_layout)
        })?;
        Ok(self)
    }

    /// Get the in-progress subarray for this query.
    ///
    /// The returned `Subarray` is tied to the lifetime of `self`.
    fn subarray(&self) -> TileDBResult<Subarray<'_>> {
        let ctx = self.base().context();
        let c_query = *self.base().query.raw;
        let mut c_subarray: *mut ffi::tiledb_subarray_t = out_ptr!();
        ctx.capi_call(|ctx| unsafe {
            ffi::tiledb_query_get_subarray_t(ctx, c_query, &mut c_subarray)
        })?;

        Ok(Subarray::new(
            self.base().array().schema()?,
            RawSubarray::Owned(c_subarray),
        ))
    }

    fn start_subarray(self) -> TileDBResult<SubarrayBuilder<Self>>
    where
        Self: Sized,
    {
        SubarrayBuilder::for_query(self)
    }

    fn query_condition(self, qc: QueryConditionExpr) -> TileDBResult<Self> {
        let raw = qc.build(&self.base().context())?;
        let c_query = **self.base().cquery();
        let c_cond = *raw;
        self.base().capi_call(|ctx| unsafe {
            ffi::tiledb_query_set_condition(ctx, c_query, c_cond)
        })?;
        Ok(self)
    }

    fn build(self) -> Self::Query;
}

pub struct BuilderBase {
    query: QueryBase,
}

impl ContextBound for BuilderBase {
    fn context(&self) -> Context {
        self.query.context()
    }
}

impl BuilderBase {
    fn carray(&self) -> &RawArray {
        self.query.array.capi()
    }
    fn cquery(&self) -> &RawQuery {
        &self.query.raw
    }

    pub fn array(&self) -> &Array {
        &self.query.array
    }
}

impl QueryBuilder for BuilderBase {
    type Query = QueryBase;

    fn base(&self) -> &BuilderBase {
        self
    }

    fn build(self) -> Self::Query {
        self.query
    }
}

impl BuilderBase {
    fn new(array: Array, query_type: QueryType) -> TileDBResult<Self> {
        let c_array = **array.capi();
        let c_query_type = ffi::tiledb_query_type_t::from(query_type);
        let mut c_query: *mut ffi::tiledb_query_t = out_ptr!();
        array.capi_call(|ctx| unsafe {
            ffi::tiledb_query_alloc(ctx, c_array, c_query_type, &mut c_query)
        })?;

        Ok(BuilderBase {
            query: QueryBase {
                array,
                raw: RawQuery::Owned(c_query),
            },
        })
    }
}

pub trait ToReadQuery {
    type ReadBuilder<'data, B>
    where
        Self: 'data;

    /// Prepares a read query to read the fields written by this operation
    /// restricted to the subarray represented by this write.
    fn attach_read<'data, B>(
        &'data self,
        b: B,
    ) -> TileDBResult<Self::ReadBuilder<'data, B>>
    where
        B: ReadQueryBuilder<'data>;
}

pub trait ToWriteQuery {
    /// Prepares a write query to insert data from this write.
    fn attach_write<'data>(
        &'data self,
        b: WriteBuilder<'data>,
    ) -> TileDBResult<WriteBuilder<'data>>;
}

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;
