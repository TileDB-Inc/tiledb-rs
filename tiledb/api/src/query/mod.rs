use std::ops::Deref;

use crate::context::{CApiInterface, Context, ContextBound};
use crate::error::Error;
use crate::{array::RawArray, Array, Result as TileDBResult};

pub mod buffer;
pub mod conditions;
pub mod read;
pub mod subarray;
pub mod write;

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;

pub use self::conditions::QueryConditionExpr;
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
        let RawQuery::Owned(ref ffi) = self;
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
    /// # use tiledb::query::{Query, QueryBase, Subarray};
    /// fn invalid_use(query: QueryBase) {
    ///     let subarray = query.subarray().unwrap();
    ///     drop(query);
    ///     /// The subarray should not be usable after the query is dropped.
    ///     let _ = subarray.ranges();
    /// }
    /// ```
    fn subarray(&self) -> TileDBResult<Subarray> {
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
                Err(self.context().expect_last_error())
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
                "Unrecognized query status: {}",
                unrecognized
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
        let c_layout = layout.capi_enum();
        self.base().capi_call(|ctx| unsafe {
            ffi::tiledb_query_set_layout(ctx, c_query, c_layout)
        })?;
        Ok(self)
    }

    /// Get the in-progress subarray for this query.
    ///
    /// The returned `Subarray` is tied to the lifetime of `self`.
    fn subarray(&self) -> TileDBResult<Subarray> {
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
        let c_query_type = query_type.capi_enum();
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
