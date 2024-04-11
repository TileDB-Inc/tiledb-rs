pub mod subarray;

use std::ops::Deref;

use crate::context::{CApiInterface, Context, ContextBound};
use crate::error::Error;
use crate::{Array, Result as TileDBResult};

pub mod read;
pub mod write;

mod private {
    use super::*;

    pub trait QueryCAPIInterface {
        fn raw(&self) -> &RawQuery;
    }
}

pub use self::read::{
    ReadBuilder, ReadQuery, ReadQueryBuilder, ReadStepOutput, TypedReadBuilder,
};
pub use self::write::WriteBuilder;
pub use crate::query::subarray::{Builder as SubarrayBuilder, Subarray};

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
        unsafe { ffi::tiledb_query_free(ffi) };
    }
}

#[derive(ContextBound)]
pub struct Query<'ctx> {
    #[base(ContextBound)]
    array: Array<'ctx>,
    raw: RawQuery,
}

impl<'ctx> Query<'ctx> {
    fn do_submit(&self) -> TileDBResult<()> {
        let c_context = self.context().capi();
        let c_query = *self.raw;
        self.capi_return(unsafe {
            ffi::tiledb_query_submit(c_context, c_query)
        })?;
        Ok(())
    }

    fn capi_status(&self) -> TileDBResult<ffi::tiledb_query_status_t> {
        let c_context = self.context().capi();
        let c_query = *self.raw;
        let mut c_status: ffi::tiledb_query_status_t = out_ptr!();
        self.capi_return(unsafe {
            ffi::tiledb_query_get_status(c_context, c_query, &mut c_status)
        })
        .map(|_| c_status)
    }
}

impl<'ctx> private::QueryCAPIInterface for Query<'ctx> {
    fn raw(&self) -> &RawQuery {
        &self.raw
    }
}

impl<'ctx> ReadQuery for Query<'ctx> {
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
                Ok(ReadStepOutput::Intermediate(()))
            }
            ffi::tiledb_query_status_t_TILEDB_UNINITIALIZED => unreachable!(),
            ffi::tiledb_query_status_t_TILEDB_INITIALIZED => unreachable!(),
            unrecognized => Err(Error::Internal(format!(
                "Unrecognized query status: {}",
                unrecognized
            ))),
        }
    }
}

pub trait QueryBuilder<'ctx>:
    ContextBound<'ctx> + private::QueryCAPIInterface + Sized
{
    type Query;

    fn array(&self) -> &Array;

    fn layout(self, layout: QueryLayout) -> TileDBResult<Self> {
        let c_context = self.context().capi();
        let c_query = **self.raw();
        let c_layout = layout.capi_enum();
        self.capi_return(unsafe {
            ffi::tiledb_query_set_layout(c_context, c_query, c_layout)
        })?;
        Ok(self)
    }

    fn add_subarray(self) -> TileDBResult<SubarrayBuilder<'ctx, Self>> {
        SubarrayBuilder::for_query(self)
    }

    fn build(self) -> Self::Query;
}

#[derive(ContextBound)]
struct BuilderBase<'ctx> {
    #[base(ContextBound)]
    query: Query<'ctx>,
}

impl<'ctx> private::QueryCAPIInterface for BuilderBase<'ctx> {
    fn raw(&self) -> &RawQuery {
        &self.query.raw
    }
}

impl<'ctx> QueryBuilder<'ctx> for BuilderBase<'ctx> {
    type Query = Query<'ctx>;

    fn array(&self) -> &Array {
        &self.query.array
    }

    fn build(self) -> Self::Query {
        self.query
    }
}

impl<'ctx> BuilderBase<'ctx> {
    fn new(
        context: &'ctx Context,
        array: Array<'ctx>,
        query_type: QueryType,
    ) -> TileDBResult<Self> {
        let c_context = context.capi();
        let c_array = array.capi();
        let c_query_type = query_type.capi_enum();
        let mut c_query: *mut ffi::tiledb_query_t = out_ptr!();
        context.capi_return(unsafe {
            ffi::tiledb_query_alloc(
                c_context,
                c_array,
                c_query_type,
                &mut c_query,
            )
        })?;
        Ok(BuilderBase {
            query: Query {
                array,
                raw: RawQuery::Owned(c_query),
            },
        })
    }
}
