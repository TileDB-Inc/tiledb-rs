pub mod subarray;

use std::ops::Deref;

use crate::context::{CApiInterface, Context, ContextBound};
use crate::error::Error;
use crate::{array::RawArray, Array, Result as TileDBResult};

pub mod read;
pub mod write;

mod private {
    use super::*;
    use std::rc::Rc;
    use std::sync::Arc;

    pub trait QueryCAPIInterface {
        fn carray(&self) -> &RawArray;
        fn cquery(&self) -> &RawQuery;

        fn do_submit<'ctx>(&self) -> TileDBResult<()>
        where
            Self: ContextBound<'ctx> + CApiInterface,
        {
            let c_context = self.context().capi();
            let c_query = **self.cquery();
            self.capi_return(unsafe {
                ffi::tiledb_query_submit(c_context, c_query)
            })?;
            Ok(())
        }

        fn do_submit_read<'ctx>(&self) -> TileDBResult<ReadStepOutput<(), ()>>
        where
            Self: ContextBound<'ctx> + CApiInterface,
        {
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

        fn capi_status<'ctx>(&self) -> TileDBResult<ffi::tiledb_query_status_t>
        where
            Self: ContextBound<'ctx> + CApiInterface,
        {
            let c_context = self.context().capi();
            let c_query = **self.cquery();
            let mut c_status: ffi::tiledb_query_status_t = out_ptr!();
            self.capi_return(unsafe {
                ffi::tiledb_query_get_status(c_context, c_query, &mut c_status)
            })
            .map(|_| c_status)
        }
    }

    impl<T> QueryCAPIInterface for Arc<T>
    where
        T: QueryCAPIInterface,
    {
        fn carray(&self) -> &RawArray {
            (**self).carray()
        }

        fn cquery(&self) -> &RawQuery {
            (**self).cquery()
        }
    }

    impl<T> QueryCAPIInterface for Rc<T>
    where
        T: QueryCAPIInterface,
    {
        fn carray(&self) -> &RawArray {
            (**self).carray()
        }

        fn cquery(&self) -> &RawQuery {
            (**self).cquery()
        }
    }
}

use self::private::QueryCAPIInterface;
pub use self::read::{
    ReadBuilder, ReadQuery, ReadQueryBuilder, ReadStepOutput, TypedReadBuilder,
};
pub use self::subarray::{Builder as SubarrayBuilder, Subarray};
pub use self::write::WriteBuilder;

pub type QueryType = crate::array::Mode;
pub type QueryLayout = crate::array::CellOrder;

pub enum RawQuery {
    Owned(*mut ffi::tiledb_query_t),
    Borrowed(*mut ffi::tiledb_query_t),
}

impl RawQuery {
    pub fn borrow(&self) -> RawQuery {
        RawQuery::Borrowed(**self)
    }
}

impl Deref for RawQuery {
    type Target = *mut ffi::tiledb_query_t;
    fn deref(&self) -> &Self::Target {
        match *self {
            RawQuery::Owned(ref ffi) => ffi,
            RawQuery::Borrowed(ref ffi) => ffi,
        }
    }
}

impl Drop for RawQuery {
    fn drop(&mut self) {
        if let RawQuery::Owned(ref mut ffi) = *self {
            unsafe { ffi::tiledb_query_free(ffi) }
        }
    }
}

#[derive(ContextBound)]
pub struct Query<'ctx> {
    #[base(ContextBound)]
    array: Array<'ctx>,
    raw: RawQuery,
}

impl<'ctx> QueryCAPIInterface for Query<'ctx> {
    fn carray(&self) -> &RawArray {
        self.array.capi()
    }

    fn cquery(&self) -> &RawQuery {
        &self.raw
    }
}

impl<'ctx> ReadQuery for Query<'ctx> {
    type Intermediate = ();
    type Final = ();

    fn step(
        &mut self,
    ) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>> {
        self.do_submit_read()
    }
}

pub trait QueryBuilder<'ctx>:
    ContextBound<'ctx> + private::QueryCAPIInterface + Sized
{
    type Query;

    fn layout(self, layout: QueryLayout) -> TileDBResult<Self> {
        let c_context = self.context().capi();
        let c_query = **self.cquery();
        let c_layout = layout.capi_enum();
        self.capi_return(unsafe {
            ffi::tiledb_query_set_layout(c_context, c_query, c_layout)
        })?;
        Ok(self)
    }

    fn start_subarray(self) -> TileDBResult<SubarrayBuilder<'ctx, Self>> {
        SubarrayBuilder::for_query(self)
    }

    fn build(self) -> Self::Query;
}

#[derive(ContextBound, QueryCAPIInterface)]
struct BuilderBase<'ctx> {
    #[base(ContextBound, QueryCAPIInterface)]
    query: Query<'ctx>,
}

impl<'ctx> QueryBuilder<'ctx> for BuilderBase<'ctx> {
    type Query = Query<'ctx>;

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
        let c_array = **array.capi();
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
