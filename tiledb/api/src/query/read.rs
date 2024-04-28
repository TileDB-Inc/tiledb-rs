use super::traits::{Query, QueryBuilder, QueryFinalizer};
use super::RawQuery;
use crate::array::Array;
use crate::context::{CApiInterface, Context, ContextBound};
use crate::Result as TileDBResult;

pub struct ReadQuery {
    array: Array,
    raw: RawQuery,
}

impl Query for ReadQuery {
    fn context(&self) -> &Context {
        self.array.context()
    }

    fn array(&self) -> &Array {
        &self.array
    }

    fn capi(&self) -> *mut ffi::tiledb_query_t {
        *self.raw
    }
}

impl QueryFinalizer for ReadQuery {
    fn context(&self) -> &Context {
        self.array.context()
    }

    fn capi(&self) -> *mut ffi::tiledb_query_t {
        *self.raw
    }
}

impl ReadQuery {
    fn new(array: Array, raw: RawQuery) -> Self {
        Self { array, raw }
    }

    pub fn finalize(self) -> TileDBResult<Array> {
        self.do_finalize()?;
        Ok(self.array)
    }
}

pub struct ReadQueryBuilder {
    query: ReadQuery,
}

impl ContextBound for ReadQueryBuilder {
    fn context(&self) -> &Context {
        self.query.array.context()
    }
}

impl QueryBuilder for ReadQueryBuilder {
    fn context(&self) -> &Context {
        ContextBound::context(self)
    }

    fn array(&self) -> &Array {
        self.query.array()
    }

    fn capi(&self) -> *mut ffi::tiledb_query_t {
        *self.query.raw
    }
}

impl ReadQueryBuilder {
    pub fn new(array: Array, raw: RawQuery) -> Self {
        Self {
            query: ReadQuery::new(array, raw),
        }
    }
}
