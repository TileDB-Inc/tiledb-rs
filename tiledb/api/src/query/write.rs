use super::traits::{Query, QueryBuilder};
use super::RawQuery;
use crate::array::Array;
use crate::context::{CApiInterface, Context, ContextBound};

pub struct WriteQuery {
    array: Array,
    raw: RawQuery,
}

impl ContextBound for WriteQuery {
    fn context(&self) -> &Context {
        self.array.context()
    }
}

impl Query for WriteQuery {
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

pub struct WriteQueryBuilder {
    query: WriteQuery,
}

impl ContextBound for WriteQueryBuilder {
    fn context(&self) -> &Context {
        self.query.array.context()
    }
}

impl QueryBuilder for WriteQueryBuilder {
    fn context(&self) -> &Context {
        self.query.array.context()
    }

    fn array(&self) -> &Array {
        self.query.array()
    }

    fn capi(&self) -> *mut ffi::tiledb_query_t {
        *self.query.raw
    }
}
