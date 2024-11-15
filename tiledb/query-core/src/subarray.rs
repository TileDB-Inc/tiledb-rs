use std::collections::HashMap;

use tiledb_common::range::Range;

use super::QueryBuilder;

pub type SubarrayData = HashMap<String, Vec<Range>>;

#[derive(Default)]
pub struct SubarrayBuilder {
    subarray: SubarrayData,
}

impl SubarrayBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_range<IntoRange: Into<Range>>(
        mut self,
        dimension: &str,
        range: IntoRange,
    ) -> Self {
        self.subarray
            .entry(dimension.to_string())
            .or_default()
            .push(range.into());
        self
    }

    pub fn build(self) -> SubarrayData {
        self.subarray
    }
}

pub struct SubarrayBuilderForQuery {
    query_builder: QueryBuilder,
    subarray_builder: SubarrayBuilder,
}

impl SubarrayBuilderForQuery {
    pub(crate) fn new(query_builder: QueryBuilder) -> Self {
        Self {
            query_builder,
            subarray_builder: SubarrayBuilder::new(),
        }
    }

    pub fn end_subarray(self) -> QueryBuilder {
        self.query_builder
            .with_subarray_data(self.subarray_builder.build())
    }

    pub fn add_range<IntoRange: Into<Range>>(
        mut self,
        dimension: &str,
        range: IntoRange,
    ) -> Self {
        self.subarray_builder =
            self.subarray_builder.add_range(dimension, range);
        self
    }
}
