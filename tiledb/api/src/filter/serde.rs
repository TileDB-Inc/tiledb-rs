use tiledb_common::filter::FilterData;

use super::{Filter, FilterList, FilterListBuilder};
use crate::error::Error as TileDBError;
use crate::{Context, Result as TileDBResult};

impl TryFrom<&Filter> for FilterData {
    type Error = TileDBError;

    fn try_from(filter: &Filter) -> Result<Self, Self::Error> {
        filter.filter_data()
    }
}

impl TryFrom<Filter> for FilterData {
    type Error = TileDBError;

    fn try_from(filter: Filter) -> Result<Self, Self::Error> {
        filter.filter_data()
    }
}

impl crate::Factory for FilterData {
    type Item = Filter;

    fn create(&self, context: &Context) -> TileDBResult<Self::Item> {
        Filter::create(context, self)
    }
}

impl TryFrom<&FilterList> for Vec<FilterData> {
    type Error = crate::error::Error;

    fn try_from(pipeline: &FilterList) -> Result<Self, Self::Error> {
        pipeline
            .to_vec()?
            .into_iter()
            .map(|f| FilterData::try_from(f))
            .collect::<Result<Self, Self::Error>>()
    }
}

impl TryFrom<FilterList> for Vec<FilterData> {
    type Error = TileDBError;

    fn try_from(pipeline: FilterList) -> Result<Self, Self::Error> {
        Self::try_from(&pipeline)
    }
}

impl crate::Factory for Vec<FilterData> {
    type Item = FilterList;

    fn create(&self, context: &Context) -> TileDBResult<Self::Item> {
        Ok(self
            .iter()
            .fold(FilterListBuilder::new(context), |b, filter| {
                b?.add_filter_data(filter)
            })?
            .build())
    }
}
