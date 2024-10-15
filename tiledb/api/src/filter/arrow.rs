use crate::filter::{FilterData, FilterList, FilterListBuilder};
use crate::{context::Context as TileDBContext, Result as TileDBResult};
use serde::{Deserialize, Serialize};

/// Encapsulates TileDB filter data for storage in Arrow Field metadata
#[derive(Deserialize, Serialize)]
pub struct FilterMetadata {
    filters: Vec<FilterData>,
}

impl FilterMetadata {
    pub fn new(
        filters: &crate::filter::list::FilterList,
    ) -> TileDBResult<Self> {
        Ok(FilterMetadata {
            filters: filters
                .to_vec()?
                .into_iter()
                .map(|f| f.filter_data())
                .collect::<TileDBResult<Vec<FilterData>>>()?,
        })
    }

    /// Updates a FilterListBuilder with the contents of this object
    pub fn apply(
        &self,
        mut filters: FilterListBuilder,
    ) -> TileDBResult<FilterListBuilder> {
        for filter in self.filters.iter() {
            filters = filters.add_filter_data(filter.clone())?;
        }
        Ok(filters)
    }

    pub fn create(&self, context: &TileDBContext) -> TileDBResult<FilterList> {
        Ok(self.apply(FilterListBuilder::new(context)?)?.build())
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use tiledb_serde::filter::strategy::FilterPipelineStrategy;

    use super::*;
    use crate::{Context, Factory};

    #[test]
    fn test_serialize_invertibility() {
        let c: TileDBContext = Context::new().unwrap();

        proptest!(|(filters_in in FilterPipelineStrategy::default())| {
            let filters_in = filters_in.create(&c)
                .expect("Error constructing arbitrary filter list");
            let metadata = FilterMetadata::new(&filters_in)
                .expect("Error serializing filter list");
            let filters_out = metadata.create(&c)
                .expect("Error deserializing filter list");

            assert_eq!(filters_in, filters_out);
        });
    }
}
