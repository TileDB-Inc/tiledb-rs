use serde::{Deserialize, Serialize};
use tiledb::filter::{FilterData, FilterList, FilterListBuilder};
use tiledb::{context::Context as TileDBContext, Result as TileDBResult};

/// Encapsulates TileDB filter data for storage in Arrow Field metadata
#[derive(Deserialize, Serialize)]
pub struct FilterMetadata {
    filters: Vec<FilterData>,
}

impl FilterMetadata {
    pub fn new(
        filters: &tiledb::filter::list::FilterList,
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
    pub fn apply<'ctx>(
        &self,
        mut filters: FilterListBuilder<'ctx>,
    ) -> TileDBResult<FilterListBuilder<'ctx>> {
        for filter in self.filters.iter() {
            filters = filters.add_filter_data(filter.clone())?;
        }
        Ok(filters)
    }

    pub fn create<'ctx>(
        &self,
        context: &'ctx TileDBContext,
    ) -> TileDBResult<FilterList<'ctx>> {
        Ok(self.apply(FilterListBuilder::new(context)?)?.build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tiledb::context::Context as TileDBContext;
    use tiledb::Factory;

    #[test]
    fn test_serialize_invertibility() {
        let c: TileDBContext = TileDBContext::new().unwrap();

        proptest!(|(filters_in in tiledb::filter::strategy::prop_filter_pipeline(Default::default()))| {
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
