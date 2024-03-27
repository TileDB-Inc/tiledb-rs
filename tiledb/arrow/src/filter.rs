use serde::{Deserialize, Serialize};
use tiledb::filter::{FilterData, FilterListBuilder};
use tiledb::Result as TileDBResult;

/// Encapsulates TileDB filter data for storage in Arrow Field metadata
#[derive(Deserialize, Serialize)]
pub struct FilterMetadata {
    filters: Vec<FilterData>,
}

impl FilterMetadata {
    pub fn new(
        filters: &tiledb::filter_list::FilterList,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tiledb::context::Context as TileDBContext;

    #[test]
    fn test_serialize_invertibility() {
        let c: TileDBContext = TileDBContext::new().unwrap();

        proptest!(|(filters_in in tiledb_test::filter::arbitrary_list(&c))| {
            let filters_in = filters_in.expect("Error constructing arbitrary filter list");
            let metadata = FilterMetadata::new(&filters_in).expect("Error serializing filter list");
            let filters_out = metadata.apply(FilterListBuilder::new(&c).unwrap()).expect("Error deserializing filter list").build();

            assert_eq!(filters_in, filters_out);
        });
    }
}
