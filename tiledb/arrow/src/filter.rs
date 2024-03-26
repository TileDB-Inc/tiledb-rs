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
