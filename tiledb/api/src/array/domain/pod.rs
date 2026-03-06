use tiledb_pod::array::dimension::DimensionData;

use super::{Builder, Domain};
use crate::error::Error as TileDBError;
use crate::{Context, Factory, Result as TileDBResult};

impl TryFrom<&Domain> for Vec<DimensionData> {
    type Error = TileDBError;

    fn try_from(domain: &Domain) -> Result<Self, Self::Error> {
        Ok((0..domain.num_dimensions()?)
            .map(|d| DimensionData::try_from(&domain.dimension(d)?))
            .collect::<TileDBResult<Vec<DimensionData>>>()?)
    }
}

impl Factory for [DimensionData] {
    type Item = Domain;

    fn create(&self, context: &Context) -> TileDBResult<Self::Item> {
        Ok(self
            .iter()
            .try_fold(Builder::new(context)?, |b, d| {
                b.add_dimension(d.create(context)?)
            })?
            .build())
    }
}
