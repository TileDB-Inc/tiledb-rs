use tiledb_serde::array::dimension::DimensionData;
use tiledb_serde::array::domain::DomainData;

use super::{Builder, Domain};
use crate::error::Error as TileDBError;
use crate::{Context, Factory, Result as TileDBResult};

impl TryFrom<&Domain> for DomainData {
    type Error = TileDBError;

    fn try_from(domain: &Domain) -> Result<Self, Self::Error> {
        Ok(DomainData {
            dimension: (0..domain.num_dimensions()?)
                .map(|d| DimensionData::try_from(&domain.dimension(d)?))
                .collect::<TileDBResult<Vec<DimensionData>>>()?,
        })
    }
}

impl TryFrom<Domain> for DomainData {
    type Error = TileDBError;

    fn try_from(domain: Domain) -> Result<Self, Self::Error> {
        Self::try_from(&domain)
    }
}

impl Factory for DomainData {
    type Item = Domain;

    fn create(&self, context: &Context) -> TileDBResult<Self::Item> {
        Ok(self
            .dimension
            .iter()
            .try_fold(Builder::new(context)?, |b, d| {
                b.add_dimension(d.create(context)?)
            })?
            .build())
    }
}
