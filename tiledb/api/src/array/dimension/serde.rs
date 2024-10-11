use tiledb_common::array::dimension::DimensionConstraints;
use tiledb_common::filter::FilterData;
use tiledb_common::physical_type_go;
use tiledb_serde::array::dimension::DimensionData;

use super::{Builder, Dimension};
use crate::error::Error as TileDBError;
use crate::{Context, Factory, Result as TileDBResult};

impl TryFrom<&Dimension> for DimensionData {
    type Error = TileDBError;

    fn try_from(dim: &Dimension) -> Result<Self, Self::Error> {
        let datatype = dim.datatype()?;
        let constraints = physical_type_go!(datatype, DT, {
            let domain = dim.domain::<DT>()?;
            let extent = dim.extent::<DT>()?;
            if let Some(domain) = domain {
                DimensionConstraints::from((domain, extent))
            } else {
                assert!(extent.is_none());
                DimensionConstraints::StringAscii
            }
        });

        Ok(DimensionData {
            name: dim.name()?,
            datatype,
            constraints,
            filters: {
                let fl = Vec::<FilterData>::try_from(&dim.filters()?)?;
                if fl.is_empty() {
                    None
                } else {
                    Some(fl)
                }
            },
        })
    }
}

impl TryFrom<Dimension> for DimensionData {
    type Error = TileDBError;

    fn try_from(dimension: Dimension) -> Result<Self, Self::Error> {
        Self::try_from(&dimension)
    }
}

impl Factory for DimensionData {
    type Item = Dimension;

    fn create(&self, context: &Context) -> TileDBResult<Self::Item> {
        let mut b = Builder::new(
            context,
            &self.name,
            self.datatype,
            self.constraints.clone(),
        )?;

        if let Some(fl) = self.filters.as_ref() {
            b = b.filters(fl.create(context)?)?;
        }

        Ok(b.cell_val_num(self.cell_val_num())?.build())
    }
}
