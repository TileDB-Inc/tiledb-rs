use std::collections::HashMap;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::array::{CellValNum, Dimension, DimensionBuilder};
use crate::context::{Context as TileDBContext, ContextBound};
use crate::datatype::arrow::{arrow_type_physical, tiledb_type_physical};
use crate::datatype::LogicalType;
use crate::filter::arrow::FilterMetadata;
use crate::filter::FilterListBuilder;
use crate::{error::Error as TileDBError, fn_typed, Result as TileDBResult};

/// Encapsulates fields of a TileDB dimension which are not part of an Arrow
/// field
#[derive(Deserialize, Serialize)]
pub struct DimensionMetadata {
    pub cell_val_num: CellValNum,
    pub domain: [serde_json::value::Value; 2],
    pub extent: serde_json::value::Value,
    pub filters: FilterMetadata,
}

impl DimensionMetadata {
    pub fn new(dim: &Dimension) -> TileDBResult<Self> {
        fn_typed!(dim.datatype()?, LT, {
            type DT = <LT as LogicalType>::PhysicalType;
            let domain = dim.domain::<DT>()?;
            let extent = dim.extent::<DT>()?;

            Ok(DimensionMetadata {
                cell_val_num: dim.cell_val_num()?,
                domain: [json!(domain[0]), json!(domain[1])],
                extent: json!(extent),
                filters: FilterMetadata::new(&dim.filters()?)?,
            })
        })
    }
}

/// Tries to construct an Arrow Field from the TileDB Dimension.
/// Details about the Dimension are stored under the key "tiledb"
/// in the Field's metadata.
pub fn arrow_field(
    dim: &Dimension,
) -> TileDBResult<Option<arrow_schema::Field>> {
    if let Some(arrow_dt) = arrow_type_physical(&dim.datatype()?) {
        let name = dim.name()?;
        let metadata = serde_json::ser::to_string(&DimensionMetadata::new(
            dim,
        )?)
        .map_err(|e| {
            TileDBError::Serialization(
                format!("dimension {} metadata", name),
                anyhow!(e),
            )
        })?;
        Ok(Some(
            arrow_schema::Field::new(name, arrow_dt, false).with_metadata(
                HashMap::<String, String>::from([(
                    String::from("tiledb"),
                    metadata,
                )]),
            ),
        ))
    } else {
        Ok(None)
    }
}

pub fn tiledb_dimension(
    context: &TileDBContext,
    field: &arrow_schema::Field,
) -> TileDBResult<Option<DimensionBuilder>> {
    let tiledb_datatype = match tiledb_type_physical(field.data_type()) {
        Some(dt) => dt,
        None => return Ok(None),
    };
    let metadata = match field.metadata().get("tiledb") {
        Some(metadata) => serde_json::from_str::<DimensionMetadata>(metadata)
            .map_err(|e| {
            TileDBError::Deserialization(
                format!("dimension {} metadata", field.name()),
                anyhow!(e),
            )
        })?,
        None => return Ok(None),
    };

    let dim = fn_typed!(tiledb_datatype, LT, {
        type DT = <LT as LogicalType>::PhysicalType;
        let deser = |v: &serde_json::value::Value| {
            serde_json::from_value::<DT>(v.clone()).map_err(|e| {
                TileDBError::Deserialization(
                    format!("dimension {} lower bound", field.name()),
                    anyhow!(e),
                )
            })
        };

        let domain = [deser(&metadata.domain[0])?, deser(&metadata.domain[1])?];
        let extent = deser(&metadata.extent)?;

        DimensionBuilder::new::<DT>(
            context,
            field.name(),
            tiledb_datatype,
            &domain,
            &extent,
        )
    })?;

    let fl = metadata
        .filters
        .apply(FilterListBuilder::new(&dim.context())?)?
        .build();

    Ok(Some(dim.cell_val_num(metadata.cell_val_num)?.filters(fl)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Factory;
    use proptest::prelude::*;

    #[test]
    fn test_tiledb_arrow_tiledb() {
        let c: TileDBContext = TileDBContext::new().unwrap();

        proptest!(|(tdb_in in crate::array::dimension::strategy::prop_dimension())| {
            let tdb_in = tdb_in.create(&c)
                .expect("Error constructing arbitrary tiledb dimension");
            if let Some(arrow_dimension) = arrow_field(&tdb_in)
                    .expect("Error constructing arrow field") {
                let tdb_out = tiledb_dimension(&c, &arrow_dimension)
                    .expect("Error converting back to tiledb dimension")
                    .unwrap()
                    .build();
                assert_eq!(tdb_in, tdb_out);
            }
        });
    }
}
