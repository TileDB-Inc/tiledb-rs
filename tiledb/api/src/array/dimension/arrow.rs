use std::collections::HashMap;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::array::{Dimension, DimensionBuilder};
use crate::context::Context as TileDBContext;
use crate::datatype::arrow::{arrow_type_physical, tiledb_type_physical};
use crate::datatype::LogicalType;
use crate::filter::arrow::FilterMetadata;
use crate::filter::FilterListBuilder;
use crate::{error::Error as TileDBError, fn_typed, Result as TileDBResult};

/// Encapsulates fields of a TileDB dimension which are not part of an Arrow
/// field
#[derive(Deserialize, Serialize)]
pub struct DimensionMetadata {
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
pub fn arrow_field(dim: &Dimension) -> TileDBResult<arrow::datatypes::Field> {
    let arrow_dt = arrow_type_physical(&dim.datatype()?, dim.cell_val_num()?);
    let name = dim.name()?;
    let metadata = serde_json::ser::to_string(&DimensionMetadata::new(dim)?)
        .map_err(|e| {
            TileDBError::Serialization(
                format!("dimension {} metadata", name),
                anyhow!(e),
            )
        })?;
    Ok(
        arrow::datatypes::Field::new(name, arrow_dt, false).with_metadata(
            HashMap::<String, String>::from([(
                String::from("tiledb"),
                metadata,
            )]),
        ),
    )
}

pub fn tiledb_dimension<'ctx>(
    context: &'ctx TileDBContext,
    field: &arrow::datatypes::Field,
) -> TileDBResult<Option<DimensionBuilder<'ctx>>> {
    let (tiledb_datatype, cell_val_num) =
        match tiledb_type_physical(field.data_type()) {
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
        .apply(FilterListBuilder::new(dim.context())?)?
        .build();

    Ok(Some(dim.cell_val_num(cell_val_num)?.filters(fl)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::dimension::DimensionData;
    use crate::Factory;
    use proptest::prelude::*;

    #[test]
    fn test_tiledb_arrow_tiledb() {
        let c: TileDBContext = TileDBContext::new().unwrap();

        let do_test_tiledb_arrow = |tdb_in: DimensionData| {
            let tdb_in = tdb_in
                .create(&c)
                .expect("Error constructing arbitrary tiledb dimension");
            let arrow_dimension =
                arrow_field(&tdb_in).expect("Error constructing arrow field");
            let tdb_out = tiledb_dimension(&c, &arrow_dimension)
                .expect("Error converting back to tiledb dimension")
                .unwrap()
                .build();
            assert_eq!(tdb_in, tdb_out);
        };

        proptest!(|(tdb_in in crate::array::dimension::strategy::prop_dimension())| {
            do_test_tiledb_arrow(tdb_in);
        });
    }
}
