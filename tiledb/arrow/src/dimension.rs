use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::json;
use tiledb::context::Context as TileDBContext;
use tiledb::filter::FilterListBuilder;
use tiledb::{error::Error as TileDBError, fn_typed, Result as TileDBResult};

use crate::datatype::{arrow_type_physical, tiledb_type_physical};
use crate::filter::FilterMetadata;

/// Encapsulates fields of a TileDB dimension which are not part of an Arrow field
#[derive(Deserialize, Serialize)]
pub struct DimensionMetadata {
    pub cell_val_num: u32,
    pub domain: [serde_json::value::Value; 2],
    pub extent: serde_json::value::Value,
    pub filters: FilterMetadata,
}

impl DimensionMetadata {
    pub fn new(dim: &tiledb::array::Dimension) -> TileDBResult<Self> {
        fn_typed!(dim.datatype(), DT, {
            let domain = dim.domain::<DT>()?;
            let extent = dim.extent::<DT>()?;

            Ok(DimensionMetadata {
                cell_val_num: dim.cell_val_num()?,
                domain: [json!(domain[0]), json!(domain[1])],
                extent: json!(extent),
                filters: FilterMetadata::new(&dim.filters())?,
            })
        })
    }
}

/// Tries to construct an Arrow Field from the TileDB Dimension.
/// Details about the Dimension are stored under the key "tiledb"
/// in the Field's metadata.
pub fn arrow_field(
    dim: &tiledb::array::Dimension,
) -> TileDBResult<Option<arrow_schema::Field>> {
    if let Some(arrow_dt) = arrow_type_physical(&dim.datatype()) {
        let name = dim.name()?;
        let metadata = serde_json::ser::to_string(&DimensionMetadata::new(
            dim,
        )?)
        .map_err(|e| {
            TileDBError::from(format!(
                "Error serializing metadata for dimension {}: {}",
                name, e
            ))
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

pub fn tiledb_dimension<'ctx>(
    context: &'ctx TileDBContext,
    field: &arrow_schema::Field,
) -> TileDBResult<Option<tiledb::array::DimensionBuilder<'ctx>>> {
    let tiledb_datatype = match tiledb_type_physical(field.data_type()) {
        Some(dt) => dt,
        None => return Ok(None),
    };
    let metadata = match field.metadata().get("tiledb") {
        Some(metadata) => serde_json::from_str::<DimensionMetadata>(metadata)
            .map_err(|e| {
            TileDBError::from(format!(
                "Error deserializing metadata for dimension {}: {}",
                field.name(),
                e
            ))
        })?,
        None => return Ok(None),
    };

    let dim = fn_typed!(tiledb_datatype, DT, {
        let deser = |v: &serde_json::value::Value| {
            serde_json::from_value::<DT>(v.clone()).map_err(|e| {
                TileDBError::from(format!(
                    "Error deserializing dimension domain: {}",
                    e
                ))
            })
        };

        let domain = [deser(&metadata.domain[0])?, deser(&metadata.domain[1])?];
        let extent = deser(&metadata.extent)?;

        tiledb::array::DimensionBuilder::new::<DT>(
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

    Ok(Some(dim.cell_val_num(metadata.cell_val_num)?.filters(fl)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tiledb::Factory;

    #[test]
    fn test_tiledb_arrow_tiledb() {
        let c: TileDBContext = TileDBContext::new().unwrap();

        proptest!(|(tdb_in in tiledb_test::dimension::arbitrary())| {
            let tdb_in = tdb_in.create(&c).expect("Error constructing arbitrary tiledb dimension");
            if let Some(arrow_dimension) = arrow_field(&tdb_in).expect("Error constructing arrow field") {
                let tdb_out = tiledb_dimension(&c, &arrow_dimension).expect("Error converting back to tiledb dimension").unwrap().build();
                assert_eq!(tdb_in, tdb_out);
            }
        });
    }
}
