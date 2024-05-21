use std::collections::HashMap;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::array::dimension::DimensionConstraints;
use crate::array::schema::arrow::{
    DimensionFromArrowResult, FieldToArrowResult,
};
use crate::array::{Dimension, DimensionBuilder};
use crate::context::{Context as TileDBContext, ContextBound};
use crate::datatype::arrow::{DatatypeFromArrowResult, DatatypeToArrowResult};
use crate::datatype::LogicalType;
use crate::filter::arrow::FilterMetadata;
use crate::filter::FilterListBuilder;
use crate::{error::Error as TileDBError, fn_typed, Result as TileDBResult};

/// Encapsulates fields of a TileDB dimension which are not part of an Arrow
/// field
#[derive(Deserialize, Serialize)]
pub struct DimensionMetadata {
    pub domain: Option<[serde_json::value::Value; 2]>,
    pub extent: Option<serde_json::value::Value>,
    pub filters: FilterMetadata,
}

impl DimensionMetadata {
    pub fn new(dim: &Dimension) -> TileDBResult<Self> {
        fn_typed!(dim.datatype()?, LT, {
            type DT = <LT as LogicalType>::PhysicalType;
            let domain = dim.domain::<DT>()?;
            let extent = dim.extent::<DT>()?;

            Ok(DimensionMetadata {
                domain: domain.map(|d| [json!(d[0]), json!(d[1])]),
                extent: extent.map(|e| json!(e)),
                filters: FilterMetadata::new(&dim.filters()?)?,
            })
        })
    }
}

/// Tries to construct an Arrow Field from the TileDB Dimension.
/// Details about the Dimension are stored under the key "tiledb"
/// in the Field's metadata.
pub fn to_arrow(dim: &Dimension) -> TileDBResult<FieldToArrowResult> {
    let arrow_dt =
        crate::datatype::arrow::to_arrow(&dim.datatype()?, dim.cell_val_num()?);

    let construct = |adt| -> TileDBResult<arrow::datatypes::Field> {
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
        Ok(
            arrow::datatypes::Field::new(name, adt, false).with_metadata(
                HashMap::<String, String>::from([(
                    String::from("tiledb"),
                    metadata,
                )]),
            ),
        )
    };

    match arrow_dt {
        DatatypeToArrowResult::Exact(adt) => {
            Ok(FieldToArrowResult::Exact(construct(adt)?))
        }
        DatatypeToArrowResult::Inexact(adt) => {
            Ok(FieldToArrowResult::Inexact(construct(adt)?))
        }
    }
}

pub fn from_arrow(
    context: &TileDBContext,
    field: &arrow::datatypes::Field,
) -> TileDBResult<DimensionFromArrowResult> {
    let construct = |datatype,
                     cell_val_num|
     -> TileDBResult<DimensionBuilder> {
        let metadata = match field.metadata().get("tiledb") {
            Some(metadata) => serde_json::from_str::<DimensionMetadata>(
                metadata,
            )
            .map_err(|e| {
                TileDBError::Deserialization(
                    format!("dimension {} metadata", field.name()),
                    anyhow!(e),
                )
            })?,
            None => Err(TileDBError::InvalidArgument(anyhow!(format!(
                "field {} missing metadata to construct dimension",
                field.name()
            ))))?,
        };

        let dim = fn_typed!(datatype, LT, {
            type DT = <LT as LogicalType>::PhysicalType;
            let deser = |v: &serde_json::value::Value| {
                serde_json::from_value::<DT>(v.clone()).map_err(|e| {
                    TileDBError::Deserialization(
                        format!("Dimension {} bound", field.name()),
                        anyhow!(e),
                    )
                })
            };

            let domain = if let Some(d) = metadata.domain {
                Some([deser(&d[0])?, deser(&d[1])?])
            } else {
                None
            };
            let extent = if let Some(e) = metadata.extent {
                Some(deser(&e)?)
            } else {
                None
            };

            if domain.is_none() && extent.is_some() {
                return Err(TileDBError::InvalidArgument(anyhow!(format!(
                    "Field {} contains invalid TileDB metadata",
                    field.name()
                ))));
            }

            match domain {
                Some(domain) => DimensionBuilder::new(
                    context,
                    field.name(),
                    datatype,
                    (domain, extent),
                ),
                None => DimensionBuilder::new(
                    context,
                    field.name(),
                    datatype,
                    DimensionConstraints::StringAscii,
                ),
            }
        })?;

        let fl = metadata
            .filters
            .apply(FilterListBuilder::new(&dim.context())?)?
            .build();

        dim.cell_val_num(cell_val_num)?.filters(fl)
    };

    match crate::datatype::arrow::from_arrow(field.data_type()) {
        DatatypeFromArrowResult::None => Ok(DimensionFromArrowResult::None),
        DatatypeFromArrowResult::Inexact(datatype, cell_val_num) => {
            Ok(DimensionFromArrowResult::Inexact(construct(
                datatype,
                cell_val_num,
            )?))
        }
        DatatypeFromArrowResult::Exact(datatype, cell_val_num) => Ok(
            DimensionFromArrowResult::Exact(construct(datatype, cell_val_num)?),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::dimension::DimensionData;
    use crate::{Datatype, Factory};
    use proptest::prelude::*;

    fn do_to_arrow(tdb_in: DimensionData) {
        let c: TileDBContext = TileDBContext::new().unwrap();

        let tdb_in = tdb_in
            .create(&c)
            .expect("Error constructing arbitrary tiledb dimension");

        let arrow_dimension =
            to_arrow(&tdb_in).expect("Error constructing arrow field");

        let is_to_arrow_exact = arrow_dimension.is_exact();

        let tdb_out = from_arrow(&c, &arrow_dimension.ok().unwrap()).unwrap();

        let is_from_arrow_exact = tdb_out.is_exact();
        let tdb_out = tdb_out.ok().unwrap().build();

        if is_to_arrow_exact {
            assert!(is_from_arrow_exact, "{:?}", tdb_out);
            assert_eq!(tdb_in, tdb_out);
        } else {
            /*
             * All should be the same but the datatype, which must be the same size.
             * NB: the conversion *back* might be (probably is) Exact,
             * which is a little misleading since we know the input was Inexact.
             */
            assert_ne!(tdb_in.datatype().unwrap(), tdb_out.datatype().unwrap());
            assert_eq!(
                tdb_in.datatype().unwrap().size(),
                tdb_out.datatype().unwrap().size()
            );

            let mut tdb_in = DimensionData::try_from(tdb_in).unwrap();
            tdb_in.datatype = Datatype::Any;

            let mut tdb_out = DimensionData::try_from(tdb_out).unwrap();
            tdb_out.datatype = Datatype::Any;

            assert_eq!(tdb_in, tdb_out);
        }
    }

    proptest! {
        #[test]
        fn test_to_arrow(tdb_in in any::<DimensionData>()) {
            do_to_arrow(tdb_in);
        }
    }
}
