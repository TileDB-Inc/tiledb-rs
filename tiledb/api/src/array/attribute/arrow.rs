use std::collections::HashMap;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::array::{Attribute, AttributeBuilder, CellValNum};
use crate::context::ContextBound;
use crate::datatype::arrow::*;
use crate::datatype::LogicalType;
use crate::error::Error;
use crate::filter::arrow::FilterMetadata;
use crate::filter::FilterListBuilder;
use crate::{fn_typed, Context, Result as TileDBResult};

/// Encapsulates TileDB Attribute fill value data for storage in Arrow field metadata
#[derive(Deserialize, Serialize)]
pub struct FillValueMetadata {
    pub data: serde_json::value::Value,
    pub nullable: bool,
}

/// Encapsulates details of a TileDB attribute for storage in Arrow field metadata
#[derive(Deserialize, Serialize)]
pub struct AttributeMetadata {
    pub cell_val_num: CellValNum,
    pub fill_value: FillValueMetadata,
    pub filters: FilterMetadata,
}

impl AttributeMetadata {
    pub fn new(attr: &Attribute) -> TileDBResult<Self> {
        Ok(AttributeMetadata {
            cell_val_num: attr.cell_val_num()?,
            fill_value: fn_typed!(attr.datatype()?, LT, {
                type DT = <LT as LogicalType>::PhysicalType;
                let (fill_value, fill_nullable) =
                    attr.fill_value_nullable::<DT>()?;
                FillValueMetadata {
                    data: json!(fill_value),
                    nullable: fill_nullable,
                }
            }),
            filters: FilterMetadata::new(&attr.filter_list()?)?,
        })
    }

    /// Updates an AttributeBuilder with the contents of this object
    pub fn apply(
        &self,
        builder: AttributeBuilder,
    ) -> TileDBResult<AttributeBuilder> {
        /* TODO: fill value */
        let fl = self
            .filters
            .apply(FilterListBuilder::new(&builder.context())?)?
            .build();
        fn_typed!(builder.datatype()?, LT, {
            type DT = <LT as LogicalType>::PhysicalType;
            let fill_value =
                serde_json::from_value::<DT>(self.fill_value.data.clone())
                    .map_err(|e| {
                        Error::Deserialization(
                            String::from("attribute fill value"),
                            anyhow!(e),
                        )
                    })?;
            builder
                .cell_val_num(self.cell_val_num)?
                .filter_list(&fl)?
                .fill_value_nullability(fill_value, self.fill_value.nullable)
        })
    }
}

/// Tries to construct an Arrow Field from the TileDB Attribute.
/// Details about the Attribute are stored under the key "tiledb"
/// in the Field's metadata.
pub fn arrow_field(
    attr: &Attribute,
) -> TileDBResult<Option<arrow_schema::Field>> {
    if let Some(arrow_dt) = arrow_type_physical(&attr.datatype()?) {
        let name = attr.name()?;
        let metadata =
            serde_json::ser::to_string(&AttributeMetadata::new(attr)?)
                .map_err(|e| {
                    Error::Serialization(
                        format!("attribute {} metadata", name),
                        anyhow!(e),
                    )
                })?;
        Ok(Some(
            arrow_schema::Field::new(name, arrow_dt, attr.is_nullable()?)
                .with_metadata(HashMap::<String, String>::from([(
                    String::from("tiledb"),
                    metadata,
                )])),
        ))
    } else {
        Ok(None)
    }
}

/// Tries to construct a TileDB array Attribute from the Arrow Field.
/// Details about the Attribute are stored under the key "tiledb"
/// in the Field's metadata, if it is present.
pub fn tiledb_attribute(
    context: &Context,
    field: &arrow_schema::Field,
) -> TileDBResult<Option<AttributeBuilder>> {
    if let Some(tiledb_dt) = tiledb_type_physical(field.data_type()) {
        let attr = AttributeBuilder::new(context, field.name(), tiledb_dt)?
            .nullability(field.is_nullable())?;

        if let Some(tiledb_metadata) = field.metadata().get("tiledb") {
            match serde_json::from_str::<AttributeMetadata>(
                tiledb_metadata.as_ref(),
            ) {
                Ok(attr_metadata) => Ok(Some(attr_metadata.apply(attr)?)),
                Err(e) => Err(Error::Deserialization(
                    format!("attribute {} metadata", field.name()),
                    anyhow!(e),
                )),
            }
        } else {
            Ok(Some(attr))
        }
    } else {
        Ok(None)
    }
}

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy {
    use proptest::prelude::*;

    pub fn prop_arrow_field() -> impl Strategy<Value = arrow_schema::Field> {
        (
            crate::array::attribute::strategy::prop_attribute_name(),
            crate::datatype::arrow::strategy::prop_arrow_implemented(),
            proptest::prelude::any::<bool>(),
        )
            .prop_map(|(name, data_type, nullable)| {
                arrow_schema::Field::new(name, data_type, nullable)
            })

        /*
         * TODO: generate arbitrary metadata?
         * Without doing so the test does not demonstrate that metadata is
         * preserved. Which the CAPI doesn't appear to offer a way to do anyway.
         */
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::Factory;
    use proptest::prelude::*;

    #[test]
    fn test_tiledb_arrow_tiledb() -> TileDBResult<()> {
        let c: Context = Context::new()?;

        /* tiledb => arrow => tiledb */
        proptest!(|(tdb_in in crate::array::attribute::strategy::prop_attribute(Default::default()))| {
            let tdb_in = tdb_in.create(&c)
                .expect("Error constructing arbitrary tiledb attribute");
            if let Some(arrow_field) = arrow_field(&tdb_in)
                .expect("Error reading tiledb attribute") {
                // convert back to TileDB attribute
                let tdb_out = tiledb_attribute(&c, &arrow_field)?
                    .expect("Arrow attribute did not invert").build();
                assert_eq!(tdb_in, tdb_out);
            }
        });

        Ok(())
    }

    #[test]
    fn test_arrow_tiledb_arrow() -> TileDBResult<()> {
        let c: Context = Context::new()?;
        /* arrow => tiledb => arrow */
        proptest!(|(arrow_in in strategy::prop_arrow_field())| {
            let tdb = tiledb_attribute(&c, &arrow_in);
            assert!(tdb.is_ok());
            let tdb = tdb.unwrap();
            assert!(tdb.is_some());
            let tdb = tdb.unwrap().build();
            let arrow_out = arrow_field(&tdb);
            assert!(arrow_out.is_ok());
            let arrow_out = arrow_out.unwrap();
            assert!(arrow_out.is_some());
            let arrow_out = {
                let arrow_out = arrow_out.unwrap();
                let metadata = {
                    let mut metadata = arrow_out.metadata().clone();
                    metadata.remove("tiledb");
                    metadata
                };
                arrow_out.with_metadata(metadata)
            };
            assert_eq!(arrow_in, arrow_out);
        });

        Ok(())
    }
}
