use std::collections::HashMap;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::array::{Attribute, AttributeBuilder};
use crate::datatype::arrow::*;
use crate::datatype::LogicalType;
use crate::error::Error;
use crate::filter::arrow::FilterMetadata;
use crate::filter::FilterListBuilder;
use crate::{fn_typed, Context, Datatype, Result as TileDBResult};

/// Encapsulates TileDB Attribute fill value data for storage in Arrow field metadata
#[derive(Deserialize, Serialize)]
pub struct FillValueMetadata {
    pub data: serde_json::value::Value,
    pub nullable: bool,
}

/// Encapsulates details of a TileDB attribute for storage in Arrow field metadata
#[derive(Deserialize, Serialize)]
pub struct AttributeMetadata {
    pub fill_value: FillValueMetadata,
    pub filters: FilterMetadata,
}

impl AttributeMetadata {
    pub fn new(attr: &Attribute) -> TileDBResult<Self> {
        Ok(AttributeMetadata {
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
    pub fn apply<'ctx>(
        &self,
        builder: AttributeBuilder<'ctx>,
    ) -> TileDBResult<AttributeBuilder<'ctx>> {
        /* TODO: fill value */
        let fl = self
            .filters
            .apply(FilterListBuilder::new(builder.context())?)?
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
                .filter_list(&fl)?
                .fill_value_nullability(fill_value, self.fill_value.nullable)
        })
    }
}

/// Tries to construct an Arrow Field from the TileDB Attribute.
/// Details about the Attribute are stored under the key "tiledb"
/// in the Field's metadata.
pub fn arrow_field(attr: &Attribute) -> TileDBResult<arrow::datatypes::Field> {
    let arrow_dt = arrow_type_physical(&attr.datatype()?, attr.cell_val_num()?);

    let name = attr.name()?;
    let metadata = serde_json::ser::to_string(&AttributeMetadata::new(attr)?)
        .map_err(|e| {
        Error::Serialization(format!("attribute {} metadata", name), anyhow!(e))
    })?;
    Ok(
        arrow::datatypes::Field::new(name, arrow_dt, attr.is_nullable()?)
            .with_metadata(HashMap::<String, String>::from([(
                String::from("tiledb"),
                metadata,
            )])),
    )
}

/// Tries to construct a TileDB array Attribute from the Arrow Field.
/// Details about the Attribute are stored under the key "tiledb"
/// in the Field's metadata, if it is present.
pub fn tiledb_attribute<'ctx>(
    context: &'ctx Context,
    field: &arrow::datatypes::Field,
) -> TileDBResult<Option<AttributeBuilder<'ctx>>> {
    if let Some((tiledb_dt, cell_val_num)) =
        tiledb_type_physical(field.data_type())
    {
        let attr = if let Datatype::Any = tiledb_dt {
            let a = AttributeBuilder::new(context, field.name(), tiledb_dt)?
                .nullability(field.is_nullable())?;
            a
        } else {
            AttributeBuilder::new(context, field.name(), tiledb_dt)?
                .nullability(field.is_nullable())?
                .cell_val_num(cell_val_num)?
        };

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
    use std::collections::HashMap;

    use proptest::prelude::*;

    pub fn prop_arrow_field() -> impl Strategy<Value = arrow::datatypes::Field>
    {
        (
            crate::array::attribute::strategy::prop_attribute_name(),
            crate::datatype::arrow::strategy::any_datatype(Default::default()),
            any::<bool>(),
            Just(HashMap::<String, String>::new()), /* TODO: we'd like to check that metadata is preserved,
                                                     * but right now the CAPI doesn't appear to have a way
                                                     * to attach metadata to an attribute
                                                     */
        )
            .prop_map(|(name, data_type, nullable, metadata)| {
                arrow::datatypes::Field::new(name, data_type, nullable)
                    .with_metadata(metadata)
            })
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::array::attribute::AttributeData;
    use crate::Factory;
    use proptest::prelude::*;

    #[test]
    fn test_tiledb_arrow() {
        let c: Context = Context::new().unwrap();

        let do_tiledb_arrow = |tdb_in: AttributeData| {
            let tdb_in = tdb_in
                .create(&c)
                .expect("Error constructing arbitrary tiledb attribute");
            let arrow_field =
                arrow_field(&tdb_in).expect("Error reading tiledb attribute");
            // convert back to TileDB attribute
            let tdb_out = tiledb_attribute(&c, &arrow_field)
                .expect("Arrow attribute did not invert")
                .expect("Arrow attribute did not invert")
                .build();
            assert_eq!(tdb_in, tdb_out);
        };

        /* tiledb => arrow => tiledb */
        proptest!(|(tdb_in in crate::array::attribute::strategy::prop_attribute(Default::default()))| {
            do_tiledb_arrow(tdb_in);
        });
    }

    #[test]
    fn test_arrow_tiledb() -> TileDBResult<()> {
        let c: Context = Context::new()?;

        let do_arrow_tiledb = |arrow_in| {
            let maybe_tdb = tiledb_attribute(&c, &arrow_in)
                .expect("Error constructing tiledb attribute");
            if let Some(tdb) = maybe_tdb {
                let tdb = tdb.build();
                let arrow_out = {
                    let arrow_out = arrow_field(&tdb)
                        .expect("Error reconstructing arrow field");
                    let metadata = {
                        let mut metadata = arrow_out.metadata().clone();
                        metadata.remove("tiledb");
                        metadata
                    };
                    arrow_out.with_metadata(metadata)
                };
                /* some datatypes are not invertible so we have to break those out */
                assert_eq!(arrow_in.name(), arrow_out.name());
                assert_eq!(arrow_in.is_nullable(), arrow_out.is_nullable());

                use arrow::datatypes::DataType as ADT;
                match (arrow_in.data_type(), arrow_out.data_type()) {
                    (ADT::Utf8, ADT::Utf8) => (),
                    (
                        ADT::FixedSizeList(ref item_in, len_in),
                        ADT::FixedSizeList(ref item_out, len_out),
                    ) => {
                        assert_eq!(len_in, len_out);
                        assert_eq!(
                            item_in.data_type().primitive_width(),
                            item_out.data_type().primitive_width()
                        );
                    }
                    (ADT::Binary, ADT::Binary) => (),
                    (
                        ADT::FixedSizeBinary(len_in),
                        ADT::FixedSizeBinary(len_out),
                    ) => {
                        assert_eq!(len_in, len_out)
                    }
                    (ADT::List(ref item_in), ADT::List(ref item_out)) => {
                        assert!(item_in.data_type().is_primitive());
                        assert_eq!(
                            item_in.data_type().primitive_width(),
                            item_out.data_type().primitive_width()
                        );
                    }
                    (ADT::FixedSizeList(ref item_in, 1), dt_out) => {
                        /*
                         * fixed size list of 1 element should have no extra data,
                         * we probably don't need to keep the FixedSizeList part
                         * for correctness, punt on it for now and see if we need
                         * to deal with it later
                         */
                        assert_eq!(
                            item_in.data_type().primitive_width(),
                            dt_out.primitive_width()
                        );
                    }
                    (dt_in, dt_out) => {
                        if dt_in.is_primitive() {
                            assert_eq!(
                                dt_in.primitive_width(),
                                dt_out.primitive_width()
                            );
                        } else {
                            unreachable!(
                                "Unexpected type: {} in, {} out",
                                dt_in, dt_out
                            )
                        }
                    }
                }
            } else {
                /* this should be some unsupported type, not that important to check */
            }
        };

        /* arrow => tiledb => arrow */
        proptest!(|(arrow_in in strategy::prop_arrow_field())| {
            do_arrow_tiledb(arrow_in);
        });

        Ok(())
    }
}
