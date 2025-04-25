use std::collections::HashMap;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tiledb_common::array::CellValNum;
use tiledb_common::datatype::Datatype;
use tiledb_common::datatype::arrow::{
    DatatypeFromArrowResult, DatatypeToArrowResult,
};
use tiledb_common::physical_type_go;

use crate::array::schema::arrow::{
    AttributeFromArrowResult, FieldToArrowResult,
};
use crate::array::{Attribute, AttributeBuilder};
use crate::context::ContextBound;
use crate::error::Error;
use crate::filter::FilterListBuilder;
use crate::filter::arrow::FilterMetadata;
use crate::{Context, Result as TileDBResult};

// additional methods with arrow features
impl Attribute {
    pub fn to_arrow(&self) -> TileDBResult<FieldToArrowResult> {
        crate::array::attribute::arrow::to_arrow(self)
    }

    pub fn from_arrow(
        context: &Context,
        field: &arrow::datatypes::Field,
    ) -> TileDBResult<AttributeFromArrowResult> {
        crate::array::attribute::arrow::from_arrow(context, field)
    }
}

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
    pub enumeration: Option<String>,
}

impl AttributeMetadata {
    pub fn new(attr: &Attribute) -> TileDBResult<Self> {
        Ok(AttributeMetadata {
            fill_value: physical_type_go!(attr.datatype()?, DT, {
                let (fill_value, fill_nullable) =
                    attr.fill_value_nullable::<&[DT]>()?;
                FillValueMetadata {
                    data: json!(fill_value),
                    nullable: fill_nullable,
                }
            }),
            filters: FilterMetadata::new(&attr.filter_list()?)?,
            enumeration: attr.enumeration_name()?,
        })
    }

    /// Updates an AttributeBuilder with the contents of this object
    pub fn apply(
        &self,
        builder: AttributeBuilder,
    ) -> TileDBResult<AttributeBuilder> {
        let builder = if let Some(enumeration) = self.enumeration.as_ref() {
            builder.enumeration_name(enumeration)?
        } else {
            builder
        };
        let fl = self
            .filters
            .apply(FilterListBuilder::new(&builder.context())?)?
            .build();
        physical_type_go!(builder.datatype()?, DT, {
            let fill_value =
                serde_json::from_value::<Vec<DT>>(self.fill_value.data.clone())
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
pub fn to_arrow(attr: &Attribute) -> TileDBResult<FieldToArrowResult> {
    let construct = |adt| -> TileDBResult<arrow::datatypes::Field> {
        let name = attr.name()?;
        let metadata =
            serde_json::ser::to_string(&AttributeMetadata::new(attr)?)
                .map_err(|e| {
                    Error::Serialization(
                        format!("attribute {name} metadata"),
                        anyhow!(e),
                    )
                })?;
        Ok(arrow::datatypes::Field::new(name, adt, attr.is_nullable()?)
            .with_metadata(HashMap::<String, String>::from([(
                String::from("tiledb"),
                metadata,
            )])))
    };

    let arrow_dt = tiledb_common::datatype::arrow::to_arrow(
        &attr.datatype()?,
        attr.cell_val_num()?,
    );

    match arrow_dt {
        DatatypeToArrowResult::Exact(adt) => {
            Ok(FieldToArrowResult::Exact(construct(adt)?))
        }
        DatatypeToArrowResult::Inexact(adt) => {
            Ok(FieldToArrowResult::Inexact(construct(adt)?))
        }
    }
}

/// Tries to construct a TileDB array Attribute from the Arrow Field.
/// Details about the Attribute are stored under the key "tiledb"
/// in the Field's metadata, if it is present.
pub fn from_arrow(
    context: &Context,
    field: &arrow::datatypes::Field,
) -> TileDBResult<AttributeFromArrowResult> {
    let construct = |datatype: Datatype, cell_val_num: CellValNum| {
        let attr = if Datatype::Any == datatype && cell_val_num.is_var_sized() {
            /*
             * sc-46696: cannot call cell_val_num() with Any datatype,
             * not even with CellValNum::Var
             */
            AttributeBuilder::new(context, field.name(), datatype)?
                .nullability(field.is_nullable())?
        } else {
            AttributeBuilder::new(context, field.name(), datatype)?
                .nullability(field.is_nullable())?
                .cell_val_num(cell_val_num)?
        };

        if let Some(tiledb_metadata) = field.metadata().get("tiledb") {
            match serde_json::from_str::<AttributeMetadata>(
                tiledb_metadata.as_ref(),
            ) {
                Ok(attr_metadata) => Ok(attr_metadata.apply(attr)?),
                Err(e) => Err(Error::Deserialization(
                    format!("attribute {} metadata", field.name()),
                    anyhow!(e),
                )),
            }
        } else {
            Ok(attr)
        }
    };

    match tiledb_common::datatype::arrow::from_arrow(field.data_type()) {
        DatatypeFromArrowResult::None => Ok(AttributeFromArrowResult::None),
        DatatypeFromArrowResult::Inexact(datatype, cell_val_num) => {
            Ok(AttributeFromArrowResult::Inexact(construct(
                datatype,
                cell_val_num,
            )?))
        }
        DatatypeFromArrowResult::Exact(datatype, cell_val_num) => Ok(
            AttributeFromArrowResult::Exact(construct(datatype, cell_val_num)?),
        ),
    }
}

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy {
    use std::collections::HashMap;

    use proptest::prelude::*;

    pub fn prop_arrow_field() -> impl Strategy<Value = arrow::datatypes::Field>
    {
        (
            tiledb_pod::array::attribute::strategy::prop_attribute_name(),
            tiledb_common::datatype::arrow::strategy::any_datatype(
                Default::default(),
            ),
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
    use proptest::prelude::*;
    use tiledb_pod::array::attribute::AttributeData;

    use super::*;
    use crate::Factory;

    fn do_tiledb_arrow(tdb_spec: AttributeData) {
        let c: Context = Context::new().unwrap();
        let tdb_in = tdb_spec
            .create(&c)
            .expect("Error constructing arbitrary tiledb attribute");
        let arrow = to_arrow(&tdb_in).expect("Error reading tiledb attribute");
        let is_to_arrow_exact = arrow.is_exact();
        let arrow = arrow.ok().expect("No arrow field for tiledb attribute");

        // convert back to TileDB attribute
        let tdb_out = from_arrow(&c, &arrow).unwrap();
        let is_from_arrow_exact = tdb_out.is_exact();
        let tdb_out = tdb_out.ok().unwrap().build();

        if is_to_arrow_exact {
            assert!(is_from_arrow_exact, "{tdb_out:?}");
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

            let mut tdb_in = AttributeData::try_from(tdb_in).unwrap();
            tdb_in.datatype = Datatype::Any;

            let mut tdb_out = AttributeData::try_from(tdb_out).unwrap();
            tdb_out.datatype = Datatype::Any;

            assert_eq!(tdb_in, tdb_out);
        }
    }

    fn do_arrow_tiledb(arrow_in: arrow::datatypes::Field) {
        let c: Context = Context::new().unwrap();
        let tdb = from_arrow(&c, &arrow_in)
            .expect("Error constructing tiledb attribute");

        let is_from_arrow_exact = tdb.is_exact();

        let tdb = match tdb.ok() {
            None => return,
            Some(tdb) => tdb.build(),
        };

        let arrow_out = to_arrow(&tdb).unwrap();

        let is_to_arrow_exact = arrow_out.is_exact();

        let arrow_out = {
            let arrow_out = arrow_out.ok().unwrap();
            let mut metadata = arrow_out.metadata().clone();
            metadata.remove("tiledb");
            arrow_out.with_metadata(metadata)
        };

        if is_from_arrow_exact {
            assert!(is_to_arrow_exact, "{arrow_in:?} => {arrow_out:?}");

            /* this should be perfectly invertible */
            assert_eq!(arrow_in, arrow_out);
        } else {
            /* not perfectly invertible but we should get something close back */
            assert_eq!(arrow_in.name(), arrow_out.name());
            assert_eq!(arrow_in.is_nullable(), arrow_out.is_nullable());

            /* break out some datatypes */
            use tiledb_common::datatype::arrow::is_physical_type_match;
            assert!(
                is_physical_type_match(
                    arrow_in.data_type(),
                    arrow_out.data_type()
                ),
                "{:?} => {:?}",
                arrow_in.data_type(),
                arrow_out.data_type()
            );
        }
    }

    proptest! {
        #[test]
        fn test_tiledb_arrow(tdb_in in tiledb_pod::array::attribute::strategy::prop_attribute(Default::default())) {
            do_tiledb_arrow(tdb_in);
        }

        #[test]
        fn test_arrow_tiledb(arrow_in in strategy::prop_arrow_field()) {
            do_arrow_tiledb(arrow_in);
        }
    }
}
