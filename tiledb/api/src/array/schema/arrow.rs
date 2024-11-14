use std::sync::Arc;

use anyhow::anyhow;
use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
use serde::{Deserialize, Serialize};
use tiledb_pod::array::EnumerationData;

use crate::array::{
    ArrayType, AttributeBuilder, CellOrder, DimensionBuilder, DomainBuilder,
    Schema, SchemaBuilder, TileOrder,
};
use crate::filter::arrow::FilterMetadata;
use crate::{error::Error, Context, Factory, Result as TileDBResult};

pub type FieldToArrowResult = crate::arrow::ArrowConversionResult<
    arrow::datatypes::Field,
    arrow::datatypes::Field,
>;

pub type FieldFromArrowResult<F> = crate::arrow::ArrowConversionResult<F, F>;

pub type AttributeFromArrowResult = FieldFromArrowResult<AttributeBuilder>;
pub type DimensionFromArrowResult = FieldFromArrowResult<DimensionBuilder>;

pub type SchemaToArrowResult =
    crate::arrow::ArrowConversionResult<ArrowSchema, ArrowSchema>;

pub type SchemaFromArrowResult =
    crate::arrow::ArrowConversionResult<SchemaBuilder, SchemaBuilder>;

// additional methods with arrow features
impl Schema {
    pub fn to_arrow(&self) -> TileDBResult<SchemaToArrowResult> {
        crate::array::schema::arrow::to_arrow(self)
    }

    pub fn from_arrow(
        context: &Context,
        schema: &arrow::datatypes::Schema,
    ) -> TileDBResult<SchemaFromArrowResult> {
        crate::array::schema::arrow::from_arrow(context, schema)
    }
}

/// Represents required metadata to convert from an arrow schema
/// to a TileDB schema.
#[derive(Deserialize, Serialize)]
pub struct SchemaMetadata {
    array_type: ArrayType,
    capacity: u64,
    allows_duplicates: bool,
    cell_order: CellOrder,
    tile_order: TileOrder,
    enumerations: Vec<EnumerationData>,
    coordinate_filters: FilterMetadata,
    offsets_filters: FilterMetadata,
    nullity_filters: FilterMetadata,

    /// Number of dimensions in this schema. The first `ndim` Fields are
    /// Dimensions, not Attributes
    ndim: usize,
}

impl SchemaMetadata {
    pub fn new(schema: &Schema) -> TileDBResult<Self> {
        Ok(SchemaMetadata {
            array_type: schema.array_type()?,
            capacity: schema.capacity()?,
            allows_duplicates: schema.allows_duplicates()?,
            cell_order: schema.cell_order()?,
            tile_order: schema.tile_order()?,
            enumerations: schema
                .enumerations()?
                .map(|e| e.and_then(|e| EnumerationData::try_from(&e)))
                .collect::<TileDBResult<Vec<EnumerationData>>>()?,
            coordinate_filters: FilterMetadata::new(
                &schema.coordinate_filters()?,
            )?,
            offsets_filters: FilterMetadata::new(&schema.offsets_filters()?)?,
            nullity_filters: FilterMetadata::new(&schema.nullity_filters()?)?,
            ndim: schema.domain()?.num_dimensions()?,
        })
    }
}

pub fn to_arrow(tiledb: &Schema) -> TileDBResult<SchemaToArrowResult> {
    let mut builder = arrow::datatypes::SchemaBuilder::with_capacity(
        tiledb.num_attributes()?,
    );

    let mut inexact = false;

    for d in 0..tiledb.domain()?.num_dimensions()? {
        let dim = tiledb.domain()?.dimension(d)?;
        match crate::array::dimension::arrow::to_arrow(&dim)? {
            FieldToArrowResult::None => {
                /*
                 * Missing a dimension is a problem, but it's mostly
                 * a problem for if we try to invert back to tiledb.
                 * See `from_arrow`.
                 */
                inexact = true;
            }
            FieldToArrowResult::Inexact(field) => {
                inexact = true;
                builder.push(field);
            }
            FieldToArrowResult::Exact(field) => {
                builder.push(field);
            }
        };
    }

    for a in 0..tiledb.num_attributes()? {
        let attr = tiledb.attribute(a)?;
        match crate::array::attribute::arrow::to_arrow(&attr)? {
            FieldToArrowResult::None => {
                /*
                 * No way to represent this arrow field in tiledb.
                 * TODO: some kind of inexactness details
                 */
                inexact = true;
            }
            FieldToArrowResult::Inexact(field) => {
                inexact = true;
                builder.push(field);
            }
            FieldToArrowResult::Exact(field) => {
                builder.push(field);
            }
        };
    }

    let metadata = serde_json::ser::to_string(&SchemaMetadata::new(tiledb)?)
        .map_err(|e| {
            Error::Serialization(String::from("schema metadata"), anyhow!(e))
        })?;
    builder
        .metadata_mut()
        .insert(String::from("tiledb"), metadata);

    Ok(if inexact {
        SchemaToArrowResult::Inexact(builder.finish())
    } else {
        SchemaToArrowResult::Exact(builder.finish())
    })
}

fn tiledb_metadata(schema: &ArrowSchema) -> TileDBResult<SchemaMetadata> {
    let Some(metadata) = schema.metadata().get("tiledb") else {
        return Err(Error::Other(format!(
            "Schema does not have tiledb metadata field '{}'",
            "tiledb"
        )));
    };
    serde_json::from_str::<SchemaMetadata>(metadata).map_err(|e| {
        Error::Deserialization(String::from("schema metadata"), anyhow!(e))
    })
}

/// Construct a TileDB schema from an Arrow schema.
///
/// A TileDB schema must have domain and dimension details.
/// These are expected to be in the schema `metadata` beneath the key `tiledb`.
/// This metadata is expected to be a JSON object with the following fields:
pub fn from_arrow(
    context: &Context,
    schema: &ArrowSchema,
) -> TileDBResult<SchemaFromArrowResult> {
    let metadata = if schema.metadata.contains_key("tiledb") {
        tiledb_metadata(schema)?
    } else {
        return Ok(SchemaFromArrowResult::None);
    };

    if schema.fields.len() < metadata.ndim {
        return Err(Error::InvalidArgument(anyhow!(format!(
            "Expected at least {} dimension fields but only found {}",
            metadata.ndim,
            schema.fields.len()
        ))));
    }

    let dimensions = schema.fields.iter().take(metadata.ndim);
    let attributes = schema.fields.iter().skip(metadata.ndim);

    let mut inexact: bool = false;

    let domain = {
        let mut b = DomainBuilder::new(context)?;
        for f in dimensions {
            match crate::array::dimension::arrow::from_arrow(context, f)? {
                DimensionFromArrowResult::None => {
                    /*
                     * In contrast to attributes (see below) this
                     * probably represents a significant problem
                     * because it completely changes the way arrays using
                     * this schema are interacted with
                     */
                    return Ok(SchemaFromArrowResult::None);
                }
                DimensionFromArrowResult::Inexact(dimension) => {
                    inexact = true;
                    b = b.add_dimension(dimension.build())?;
                }
                DimensionFromArrowResult::Exact(dimension) => {
                    b = b.add_dimension(dimension.build())?;
                }
            }
        }
        b.build()
    };

    let mut b = SchemaBuilder::new(context, metadata.array_type, domain)?
        .capacity(metadata.capacity)?
        .allow_duplicates(metadata.allows_duplicates)?
        .cell_order(metadata.cell_order)?
        .tile_order(metadata.tile_order)?
        .coordinate_filters(&metadata.coordinate_filters.create(context)?)?
        .offsets_filters(&metadata.offsets_filters.create(context)?)?
        .nullity_filters(&metadata.nullity_filters.create(context)?)?;

    b = metadata
        .enumerations
        .iter()
        .try_fold(b, |b, enmr| b.add_enumeration(enmr.create(context)?))?;

    for f in attributes {
        match crate::array::attribute::arrow::from_arrow(context, f)? {
            AttributeFromArrowResult::None => {
                /*
                 * No way to represent this arrow field in tiledb.
                 * TODO: some kind of inexactness details
                 */
                inexact = true;
            }
            AttributeFromArrowResult::Inexact(attr) => {
                inexact = true;
                b = b.add_attribute(attr.build())?;
            }
            AttributeFromArrowResult::Exact(attr) => {
                b = b.add_attribute(attr.build())?;
            }
        }
    }

    Ok(if inexact {
        SchemaFromArrowResult::Inexact(b)
    } else {
        SchemaFromArrowResult::Exact(b)
    })
}

/// Returns an `Iterator` over the fields of a schema which represent
/// tiledb array dimensions.
pub fn dimensions(schema: &ArrowSchema) -> TileDBResult<&[Arc<ArrowField>]> {
    let metadata = tiledb_metadata(schema)?;
    Ok(schema.fields.split_at(metadata.ndim).0)
}

pub fn attributes(schema: &ArrowSchema) -> TileDBResult<&[Arc<ArrowField>]> {
    let metadata = tiledb_metadata(schema)?;
    Ok(schema.fields.split_at(metadata.ndim).1)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use proptest::prelude::*;
    use tiledb_pod::array::attribute::AttributeData;
    use tiledb_pod::array::dimension::DimensionData;
    use tiledb_pod::array::schema::SchemaData;

    use super::*;
    use crate::array::schema::Field as SchemaField;
    use crate::Factory;

    fn do_to_arrow(tdb_in: SchemaData) {
        let c: Context = Context::new().unwrap();

        let tdb_in = tdb_in
            .create(&c)
            .expect("Error constructing arbitrary tiledb attribute");

        let arrow_schema = to_arrow(&tdb_in).unwrap();
        match arrow_schema {
            SchemaToArrowResult::None => unreachable!(),
            SchemaToArrowResult::Exact(arrow_schema) => {
                /* this should invert entirely */
                let tdb_out = from_arrow(&c, &arrow_schema).unwrap();
                if let SchemaFromArrowResult::Exact(tdb_out) = tdb_out {
                    let tdb_out = tdb_out.build().unwrap();
                    assert_eq!(tdb_in, tdb_out);
                } else {
                    unreachable!("Exact schema did not invert")
                }
            }
            SchemaToArrowResult::Inexact(arrow_schema) => {
                let tdb_out = from_arrow(&c, &arrow_schema).unwrap();
                let tdb_out = tdb_out.ok().unwrap().build().unwrap();

                /*
                 * All datatypes map *to* arrow, so it should be the same number of fields.
                 * At least one datatype must be inexact, otherwise we should have an exact match
                 * (If we started with arrow rather than tiledb then we would also need to check
                 * for missing fields)
                 */
                let mut inexact_field = false;

                let fields_in = tdb_in.fields().unwrap();
                let fields_out = tdb_out.fields().unwrap();
                assert_eq!(fields_in.num_fields(), fields_out.num_fields());

                for (field_in, field_out) in fields_in.zip(fields_out) {
                    match (field_in.unwrap(), field_out.unwrap()) {
                        (
                            SchemaField::Attribute(attr_in),
                            SchemaField::Attribute(attr_out),
                        ) => {
                            if attr_in == attr_out {
                                continue;
                            }
                            inexact_field = true;

                            let attr_out =
                                AttributeData::try_from(attr_out).unwrap();
                            let attr_in = {
                                let mut a =
                                    AttributeData::try_from(attr_in).unwrap();
                                assert!(
                                    attr_out.datatype.size()
                                        == a.datatype.size()
                                );
                                a.datatype = attr_out.datatype;
                                a
                            };
                            assert_eq!(attr_in, attr_out)
                        }
                        (
                            SchemaField::Dimension(dim_in),
                            SchemaField::Dimension(dim_out),
                        ) => {
                            if dim_in == dim_out {
                                continue;
                            }
                            inexact_field = true;

                            let dim_out =
                                DimensionData::try_from(dim_out).unwrap();
                            let dim_in = {
                                let mut a =
                                    DimensionData::try_from(dim_in).unwrap();
                                assert!(
                                    dim_out.datatype.size()
                                        == a.datatype.size()
                                );
                                a.datatype = dim_out.datatype;
                                a
                            };
                            assert_eq!(dim_in, dim_out)
                        }
                        _ => unreachable!(),
                    }
                }
                assert!(inexact_field);
            }
        }
    }

    fn do_iterators(tdb: SchemaData) {
        let arrow_schema = {
            let c: Context = Context::new().unwrap();

            let tdb = tdb
                .create(&c)
                .expect("Error constructing arbitrary tiledb attribute");

            to_arrow(&tdb).unwrap().ok().unwrap()
        };

        let mut field_names = HashSet::new();

        for f in dimensions(&arrow_schema).unwrap() {
            assert!(tdb.field(f.name()).unwrap().is_dimension());
            assert!(field_names.insert(f.name()));
        }
        assert_eq!(
            tdb.domain.dimension.len(),
            dimensions(&arrow_schema).unwrap().len()
        );

        for f in attributes(&arrow_schema).unwrap() {
            assert!(tdb.field(f.name()).unwrap().is_attribute());
            assert!(field_names.insert(f.name()));
        }
        assert_eq!(
            tdb.attributes.len(),
            attributes(&arrow_schema).unwrap().len()
        );
    }

    proptest! {
        #[test]
        fn test_to_arrow(tdb_in in any::<SchemaData>()) {
            do_to_arrow(tdb_in)
        }

        #[test]
        fn test_iterators(tdb in any::<SchemaData>()) {
            do_iterators(tdb)
        }
    }
}
