use anyhow::anyhow;
use arrow::datatypes::Schema as ArrowSchema;
use serde::{Deserialize, Serialize};

use crate::array::{
    ArrayType, AttributeBuilder, CellOrder, DimensionBuilder, DomainBuilder,
    Schema, SchemaBuilder, TileOrder,
};
use crate::filter::arrow::FilterMetadata;
use crate::{error::Error, Context, Result as TileDBResult};

pub type FieldToArrowResult = crate::arrow::ArrowConversionResult<
    arrow::datatypes::Field,
    arrow::datatypes::Field,
>;

pub type FieldFromArrowResult<F> = crate::arrow::ArrowConversionResult<F, F>;

pub type AttributeFromArrowResult<'ctx> =
    FieldFromArrowResult<AttributeBuilder<'ctx>>;
pub type DimensionFromArrowResult<'ctx> =
    FieldFromArrowResult<DimensionBuilder<'ctx>>;

pub type SchemaToArrowResult =
    crate::arrow::ArrowConversionResult<ArrowSchema, ArrowSchema>;

pub type SchemaFromArrowResult<'ctx> = crate::arrow::ArrowConversionResult<
    SchemaBuilder<'ctx>,
    SchemaBuilder<'ctx>,
>;

/// Represents required metadata to convert from an arrow schema
/// to a TileDB schema.
#[derive(Deserialize, Serialize)]
pub struct SchemaMetadata {
    array_type: ArrayType,
    version: i64,
    capacity: u64,
    allows_duplicates: bool,
    cell_order: CellOrder,
    tile_order: TileOrder,
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
            version: schema.version()?,
            capacity: schema.capacity()?,
            allows_duplicates: schema.allows_duplicates()?,
            cell_order: schema.cell_order()?,
            tile_order: schema.tile_order()?,
            coordinate_filters: FilterMetadata::new(
                &schema.coordinate_filters()?,
            )?,
            offsets_filters: FilterMetadata::new(&schema.offsets_filters()?)?,
            nullity_filters: FilterMetadata::new(&schema.nullity_filters()?)?,
            ndim: schema.domain()?.ndim()?,
        })
    }
}

pub fn to_arrow<'ctx>(
    tiledb: &'ctx Schema<'ctx>,
) -> TileDBResult<SchemaToArrowResult> {
    let mut builder =
        arrow::datatypes::SchemaBuilder::with_capacity(tiledb.nattributes()?);

    let mut inexact = false;

    for d in 0..tiledb.domain()?.ndim()? {
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

    for a in 0..tiledb.nattributes()? {
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

/// Construct a TileDB schema from an Arrow schema.
/// A TileDB schema must have domain and dimension details.
/// These are expected to be in the schema `metadata` beneath the key `tiledb`.
/// This metadata is expected to be a JSON object with the following fields:
pub fn from_arrow<'ctx>(
    context: &'ctx Context,
    schema: &ArrowSchema,
) -> TileDBResult<SchemaFromArrowResult<'ctx>> {
    let metadata = match schema.metadata().get("tiledb") {
        Some(metadata) => serde_json::from_str::<SchemaMetadata>(metadata)
            .map_err(|e| {
                Error::Deserialization(
                    String::from("schema metadata"),
                    anyhow!(e),
                )
            })?,
        None => return Ok(SchemaFromArrowResult::None),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::schema::SchemaData;
    use crate::Factory;
    use proptest::prelude::*;

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
            SchemaToArrowResult::Inexact(_) => {
                /* this could have missing attributes, inexact attributes, something */
                /*
                 * NB: this is currently unreachable by schemata produced by `any::<SchemaData>()`
                 * because the datatypes used for the attributes and dimensions are coincidentally
                 * restricted to datatypes that have an exact match. SC-46813
                 */
                unimplemented!()
            }
        }
    }

    proptest! {
        #[test]
        fn test_to_arrow(tdb_in in any::<SchemaData>()) {
            do_to_arrow(tdb_in)
        }
    }
}
