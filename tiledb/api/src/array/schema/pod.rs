use tiledb_common::filter::FilterData;
use tiledb_pod::array::attribute::AttributeData;
use tiledb_pod::array::dimension::DimensionData;
use tiledb_pod::array::domain::DomainData;
use tiledb_pod::array::schema::{FieldData, SchemaData};

use super::{Builder, Field, Schema};
use crate::error::Error as TileDBError;
use crate::{Context, Factory, Result as TileDBResult};

impl TryFrom<&Schema> for SchemaData {
    type Error = TileDBError;

    fn try_from(schema: &Schema) -> Result<Self, Self::Error> {
        Ok(SchemaData {
            array_type: schema.array_type()?,
            domain: DomainData::try_from(&schema.domain()?)?,
            capacity: Some(schema.capacity()?),
            cell_order: Some(schema.cell_order()?),
            tile_order: Some(schema.tile_order()?),
            allow_duplicates: Some(schema.allows_duplicates()?),
            attributes: (0..schema.num_attributes()?)
                .map(|a| AttributeData::try_from(&schema.attribute(a)?))
                .collect::<TileDBResult<Vec<AttributeData>>>()?,
            coordinate_filters: Vec::<FilterData>::try_from(
                &schema.coordinate_filters()?,
            )?,
            offsets_filters: Vec::<FilterData>::try_from(
                &schema.offsets_filters()?,
            )?,
            nullity_filters: Vec::<FilterData>::try_from(
                &schema.nullity_filters()?,
            )?,
        })
    }
}

impl TryFrom<Schema> for SchemaData {
    type Error = TileDBError;

    fn try_from(schema: Schema) -> Result<Self, Self::Error> {
        Self::try_from(&schema)
    }
}

impl Factory for SchemaData {
    type Item = Schema;

    fn create(&self, context: &Context) -> TileDBResult<Self::Item> {
        let mut b = self.attributes.iter().try_fold(
            Builder::new(
                context,
                self.array_type,
                self.domain.create(context)?,
            )?
            .coordinate_filters(self.coordinate_filters.create(context)?)?
            .offsets_filters(self.offsets_filters.create(context)?)?
            .nullity_filters(self.nullity_filters.create(context)?)?,
            |b, a| b.add_attribute(a.create(context)?),
        )?;
        if let Some(c) = self.capacity {
            b = b.capacity(c)?;
        }
        if let Some(d) = self.allow_duplicates {
            b = b.allow_duplicates(d)?;
        }
        if let Some(o) = self.cell_order {
            b = b.cell_order(o)?;
        }
        if let Some(o) = self.tile_order {
            b = b.tile_order(o)?;
        }

        b.build()
    }
}

impl TryFrom<&Field> for FieldData {
    type Error = TileDBError;

    fn try_from(field: &Field) -> Result<Self, Self::Error> {
        match field {
            Field::Dimension(d) => Ok(Self::from(DimensionData::try_from(d)?)),
            Field::Attribute(a) => Ok(Self::from(AttributeData::try_from(a)?)),
        }
    }
}

impl TryFrom<Field> for FieldData {
    type Error = TileDBError;

    fn try_from(field: Field) -> Result<Self, Self::Error> {
        Self::try_from(&field)
    }
}
