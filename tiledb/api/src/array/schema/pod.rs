use itertools::Itertools;
use tiledb_common::filter::FilterData;
use tiledb_pod::array::attribute::AttributeData;
use tiledb_pod::array::schema::{FieldData, SchemaData};
use tiledb_pod::array::{DimensionData, DomainData, EnumerationData};

use super::{Builder, EnumerationKey, Field, Schema};
use crate::error::Error;
use crate::{Context, Factory, Result as TileDBResult};

impl TryFrom<&Schema> for SchemaData {
    type Error = Error;

    fn try_from(schema: &Schema) -> Result<Self, Self::Error> {
        let attributes = (0..schema.num_attributes()?)
            .map(|a| AttributeData::try_from(&schema.attribute(a)?))
            .collect::<TileDBResult<Vec<AttributeData>>>()?;

        let enumerations = attributes
            .iter()
            .filter_map(|a| a.enumeration.as_ref())
            .unique()
            .map(|ename| {
                EnumerationData::try_from(
                    schema
                        .enumeration(EnumerationKey::EnumerationName(ename))?,
                )
            })
            .collect::<TileDBResult<Vec<EnumerationData>>>()?;

        Ok(SchemaData {
            array_type: schema.array_type()?,
            domain: DomainData::try_from(&schema.domain()?)?,
            capacity: Some(schema.capacity()?),
            cell_order: Some(schema.cell_order()?),
            tile_order: Some(schema.tile_order()?),
            allow_duplicates: Some(schema.allows_duplicates()?),
            attributes,
            enumerations,
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
    type Error = Error;

    fn try_from(schema: Schema) -> Result<Self, Self::Error> {
        Self::try_from(&schema)
    }
}

impl Factory for SchemaData {
    type Item = Schema;

    fn create(&self, context: &Context) -> TileDBResult<Self::Item> {
        let mut b = Builder::new(
            context,
            self.array_type,
            self.domain.create(context)?,
        )?
        .coordinate_filters(self.coordinate_filters.create(context)?)?
        .offsets_filters(self.offsets_filters.create(context)?)?
        .nullity_filters(self.nullity_filters.create(context)?)?;

        b = self
            .enumerations
            .iter()
            .try_fold(b, |b, e| b.add_enumeration(e.create(context)?))?;

        b = self
            .attributes
            .iter()
            .try_fold(b, |b, a| b.add_attribute(a.create(context)?))?;

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
    type Error = Error;

    fn try_from(field: &Field) -> Result<Self, Self::Error> {
        match field {
            Field::Dimension(d) => Ok(Self::from(DimensionData::try_from(d)?)),
            Field::Attribute(a) => Ok(Self::from(AttributeData::try_from(a)?)),
        }
    }
}

impl TryFrom<Field> for FieldData {
    type Error = Error;

    fn try_from(field: Field) -> Result<Self, Self::Error> {
        Self::try_from(&field)
    }
}
