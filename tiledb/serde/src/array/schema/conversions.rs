use std::borrow::Borrow;

impl TryFrom<B> for SchemaData
where
    B: Borrow<Schema>,
{
    type Error = crate::error::Error;

    fn try_from(schema: B) -> TileDBResult<Self> {
        let schema = schema.borrow();
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
            coordinate_filters: FilterListData::try_from(
                &schema.coordinate_filters()?,
            )?,
            offsets_filters: FilterListData::try_from(
                &schema.offsets_filters()?,
            )?,
            nullity_filters: FilterListData::try_from(
                &schema.nullity_filters()?,
            )?,
        })
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
