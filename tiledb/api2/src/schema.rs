use tiledb_sys2::schema;

use tiledb_common::array::{ArrayType, CellOrder, TileOrder};

use crate::attribute::Attribute;
use crate::context::Context;
use crate::domain::Domain;
use crate::enumeration::Enumeration;
use crate::error::TileDBError;
use crate::filter_list::FilterList;

pub struct Schema {
    pub(crate) schema: cxx::SharedPtr<schema::Schema>,
}

impl Schema {
    pub(crate) fn new(schema: cxx::SharedPtr<schema::Schema>) -> Self {
        Self { schema }
    }

    pub fn array_type(&self) -> Result<ArrayType, TileDBError> {
        Ok(self.schema.array_type()?.try_into()?)
    }

    pub fn capacity(&self) -> Result<u64, TileDBError> {
        Ok(self.schema.capacity()?)
    }

    pub fn allows_dups(&self) -> Result<bool, TileDBError> {
        Ok(self.schema.allows_dups()?)
    }

    pub fn tile_order(&self) -> Result<TileOrder, TileDBError> {
        Ok(self.schema.tile_order()?.try_into()?)
    }

    pub fn cell_order(&self) -> Result<CellOrder, TileDBError> {
        Ok(self.schema.cell_order()?.try_into()?)
    }

    pub fn domain(&self) -> Result<Domain, TileDBError> {
        let dom = self.schema.domain()?;
        Ok(Domain::new(dom))
    }

    pub fn num_attributes(&self) -> Result<u32, TileDBError> {
        Ok(self.schema.num_attributes()?)
    }

    pub fn has_attribute(&self, name: &str) -> Result<bool, TileDBError> {
        Ok(self.schema.has_attribute(name)?)
    }

    pub fn attribute_from_name(
        &self,
        name: &str,
    ) -> Result<Attribute, TileDBError> {
        let attr = self.schema.attribute_from_name(name)?;
        Ok(Attribute::new(attr))
    }

    pub fn attribute_from_index(
        &self,
        index: u32,
    ) -> Result<Attribute, TileDBError> {
        let attr = self.schema.attribute_from_index(index)?;
        Ok(Attribute::new(attr))
    }

    pub fn enumeration(
        &self,
        enmr_name: &str,
    ) -> Result<Enumeration, TileDBError> {
        let enmr = self.schema.enumeration(enmr_name)?;
        Ok(Enumeration::new(enmr))
    }

    pub fn enumeration_for_attribute(
        &self,
        attr_name: &str,
    ) -> Result<Enumeration, TileDBError> {
        let enmr = self.schema.enumeration_for_attribute(attr_name)?;
        Ok(Enumeration::new(enmr))
    }

    pub fn coords_filter_list(&self) -> Result<FilterList, TileDBError> {
        let filters = self.schema.coords_filter_list()?;
        Ok(FilterList::new(filters))
    }

    pub fn offsets_filter_list(&self) -> Result<FilterList, TileDBError> {
        let filters = self.schema.offsets_filter_list()?;
        Ok(FilterList::new(filters))
    }

    pub fn validity_filter_list(&self) -> Result<FilterList, TileDBError> {
        let filters = self.schema.validity_filter_list()?;
        Ok(FilterList::new(filters))
    }

    pub fn timestamp_range(&self) -> Result<(u64, u64), TileDBError> {
        let mut start = 0u64;
        let mut end = 0u64;
        self.schema.timestamp_range(&mut start, &mut end)?;
        Ok((start, end))
    }
}

pub struct SchemaBuilder {
    pub(crate) builder: cxx::SharedPtr<schema::SchemaBuilder>,
}

impl SchemaBuilder {
    pub fn new(
        ctx: &Context,
        array_type: ArrayType,
    ) -> Result<Self, TileDBError> {
        Ok(SchemaBuilder {
            builder: schema::create_schema_builder(
                ctx.ctx.clone(),
                array_type.into(),
            )?,
        })
    }

    pub fn build(self) -> Result<Schema, TileDBError> {
        let schema = self.builder.build()?;
        Ok(Schema::new(schema))
    }

    pub fn with_capacity(self, capacity: u64) -> Result<Self, TileDBError> {
        self.builder.set_capacity(capacity)?;
        Ok(self)
    }

    pub fn with_allows_dups(
        self,
        allows_dups: bool,
    ) -> Result<Self, TileDBError> {
        self.builder.set_allows_dups(allows_dups)?;
        Ok(self)
    }

    pub fn with_tile_order(
        self,
        order: TileOrder,
    ) -> Result<Self, TileDBError> {
        self.builder.set_tile_order(order.into())?;
        Ok(self)
    }

    pub fn with_cell_order(
        self,
        order: CellOrder,
    ) -> Result<Self, TileDBError> {
        self.builder.set_cell_order(order.into())?;
        Ok(self)
    }

    pub fn with_domain(self, domain: Domain) -> Result<Self, TileDBError> {
        self.builder.set_domain(domain.dom.clone())?;
        Ok(self)
    }

    pub fn with_attribute(self, attr: Attribute) -> Result<Self, TileDBError> {
        self.builder.add_attribute(attr.attr)?;
        Ok(self)
    }

    pub fn with_attributes(
        self,
        attrs: &[Attribute],
    ) -> Result<Self, TileDBError> {
        for attr in attrs {
            self.builder.add_attribute(attr.attr.clone())?;
        }
        Ok(self)
    }

    pub fn with_enumeration(
        self,
        enmr: Enumeration,
    ) -> Result<Self, TileDBError> {
        self.builder.add_enumeration(enmr.enmr)?;
        Ok(self)
    }

    pub fn with_enumerations(
        self,
        enmrs: &[Enumeration],
    ) -> Result<Self, TileDBError> {
        for enmr in enmrs {
            self.builder.add_enumeration(enmr.enmr.clone())?;
        }
        Ok(self)
    }

    pub fn set_coods_filter_list(
        self,
        filters: FilterList,
    ) -> Result<Self, TileDBError> {
        self.builder.set_coords_filter_list(filters.list)?;
        Ok(self)
    }

    pub fn set_offsets_filter_list(
        self,
        filters: FilterList,
    ) -> Result<Self, TileDBError> {
        self.builder.set_offsets_filter_list(filters.list)?;
        Ok(self)
    }

    pub fn set_validity_filter_list(
        self,
        filters: FilterList,
    ) -> Result<Self, TileDBError> {
        self.builder.set_validity_filter_list(filters.list)?;
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use tiledb_sys2::datatype::Datatype;

    use super::*;
    use crate::attribute::AttributeBuilder;
    use crate::dimension::DimensionBuilder;
    use crate::domain::DomainBuilder;
    use crate::enumeration::EnumerationBuilder;

    #[test]
    fn basic() -> Result<(), TileDBError> {
        let ctx = Context::new()?;

        let rows = DimensionBuilder::new(&ctx, "rows", Datatype::Int32)?
            .with_domain(&[0, 4])?
            .with_tile_extent(1)?
            .build()?;

        let cols = DimensionBuilder::new(&ctx, "cols", Datatype::Int32)?
            .with_domain(&[0, 4])?
            .with_tile_extent(1)?
            .build()?;

        let dom = DomainBuilder::new(&ctx)?
            .with_dimensions(&[rows, cols])?
            .build()?;

        let attr =
            AttributeBuilder::new(&ctx, "attr", Datatype::Int32)?.build()?;

        let schema = SchemaBuilder::new(&ctx, ArrayType::Sparse)?
            .with_capacity(1000)?
            .with_allows_dups(true)?
            .with_tile_order(TileOrder::RowMajor)?
            .with_cell_order(CellOrder::RowMajor)?
            .with_domain(dom)?
            .with_attribute(attr)?
            .build()?;

        assert_eq!(schema.array_type()?, ArrayType::Sparse);
        assert_eq!(schema.capacity()?, 1000);
        assert!(schema.allows_dups()?);
        assert_eq!(schema.tile_order()?, TileOrder::RowMajor);
        assert_eq!(schema.cell_order()?, CellOrder::RowMajor);
        assert_eq!(schema.domain()?.num_dimensions()?, 2);
        assert!(schema.domain()?.dimension_from_name("rows").is_ok());
        assert!(schema.domain()?.dimension_from_index(1).is_ok());
        assert_eq!(schema.num_attributes()?, 1);
        assert!(schema.has_attribute("attr")?);
        assert!(schema.attribute_from_index(0).is_ok());
        assert!(schema.attribute_from_name("attr").is_ok());
        assert!(schema.coords_filter_list().is_ok());
        assert!(schema.offsets_filter_list().is_ok());
        assert!(schema.validity_filter_list().is_ok());
        assert!(schema.timestamp_range().is_ok());

        Ok(())
    }

    #[test]
    fn check_enumerations() -> Result<(), TileDBError> {
        let ctx = Context::new()?;

        let dim = DimensionBuilder::new(&ctx, "dim", Datatype::Int32)?
            .with_domain(&[0, 4])?
            .with_tile_extent(1)?
            .build()?;

        let dom = DomainBuilder::new(&ctx)?.with_dimension(dim)?.build()?;

        let flintstones = ["fred", "wilma", "barney", "betty"];
        let enmr1 = EnumerationBuilder::from_strings(
            &ctx,
            "flintstones",
            &flintstones,
        )?
        .build()?;

        let crawlers = ["carl", "doughnut", "mordecai", "katia"];
        let enmr2 =
            EnumerationBuilder::from_strings(&ctx, "crawlers", &crawlers)?
                .build()?;

        let hwfwm = ["jason", "humphrey", "clive", "sophie", "belinda", "neal"];
        let enmr3 =
            EnumerationBuilder::from_strings(&ctx, "hwfwm", &hwfwm)?.build()?;

        let attr1 = AttributeBuilder::new(&ctx, "attr1", Datatype::Int32)?
            .with_enumeration_name("flintstones")?
            .build()?;

        let attr2 = AttributeBuilder::new(&ctx, "attr2", Datatype::Int32)?
            .with_enumeration_name("crawlers")?
            .build()?;

        let schema = SchemaBuilder::new(&ctx, ArrayType::Sparse)?
            .with_domain(dom)?
            .with_enumeration(enmr1)?
            .with_enumerations(&[enmr2, enmr3])?
            .with_attributes(&[attr1, attr2])?
            .build()?;

        assert!(schema.enumeration("flintstones").is_ok());
        assert!(schema.enumeration("crawlers").is_ok());
        assert!(schema.enumeration("hwfwm").is_ok());
        assert_eq!(
            schema.attribute_from_name("attr1")?.enumeration_name()?,
            Some("flintstones".into())
        );
        assert!(schema.enumeration_for_attribute("attr2").is_ok());

        Ok(())
    }
}
