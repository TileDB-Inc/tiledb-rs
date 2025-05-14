use std::pin::Pin;

use tiledb_sys2::attribute;
use tiledb_sys2::buffer::Buffer;
use tiledb_sys2::datatype::Datatype;

use crate::context::Context;
use crate::error::TileDBError;
use crate::filter_list::FilterList;

pub struct Attribute {
    pub(crate) attr: cxx::SharedPtr<attribute::Attribute>,
}

impl Attribute {
    pub(crate) fn new(attr: cxx::SharedPtr<attribute::Attribute>) -> Self {
        Self { attr }
    }

    pub fn name(&self) -> Result<String, TileDBError> {
        Ok(self.attr.name()?)
    }

    pub fn datatype(&self) -> Result<Datatype, TileDBError> {
        Ok(self.attr.datatype()?.try_into()?)
    }

    pub fn cell_size(&self) -> Result<u64, TileDBError> {
        Ok(self.attr.cell_size()?)
    }

    pub fn cell_val_num(&self) -> Result<u32, TileDBError> {
        Ok(self.attr.cell_val_num()?)
    }

    pub fn nullable(&self) -> Result<bool, TileDBError> {
        Ok(self.attr.nullable()?)
    }

    pub fn enumeration_name(&self) -> Result<Option<String>, TileDBError> {
        let mut name = String::new();
        if self.attr.enumeration_name(&mut name)? {
            Ok(Some(name))
        } else {
            Ok(None)
        }
    }

    pub fn filter_list(&self) -> Result<FilterList, TileDBError> {
        Ok(FilterList::new(self.attr.filter_list()?))
    }

    pub fn fill_value(&self) -> Result<Buffer, TileDBError> {
        let mut value = Buffer::new(self.datatype()?);
        self.attr.fill_value(Pin::new(&mut value))?;
        Ok(value)
    }

    pub fn fill_value_nullable(&self) -> Result<(Buffer, u8), TileDBError> {
        let mut value = Buffer::new(self.datatype()?);
        let mut validity: u8 = 0;
        self.attr
            .fill_value_nullable(Pin::new(&mut value), &mut validity)?;
        Ok((value, validity))
    }
}

pub struct AttributeBuilder {
    builder: cxx::SharedPtr<attribute::AttributeBuilder>,
}

impl AttributeBuilder {
    pub fn new(
        ctx: Context,
        name: &str,
        dtype: Datatype,
    ) -> Result<Self, TileDBError> {
        Ok(Self {
            builder: attribute::create_attribute_builder(
                ctx.ctx,
                name,
                dtype.into(),
            )?,
        })
    }

    pub fn build(self) -> Result<Attribute, TileDBError> {
        Ok(Attribute::new(self.builder.build()?))
    }

    pub fn with_nullable(self, nullable: bool) -> Result<Self, TileDBError> {
        self.builder.set_nullable(nullable)?;
        Ok(self)
    }

    pub fn with_cell_val_num(self, cvn: u32) -> Result<Self, TileDBError> {
        self.builder.set_cell_val_num(cvn)?;
        Ok(self)
    }

    pub fn with_enumeration_name(
        self,
        name: &str,
    ) -> Result<Self, TileDBError> {
        self.builder.set_enumeration_name(name)?;
        Ok(self)
    }

    pub fn with_fill_value(
        self,
        value: &mut Buffer,
    ) -> Result<Self, TileDBError> {
        self.builder.set_fill_value(Pin::new(value))?;
        Ok(self)
    }

    pub fn with_fill_value_nullable(
        self,
        value: &mut Buffer,
        validity: u8,
    ) -> Result<Self, TileDBError> {
        self.builder
            .set_fill_value_nullable(Pin::new(value), validity)?;
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() -> Result<(), TileDBError> {
        let ctx = Context::new()?;
        let builder = AttributeBuilder::new(ctx, "foo", Datatype::Int32)?;
        let attr = builder
            .with_nullable(true)?
            .with_cell_val_num(2)?
            .with_enumeration_name("bar")?
            .build()?;

        assert_eq!(attr.name()?, "foo");
        assert_eq!(attr.datatype()?, Datatype::Int32);
        assert!(attr.nullable()?);
        assert_eq!(attr.cell_size()?, 4 * 2);
        assert_eq!(attr.enumeration_name()?, Some("bar".into()));

        Ok(())
    }

    #[test]
    fn fill_value() -> Result<(), TileDBError> {
        let ctx = Context::new()?;
        let builder =
            AttributeBuilder::new(ctx.clone(), "foo", Datatype::Int32)?;
        let mut fill = Buffer::try_from((Datatype::Int32, vec![42i32]))?;
        let attr = builder.with_fill_value(&mut fill)?.build()?;
        let buffer = attr.fill_value()?;

        assert_eq!(buffer.into_vec::<i32>()?, vec![42]);

        Ok(())
    }

    #[test]
    fn fill_value_nullable() -> Result<(), TileDBError> {
        let ctx = Context::new()?;
        let builder = AttributeBuilder::new(ctx, "foo", Datatype::Int32)?;
        let mut fill = Buffer::try_from((Datatype::Int32, vec![42i32]))?;
        let attr = builder
            .with_nullable(true)?
            .with_fill_value_nullable(&mut fill, 127)?
            .build()?;
        let (buffer, validity) = attr.fill_value_nullable()?;

        assert_eq!(buffer.into_vec::<i32>()?, vec![42]);
        assert_eq!(validity, 127);

        Ok(())
    }
}
