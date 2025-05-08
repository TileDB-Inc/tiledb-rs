use tiledb_sys2::arrow::{Buffer, MutableBuffer, PhysicalType};
use tiledb_sys2::attribute;
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
        Ok(self.attr.datatype()?)
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

    pub fn fill_value<T: PhysicalType>(&self) -> Result<Buffer, TileDBError> {
        let size = self.attr.fill_value_size()? as usize;
        let mut buffer = MutableBuffer::new(size);
        self.attr.fill_value(buffer.as_slice_mut())?;
        Ok(buffer.into())
    }

    pub fn fill_value_nullable(&self) -> Result<(Buffer, u8), TileDBError> {
        let size = self.attr.fill_value_size()? as usize;
        let mut buffer = MutableBuffer::new(size);
        let mut validity: u8 = 0;
        self.attr
            .fill_value_nullable(buffer.as_slice_mut(), &mut validity)?;
        Ok((buffer.into(), validity))
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
            builder: attribute::create_attribute_builder(ctx.ctx, name, dtype)?,
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

    pub fn with_fill_value<T: PhysicalType>(
        self,
        value: &[T],
    ) -> Result<Self, TileDBError> {
        let buffer = T::slice_to_buffer(value);
        self.builder.set_fill_value(buffer.as_slice())?;
        Ok(self)
    }

    pub fn with_fill_value_nullable<T: PhysicalType>(
        self,
        value: &[T],
        validity: u8,
    ) -> Result<Self, TileDBError> {
        let buffer = T::slice_to_buffer(value);
        self.builder
            .set_fill_value_nullable(buffer.as_slice(), validity)?;
        Ok(self)
    }
}
