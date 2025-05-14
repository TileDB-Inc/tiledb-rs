use std::pin::Pin;

use tiledb_sys2::buffer::Buffer;
use tiledb_sys2::datatype::{Datatype, DatatypeError};
use tiledb_sys2::dimension;
use tiledb_sys2::types::PhysicalType;

use crate::context::Context;
use crate::error::TileDBError;
use crate::filter_list::FilterList;

pub struct Dimension {
    pub(crate) dim: cxx::SharedPtr<dimension::Dimension>,
}

impl Dimension {
    pub(crate) fn new(dim: cxx::SharedPtr<dimension::Dimension>) -> Self {
        Self { dim }
    }

    pub fn name(&self) -> Result<String, TileDBError> {
        Ok(self.dim.name()?)
    }

    pub fn datatype(&self) -> Result<Datatype, TileDBError> {
        Ok(self.dim.datatype()?.try_into()?)
    }

    pub fn domain(&self) -> Result<Option<Buffer>, TileDBError> {
        let dtype = self.datatype()?;
        let mut buf = Buffer::new(dtype);
        if self.dim.domain(Pin::new(&mut buf))? {
            Ok(Some(buf))
        } else {
            Ok(None)
        }
    }

    pub fn tile_extent(&self) -> Result<Option<Buffer>, TileDBError> {
        let dtype = self.datatype()?;
        let mut buf = Buffer::new(dtype);
        if self.dim.tile_extent(Pin::new(&mut buf))? {
            Ok(Some(buf))
        } else {
            Ok(None)
        }
    }

    pub fn cell_val_num(&self) -> Result<u32, TileDBError> {
        Ok(self.dim.cell_val_num()?)
    }

    pub fn filter_list(&self) -> Result<FilterList, TileDBError> {
        let ptr = self.dim.filter_list()?;
        Ok(FilterList::new(ptr))
    }
}

pub struct DimensionBuilder {
    ctx: Context,
    name: String,
    dtype: Datatype,
    domain: Option<Buffer>,
    extent: Option<Buffer>,
    cvn: Option<u32>,
    filters: Option<FilterList>,
}

impl DimensionBuilder {
    pub fn new(
        ctx: &Context,
        name: &str,
        dtype: Datatype,
    ) -> Result<Self, TileDBError> {
        Ok(DimensionBuilder {
            ctx: ctx.clone(),
            name: name.into(),
            dtype,
            domain: None,
            extent: None,
            cvn: None,
            filters: None,
        })
    }

    pub fn build(self) -> Result<Dimension, TileDBError> {
        let mut domain = self.domain.unwrap_or_else(|| Buffer::new(self.dtype));
        let mut extent = self.extent.unwrap_or_else(|| Buffer::new(self.dtype));

        let builder = dimension::create_dimension_builder(
            self.ctx.ctx,
            &self.name,
            self.dtype.into(),
            Pin::new(&mut domain),
            Pin::new(&mut extent),
        )?;

        if self.cvn.is_some() {
            builder.set_cell_val_num(self.cvn.unwrap())?
        }

        if self.filters.is_some() {
            builder.set_filter_list(self.filters.unwrap().list)?;
        }

        Ok(Dimension::new(builder.build()?))
    }

    pub fn with_domain<T: PhysicalType>(
        mut self,
        domain: &[T; 2],
    ) -> Result<Self, TileDBError> {
        let domain: Vec<_> = domain.into();
        let buf = Buffer::try_from((self.dtype, domain))?;
        self.domain = Some(buf);
        Ok(self)
    }

    pub fn with_domain_buffer(
        mut self,
        buf: Buffer,
    ) -> Result<Self, TileDBError> {
        if !buf.is_compatible(self.dtype) {
            Err(DatatypeError::LogicalTypeMismatch {
                source_type: buf.datatype(),
                target_type: self.dtype,
            })?;
        }

        self.domain = Some(buf);
        Ok(self)
    }

    pub fn with_tile_extent<T: PhysicalType>(
        mut self,
        extent: T,
    ) -> Result<Self, TileDBError> {
        let buf = Buffer::try_from((self.dtype, vec![extent]))?;
        self.extent = Some(buf);
        Ok(self)
    }

    pub fn with_tile_extent_buffer(
        mut self,
        buf: Buffer,
    ) -> Result<Self, TileDBError> {
        if !buf.is_compatible(self.dtype) {
            Err(DatatypeError::LogicalTypeMismatch {
                source_type: buf.datatype(),
                target_type: self.dtype,
            })?;
        }

        self.extent = Some(buf);
        Ok(self)
    }

    pub fn with_cell_val_num(
        mut self,
        cell_val_num: u32,
    ) -> Result<Self, TileDBError> {
        self.cvn = Some(cell_val_num);
        Ok(self)
    }

    pub fn with_filter_list(
        mut self,
        filters: FilterList,
    ) -> Result<Self, TileDBError> {
        self.filters = Some(filters);
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() -> Result<(), TileDBError> {
        let ctx = Context::new()?;
        let dim = DimensionBuilder::new(&ctx, "dim", Datatype::Int32)?
            .with_domain(&[0i32, 1024])?
            .with_tile_extent(64i32)?
            .with_cell_val_num(1)?
            .build()?;

        assert_eq!(dim.name()?, "dim");
        assert_eq!(dim.datatype()?, Datatype::Int32);

        let dom = dim.domain()?;
        assert_eq!(dom.unwrap().into_vec::<i32>()?, vec![0i32, 1024]);

        let ext = dim.tile_extent()?;
        assert_eq!(ext.unwrap().into_vec::<i32>()?, vec![64i32]);

        Ok(())
    }

    #[test]
    fn string_dimension() -> Result<(), TileDBError> {
        let ctx = Context::new()?;
        let dim =
            DimensionBuilder::new(&ctx, "str_dim", Datatype::StringAscii)?
                .with_cell_val_num(u32::MAX)?
                .build()?;

        assert_eq!(dim.name()?, "str_dim");
        assert_eq!(dim.datatype()?, Datatype::StringAscii);
        assert!(dim.domain()?.is_none());
        assert!(dim.tile_extent()?.is_none());
        assert_eq!(dim.cell_val_num()?, u32::MAX);

        Ok(())
    }

    #[test]
    fn wrong_domain_type() -> Result<(), TileDBError> {
        let ctx = Context::new()?;
        let dim = DimensionBuilder::new(&ctx, "dim", Datatype::Int32)?;
        assert!(dim.with_domain(&[0u64, 1024]).is_err());

        Ok(())
    }

    #[test]
    fn wrong_extent_type() -> Result<(), TileDBError> {
        let ctx = Context::new()?;
        let dim = DimensionBuilder::new(&ctx, "dim", Datatype::Int32)?;
        assert!(dim.with_tile_extent(64u64).is_err());

        Ok(())
    }
}
