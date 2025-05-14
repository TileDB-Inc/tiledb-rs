use tiledb_sys2::datatype::Datatype;
use tiledb_sys2::domain;

use crate::context::Context;
use crate::dimension::Dimension;
use crate::error::TileDBError;

pub struct Domain {
    pub(crate) dom: cxx::SharedPtr<domain::Domain>,
}

impl Domain {
    pub(crate) fn new(dom: cxx::SharedPtr<domain::Domain>) -> Self {
        Self { dom }
    }

    pub fn datatype(&self) -> Result<Datatype, TileDBError> {
        Ok(self.dom.datatype()?.try_into()?)
    }

    pub fn num_dimensions(&self) -> Result<u32, TileDBError> {
        Ok(self.dom.num_dimensions()?)
    }

    pub fn dimension_from_index(
        &self,
        idx: u32,
    ) -> Result<Dimension, TileDBError> {
        let dim = self.dom.dimension_from_index(idx)?;
        Ok(Dimension::new(dim))
    }

    pub fn dimension_from_name(
        &self,
        name: &str,
    ) -> Result<Dimension, TileDBError> {
        let dim = self.dom.dimension_from_name(name)?;
        Ok(Dimension::new(dim))
    }

    pub fn has_dimension(&self, name: &str) -> Result<bool, TileDBError> {
        Ok(self.dom.has_dimension(name)?)
    }
}

pub struct DomainBuilder {
    builder: cxx::SharedPtr<domain::DomainBuilder>,
}

impl DomainBuilder {
    pub fn new(ctx: &Context) -> Result<Self, TileDBError> {
        let ctx = ctx.clone();
        Ok(Self {
            builder: domain::create_domain_builder(ctx.ctx)?,
        })
    }

    pub fn build(self) -> Result<Domain, TileDBError> {
        let dom = self.builder.build()?;
        Ok(Domain::new(dom))
    }

    pub fn with_dimension(self, dim: &Dimension) -> Result<Self, TileDBError> {
        self.builder.add_dimension(dim.dim.clone())?;
        Ok(self)
    }

    pub fn with_dimensions(
        self,
        dims: &[Dimension],
    ) -> Result<Self, TileDBError> {
        for dim in dims {
            self.builder.add_dimension(dim.dim.clone())?;
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dimension::DimensionBuilder;

    #[test]
    fn basic() -> Result<(), TileDBError> {
        let ctx = Context::new()?;
        let dim = DimensionBuilder::new(&ctx, "foo", Datatype::Int32)?
            .with_domain(&[0i32, 1024])?
            .build()?;

        let dom = DomainBuilder::new(&ctx)?.with_dimension(&dim)?.build()?;
        assert_eq!(dom.datatype()?, Datatype::Int32);
        assert_eq!(dom.num_dimensions()?, 1);
        assert!(dom.dimension_from_index(0).is_ok());
        assert!(dom.dimension_from_name("foo").is_ok());
        assert!(dom.has_dimension("foo")?);

        Ok(())
    }

    #[test]
    fn check_errors() -> Result<(), TileDBError> {
        let ctx = Context::new()?;
        let d1 = DimensionBuilder::new(&ctx, "d1", Datatype::Int32)?
            .with_domain(&[0i32, 1024])?
            .build()?;

        let d2 = DimensionBuilder::new(&ctx, "d2", Datatype::Float64)?
            .with_domain(&[0.0f64, 1024.0])?
            .build()?;

        let dom = DomainBuilder::new(&ctx)?
            .with_dimensions(&[d1, d2])?
            .build()?;

        assert!(dom.datatype().is_err());
        assert_eq!(dom.num_dimensions()?, 2);
        assert!(dom.dimension_from_index(0).is_ok());
        assert!(dom.dimension_from_index(6).is_err());
        assert!(dom.dimension_from_name("foo").is_err());
        assert!(!dom.has_dimension("foo")?);

        Ok(())
    }
}
