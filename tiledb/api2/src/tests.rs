use tiledb_common::array::{ArrayType, CellOrder, TileOrder};
use tiledb_sys2::datatype::Datatype;

use crate::attribute::AttributeBuilder;
use crate::context::Context;
use crate::dimension::DimensionBuilder;
use crate::domain::DomainBuilder;
use crate::error::TileDBError;
use crate::schema::{Schema, SchemaBuilder};

pub fn create_quickstart_schema(
    ctx: &Context,
    atype: ArrayType,
) -> Result<Schema, TileDBError> {
    let rows = DimensionBuilder::new(ctx, "rows", Datatype::Int32)?
        .with_domain(&[0, 4])?
        .with_tile_extent(1)?
        .build()?;

    let cols = DimensionBuilder::new(ctx, "columns", Datatype::Int32)?
        .with_domain(&[0, 4])?
        .with_tile_extent(1)?
        .build()?;

    let dom = DomainBuilder::new(ctx)?
        .with_dimensions(&[rows, cols])?
        .build()?;

    let attr = AttributeBuilder::new(ctx, "a", Datatype::Int32)?.build()?;

    let schema = SchemaBuilder::new(ctx, atype)?
        .with_capacity(1000)?
        .with_tile_order(TileOrder::RowMajor)?
        .with_cell_order(CellOrder::RowMajor)?
        .with_domain(dom)?
        .with_attribute(attr)?
        .build()?;

    Ok(schema)
}
