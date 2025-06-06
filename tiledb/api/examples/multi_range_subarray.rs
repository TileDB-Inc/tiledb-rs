extern crate tiledb_api as tiledb;

use std::path::PathBuf;

use itertools::izip;

use tiledb::Datatype;
use tiledb::Result as TileDBResult;
use tiledb::array::{
    Array, ArrayType, AttributeBuilder, CellOrder, DimensionBuilder,
    DomainBuilder, SchemaBuilder, TileOrder,
};
use tiledb::context::Context;
use tiledb::query::{
    Query, QueryBuilder, ReadBuilder, ReadQuery, ReadQueryBuilder, WriteBuilder,
};

const ARRAY_URI: &str = "multi_range_slicing";

/// This example creates a 4x4 dense array with the contents:
///
/// Col:     1   2   3   4
/// Row: 1   1   2   3   4
///      2   5   6   7   8
///      3   9  10  11  12
///      4  13  14  15  16
///
/// The query run restricts rows to [1, 2, 4] and returns all columns which
/// should produce these rows:
///
/// Row Col Value
/// 1   1   1
/// 1   2   2
/// 1   3   3
/// 1   4   4
/// 2   1   5
/// 2   2   6
/// 2   3   7
/// 2   4   8
/// 4   1   13
/// 4   2   14
/// 4   3   15
/// 4   4   16
fn main() -> TileDBResult<()> {
    if let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR") {
        let _ = std::env::set_current_dir(
            PathBuf::from(manifest_dir).join("examples").join("output"),
        );
    }

    let ctx = Context::new()?;
    if !Array::exists(&ctx, ARRAY_URI)? {
        create_array(&ctx)?;
        write_array(&ctx)?;
    }

    let array = Array::open(&ctx, ARRAY_URI, tiledb::array::Mode::Read)?;
    let mut query = ReadBuilder::new(array)?
        .layout(tiledb::query::QueryLayout::RowMajor)?
        .register_constructor::<_, Vec<i32>>("rows", Default::default())?
        .register_constructor::<_, Vec<i32>>("cols", Default::default())?
        .register_constructor::<_, Vec<i32>>("a", Default::default())?
        .start_subarray()?
        .add_range("rows", &[1, 2])?
        .add_range("rows", &[4, 4])?
        .add_range("cols", &[1, 4])?
        .finish_subarray()?
        .build();

    let (a, (cols, (rows, ()))) = query.execute()?;

    for (row, col, a) in izip!(rows, cols, a) {
        println!("{row} {col} {a}");
    }
    println!();

    Ok(())
}

fn create_array(ctx: &Context) -> TileDBResult<()> {
    let schema = {
        let domain = DomainBuilder::new(ctx)?
            .add_dimension(
                DimensionBuilder::new(
                    ctx,
                    "rows",
                    Datatype::Int32,
                    ([1i32, 4], 4i32),
                )?
                .build(),
            )?
            .add_dimension(
                DimensionBuilder::new(
                    ctx,
                    "cols",
                    Datatype::Int32,
                    ([1i32, 4], 4i32),
                )?
                .build(),
            )?
            .build();

        SchemaBuilder::new(ctx, ArrayType::Dense, domain)?
            .cell_order(CellOrder::RowMajor)?
            .tile_order(TileOrder::RowMajor)?
            .add_attribute(
                AttributeBuilder::new(ctx, "a", Datatype::Int32)?.build(),
            )?
            .build()?
    };

    Array::create(ctx, ARRAY_URI, schema)?;
    Ok(())
}

fn write_array(ctx: &Context) -> TileDBResult<()> {
    let data = vec![1i32, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

    let array =
        tiledb::Array::open(ctx, ARRAY_URI, tiledb::array::Mode::Write)?;

    let query = WriteBuilder::new(array)?
        .layout(CellOrder::RowMajor)?
        .data_typed("a", &data)?
        .build();

    query.submit().and_then(|_| query.finalize())?;

    Ok(())
}
