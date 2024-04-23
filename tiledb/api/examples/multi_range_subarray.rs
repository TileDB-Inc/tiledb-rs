extern crate tiledb;

use itertools::izip;
use serde_json::json;

use tiledb::array::{
    Array, ArrayType, AttributeData, CellOrder, DimensionData, DomainData,
    SchemaData, TileOrder,
};
use tiledb::context::Context;
use tiledb::query::{
    Query, QueryBuilder, ReadBuilder, ReadQuery, ReadQueryBuilder, WriteBuilder,
};
use tiledb::Result as TileDBResult;
use tiledb::{Datatype, Factory};

const ARRAY_URI: &str = "multi_range_slicing";

fn main() -> TileDBResult<()> {
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
        println!("{} {} {}", row, col, a);
    }
    println!();

    Ok(())
}

fn create_array(ctx: &Context) -> TileDBResult<()> {
    let schema = SchemaData {
        array_type: ArrayType::Dense,
        domain: DomainData {
            dimension: vec![
                DimensionData {
                    name: "rows".to_owned(),
                    datatype: Datatype::Int32,
                    domain: Some([json!(1), json!(4)]),
                    extent: Some(json!(4)),
                    ..Default::default()
                },
                DimensionData {
                    name: "cols".to_owned(),
                    datatype: Datatype::Int32,
                    domain: Some([json!(1), json!(4)]),
                    extent: Some(json!(4)),
                    ..Default::default()
                },
            ],
        },
        attributes: vec![AttributeData {
            name: "a".to_owned(),
            datatype: Datatype::Int32,
            ..Default::default()
        }],
        tile_order: Some(TileOrder::RowMajor),
        cell_order: Some(CellOrder::RowMajor),

        ..Default::default()
    };

    let schema = schema.create(ctx)?;
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
