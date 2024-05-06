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

const ARRAY_URI: &str = "quickstart_sparse_string_array";

fn main() -> TileDBResult<()> {
    let ctx = Context::new()?;
    if !Array::exists(&ctx, ARRAY_URI)? {
        create_array(&ctx)?;
        write_array(&ctx)?;
    }

    let array = Array::open(&ctx, ARRAY_URI, tiledb::array::Mode::Read)?;
    let mut query = ReadBuilder::new(array)?
        .layout(tiledb::query::QueryLayout::RowMajor)?
        .register_constructor::<_, Vec<String>>("rows", Default::default())?
        .register_constructor::<_, Vec<i32>>("cols", Default::default())?
        .register_constructor::<_, Vec<i32>>("a", Default::default())?
        .start_subarray()?
        .add_range("rows", &["a", "c"])?
        .add_range("cols", &[2, 4])?
        .finish_subarray()?
        .build();

    let (a, (cols, (rows, ()))) = query.execute()?;

    for (row, col, a) in izip!(rows, cols, a) {
        println!("Cell ({} {}) has data {}", row, col, a);
    }
    println!();

    Ok(())
}

fn create_array(ctx: &Context) -> TileDBResult<()> {
    let schema = SchemaData {
        array_type: ArrayType::Sparse,
        domain: DomainData {
            dimension: vec![
                DimensionData {
                    name: "rows".to_owned(),
                    datatype: Datatype::StringAscii,
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
    let row_data = vec!["a", "bb", "c"];
    let col_data = vec![1, 4, 3];
    let a_data = vec![1, 2, 3];

    let array =
        tiledb::Array::open(ctx, ARRAY_URI, tiledb::array::Mode::Write)?;

    let query = WriteBuilder::new(array)?
        .layout(CellOrder::Unordered)?
        .data_typed("rows", &row_data)?
        .data_typed("cols", &col_data)?
        .data_typed("a", &a_data)?
        .build();

    query.submit().and_then(|_| query.finalize())?;

    Ok(())
}
