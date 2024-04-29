extern crate tiledb;

use itertools::izip;
use serde_json::json;

use tiledb::array::{
    Array, ArrayType, AttributeData, CellOrder, DimensionData, DomainData,
    SchemaData, TileOrder,
};
use tiledb::context::Context;
use tiledb::query::buffer::{ReadBufferCollection, WriteBufferCollection};
use tiledb::query::read::{
    ReadQueryBuilder, ReadQueryField as RQField, ReadQueryFieldAccessor,
    ReadQueryFieldAsIterator,
};
use tiledb::query::traits::QueryBuilder;
use tiledb::query::write::WriteQueryBuilder;
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
    let mut query = ReadQueryBuilder::new(array)?
        .layout(tiledb::query::QueryLayout::RowMajor)?
        .start_subarray()?
        .add_range("rows", &[1, 2])?
        .add_range("rows", &[4, 4])?
        .add_range("cols", &[1, 4])?
        .finish_subarray()?
        .build();

    let rows_data = vec![0i32; 16].into_boxed_slice();
    let cols_data = vec![0i32; 16].into_boxed_slice();
    let a_data = vec![0i32; 16].into_boxed_slice();

    let buffers = ReadBufferCollection::new();
    buffers
        .borrow_mut()
        .add_buffer("rows", rows_data)?
        .add_buffer("cols", cols_data)?
        .add_buffer("attr", a_data)?;

    let result = query.submit(&buffers)?;
    assert!(result.completed());

    let slices = result.slices()?;
    let rows: RQField<i32> = slices.field("rows")?;
    let cols: RQField<i32> = slices.field("cols")?;
    let attrs: RQField<i32> = slices.field("attr")?;

    for (row, col, attr) in izip!(rows.iter()?, cols.iter()?, attrs.iter()?) {
        println!("{} {} {}", row, col, attr);
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
            name: "attr".to_owned(),
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

    let mut query = WriteQueryBuilder::new(array)?
        .layout(CellOrder::RowMajor)?
        .build();

    let buffers =
        WriteBufferCollection::new().add_buffer("attr", data.as_slice())?;

    let _ = query.submit(&buffers)?;
    query.finalize()?;

    Ok(())
}
