extern crate tiledb;

use itertools::izip;
use serde_json::json;

use tiledb::array::{
    Array, ArrayType, AttributeData, CellOrder, DimensionData, DomainData,
    SchemaData, TileOrder,
};
use tiledb::context::Context;
use tiledb::datatype::Datatype;
use tiledb::query::buffer::{
    AllocatedWriteBuffer, ReadBufferCollection, WriteBufferCollection,
};
use tiledb::query::read::{
    ReadQueryBuilder, ReadQueryField as RQField, ReadQueryFieldAccessor,
    ReadQueryFieldAsIterator, ReadQueryFieldAsStringIterator,
};
use tiledb::query::traits::QueryBuilder;
use tiledb::query::write::WriteQueryBuilder;
use tiledb::{Factory, Result as TileDBResult};

const ARRAY_URI: &str = "quickstart_sparse_string_array";

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
        .add_range("rows", &["a", "c"])?
        .add_range("cols", &[2, 4])?
        .finish_subarray()?
        .build();

    let rows_data = vec![0u8; 4].into_boxed_slice();
    let rows_offsets = vec![0u64; 3].into_boxed_slice();
    let cols_data = vec![0i32; 3].into_boxed_slice();
    let attr_data = vec![0i32; 3].into_boxed_slice();

    let buffers = ReadBufferCollection::new();
    buffers
        .borrow_mut()
        .add_buffer("rows", (rows_data, rows_offsets))?
        .add_buffer("cols", cols_data)?
        .add_buffer("attr", attr_data)?;

    let result = query.submit(&buffers)?;
    assert!(result.completed());

    let slices = result.slices()?;
    let rows: RQField<u8> = slices.field("rows")?;
    let cols: RQField<i32> = slices.field("cols")?;
    let attrs: RQField<i32> = slices.field("attr")?;

    for (row, col, attr) in
        izip!(rows.lossy_str_iter()?, cols.iter()?, attrs.iter()?)
    {
        println!("Cell ({}, {}) has attr {}", row, col, attr);
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
    let row_data = vec!["a", "bb", "c"];
    let row_data = AllocatedWriteBuffer::from(&row_data);
    let col_data = vec![1, 4, 3];
    let attr_data = vec![1, 2, 3];

    let array =
        tiledb::Array::open(ctx, ARRAY_URI, tiledb::array::Mode::Write)?;

    let mut query = WriteQueryBuilder::new(array)?
        .layout(CellOrder::Unordered)?
        .build();

    let buffers = WriteBufferCollection::new()
        .add_buffer("rows", &row_data)?
        .add_buffer("cols", col_data.as_slice())?
        .add_buffer("attr", attr_data.as_slice())?;

    let _ = query.submit(&buffers)?;
    query.finalize()?;

    Ok(())
}
