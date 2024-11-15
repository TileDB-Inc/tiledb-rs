use std::path::PathBuf;
use std::sync::Arc;

use arrow::array as aa;
use itertools::izip;

use tiledb_api::array::Array;
use tiledb_api::context::Context;
use tiledb_api::{Factory, Result as TileDBResult};
use tiledb_common::array::{ArrayType, CellOrder, Mode, TileOrder};
use tiledb_common::Datatype;
use tiledb_pod::array::{AttributeData, DimensionData, DomainData, SchemaData};
use tiledb_query_core::{QueryBuilder, QueryLayout, QueryType};

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
    if Array::exists(&ctx, ARRAY_URI)? {
        Array::delete(&ctx, ARRAY_URI)?;
    }

    create_array(&ctx)?;
    write_array(&ctx)?;

    let array = Array::open(&ctx, ARRAY_URI, Mode::Read)?;
    let mut query = QueryBuilder::new(array, QueryType::Read)
        .with_layout(QueryLayout::RowMajor)
        .start_fields()
        .field("rows")
        .field("cols")
        .field("a")
        .end_fields()
        .start_subarray()
        .add_range("rows", &[1, 2])
        .add_range("rows", &[4, 4])
        .add_range("cols", &[1, 4])
        .end_subarray()
        .build()?;

    let status = query.submit()?;
    assert!(status.is_complete());

    let buffers = query.buffers()?;

    let rows = buffers.get::<aa::Int32Array>("rows").unwrap();
    let cols = buffers.get::<aa::Int32Array>("cols").unwrap();
    let attr = buffers.get::<aa::Int32Array>("a").unwrap();

    for (r, c, a) in izip!(rows.values(), cols.values(), attr.values()) {
        println!("{} {} {}", r, c, a);
    }

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
                    constraints: ([1i32, 4], 4i32).into(),
                    filters: None,
                },
                DimensionData {
                    name: "cols".to_owned(),
                    datatype: Datatype::Int32,
                    constraints: ([1i32, 4], 4i32).into(),
                    filters: None,
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
    let data = Arc::new(aa::Int32Array::from(vec![
        1i32, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
    ]));

    let array = Array::open(ctx, ARRAY_URI, Mode::Write)?;

    let mut query = QueryBuilder::new(array, QueryType::Write)
        .with_layout(QueryLayout::RowMajor)
        .start_fields()
        .field_with_buffer("a", data)
        .end_fields()
        .build()?;

    let (_, _) = query.submit().and_then(|_| query.finalize())?;

    Ok(())
}
