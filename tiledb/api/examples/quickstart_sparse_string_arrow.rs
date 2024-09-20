use std::path::PathBuf;
use std::sync::Arc;

use arrow::array as aa;
use itertools::izip;

use tiledb::array::dimension::DimensionConstraints;
use tiledb::array::{
    Array, ArrayType, AttributeData, CellOrder, DimensionData, DomainData,
    SchemaData, TileOrder,
};
use tiledb::context::Context;
use tiledb::error::Error as TileDBError;
use tiledb::query_arrow::{QueryBuilder, QueryLayout, QueryStatus, QueryType};
use tiledb::Result as TileDBResult;
use tiledb::{Datatype, Factory};

const ARRAY_URI: &str = "quickstart_sparse_string";

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

    let array = Array::open(&ctx, ARRAY_URI, tiledb::array::Mode::Read)?;
    let mut query = QueryBuilder::new(array, QueryType::Read)
        .with_layout(QueryLayout::RowMajor)
        .start_fields()
        .field("rows")
        .field("cols")
        .field("a")
        .end_fields()
        .start_subarray()
        .add_range("rows", &["a", "c"])
        .add_range("cols", &[2, 4])
        .end_subarray()
        .build()
        .map_err(|e| TileDBError::Other(format!("{}", e)))?;

    let status = query
        .submit()
        .map_err(|e| TileDBError::Other(format!("{}", e)))?;

    if !matches!(status, QueryStatus::Completed) {
        return Err(TileDBError::Other("Make this better.".to_string()));
    }

    let buffers = query
        .buffers()
        .map_err(|e| TileDBError::Other(format!("{}", e)))?;

    let rows = buffers.get::<aa::LargeStringArray>("rows").unwrap();
    let cols = buffers.get::<aa::Int32Array>("cols").unwrap();
    let attr = buffers.get::<aa::Int32Array>("a").unwrap();

    for (row, col, attr) in izip!(rows, cols.values(), attr.values()) {
        println!("{} {} {}", row.unwrap(), col, attr);
    }

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
                    constraints: DimensionConstraints::StringAscii,
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
    let row_data = Arc::new(aa::LargeStringArray::from(vec!["a", "bb", "c"]));
    let col_data = Arc::new(aa::Int32Array::from(vec![1, 4, 3]));
    let a_data = Arc::new(aa::Int32Array::from(vec![1, 2, 3]));

    let array =
        tiledb::Array::open(ctx, ARRAY_URI, tiledb::array::Mode::Write)?;

    let mut query = QueryBuilder::new(array, QueryType::Write)
        .with_layout(CellOrder::Unordered)
        .start_fields()
        .field_with_buffer("rows", row_data)
        .field_with_buffer("cols", col_data)
        .field_with_buffer("a", a_data)
        .end_fields()
        .build()
        .map_err(|e| TileDBError::Other(format!("{}", e)))?;

    let status = query
        .submit()
        .map_err(|e| TileDBError::Other(format!("{}", e)))?;

    if !matches!(status, QueryStatus::Completed) {
        return Err(TileDBError::Other("Make this better.".to_string()));
    }

    let (_, _) = query
        .finalize()
        .map_err(|e| TileDBError::Other(format!("{e}")))?;

    Ok(())
}
