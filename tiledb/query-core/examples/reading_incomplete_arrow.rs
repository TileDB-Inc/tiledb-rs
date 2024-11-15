use std::path::PathBuf;
use std::sync::Arc;

use arrow::array as aa;
use itertools::izip;

use tiledb_api::array::Array;
use tiledb_api::context::Context;
use tiledb_api::{Factory, Result as TileDBResult};
use tiledb_common::array::{ArrayType, CellOrder, CellValNum, Mode, TileOrder};
use tiledb_common::Datatype;
use tiledb_pod::array::{AttributeData, DimensionData, DomainData, SchemaData};
use tiledb_query_core::buffers::Error as BuffersError;
use tiledb_query_core::fields::QueryFieldsBuilder;
use tiledb_query_core::{
    Error as QueryError, QueryBuilder, QueryLayout, QueryType, SharedBuffers,
};

const ARRAY_URI: &str = "reading_incomplete";

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
    read_array(&ctx)?;
    Ok(())
}

/// Creates a dense array at URI `ARRAY_NAME`.
/// The array has two i32 dimensions ["rows", "columns"] with two
/// attributes in each cell - (a1 INT32, a2 CHAR).
/// Both "rows" and "columns" dimensions range from 1 to 4, and the tiles
/// span all 4 elements on each dimension.
/// Hence we have 16 cells of data and a single tile for the whole array.
fn create_array(ctx: &Context) -> TileDBResult<()> {
    let schema = SchemaData {
        array_type: ArrayType::Sparse,
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
        attributes: vec![
            AttributeData {
                name: "a1".to_string(),
                datatype: Datatype::Int32,
                ..Default::default()
            },
            AttributeData {
                name: "a2".to_string(),
                datatype: Datatype::StringUtf8,
                cell_val_num: Some(CellValNum::Var),
                ..Default::default()
            },
        ],
        tile_order: Some(TileOrder::RowMajor),
        cell_order: Some(CellOrder::RowMajor),

        ..Default::default()
    };

    let schema = schema.create(ctx)?;
    Array::create(ctx, ARRAY_URI, schema)?;
    Ok(())
}

/// Writes data into the array.
/// After the write, the contents of the array will be:
/// [[ (1, "a"), (2, "bb"),  _, _],
///  [ _,        (3, "ccc"), _, _],
///  [ _,        _,          _, _],
///  [ _,        _,          _, _]]
fn write_array(ctx: &Context) -> TileDBResult<()> {
    let rows_data = Arc::new(aa::Int32Array::from(vec![1, 2, 2, 2]));
    let cols_data = Arc::new(aa::Int32Array::from(vec![1, 1, 2, 3]));
    let a1_data = Arc::new(aa::Int32Array::from(vec![1, 2, 3, 3]));
    let a2_data =
        Arc::new(aa::LargeStringArray::from(vec!["a", "bb", "ccc", "dddd"]));

    let array = Array::open(&ctx, ARRAY_URI, Mode::Write)?;

    let mut query = QueryBuilder::new(array, QueryType::Write)
        .with_layout(QueryLayout::Global)
        .start_fields()
        .field_with_buffer("rows", rows_data)
        .field_with_buffer("cols", cols_data)
        .field_with_buffer("a1", a1_data)
        .field_with_buffer("a2", a2_data)
        .end_fields()
        .build()?;

    query.submit().and_then(|_| query.finalize())?;
    Ok(())
}

fn read_array(ctx: &Context) -> TileDBResult<()> {
    let mut curr_capacity = 1;

    let array = Array::open(ctx, ARRAY_URI, Mode::Read)?;

    let make_fields = |capacity| {
        QueryFieldsBuilder::new()
            .field_with_capacity("rows", capacity)
            .field_with_capacity("cols", capacity)
            .field_with_capacity("a1", capacity)
            .field_with_capacity("a2", capacity)
            .build()
    };

    let mut query = QueryBuilder::new(array, QueryType::Read)
        .with_layout(QueryLayout::RowMajor)
        .with_fields(make_fields(curr_capacity))
        .start_subarray()
        .add_range("rows", &[1i32, 4])
        .add_range("cols", &[1i32, 4])
        .end_subarray()
        .build()?;

    let mut external_ref: Option<SharedBuffers> = None;

    loop {
        let result = query.submit();

        if result.is_err() {
            let err = result.err().unwrap();
            println!("ERROR: {:?}", err);

            if matches!(
                err,
                QueryError::QueryBuffersError(BuffersError::ArrayInUse)
            ) {
                drop(external_ref.take());
                continue;
            }

            return Err(err.into());
        }

        let status = result.ok().unwrap();

        // Double our buffer sizes if we didn't manage to get any data out
        // of the query.
        if !status.has_data() {
            println!(
                "Doubling buffer capacity: {} to {}",
                curr_capacity,
                curr_capacity * 2
            );
            curr_capacity = curr_capacity * 2;
            query.replace_buffers(make_fields(curr_capacity))?;
            continue;
        }

        // Print any results we did get.
        let buffers = query.buffers()?;

        // Simulate what happens if the client doesn't let go of their
        // SharedBuffers reference.
        external_ref = Some(buffers.clone());

        let rows = buffers.get::<aa::Int32Array>("rows").unwrap();
        let cols = buffers.get::<aa::Int32Array>("cols").unwrap();
        let a1 = buffers.get::<aa::Int32Array>("a1").unwrap();
        let a2 = buffers.get::<aa::LargeStringArray>("a2").unwrap();

        for (r, c, a1, a2) in izip!(rows, cols, a1, a2) {
            println!(
                "\tCell ({}, {}) a1: {}, a2: {}",
                r.unwrap(),
                c.unwrap(),
                a1.unwrap(),
                a2.unwrap()
            );
        }

        // Break from the loop when completed.
        if status.is_complete() {
            break;
        }
    }

    Ok(())
}
