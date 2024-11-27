use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Array as ArrowArray, Int32Array};
use itertools::izip;

use tiledb_api::array::{
    Array, AttributeBuilder, Dimension, DimensionBuilder, DomainBuilder,
    SchemaBuilder,
};
use tiledb_api::context::Context;
use tiledb_api::Result as TileDBResult;
use tiledb_common::array::{ArrayType, Mode};
use tiledb_common::Datatype;
use tiledb_query_core::{QueryBuilder, QueryLayout, QueryType};

const QUICKSTART_DENSE_ARRAY_URI: &str = "quickstart_dense";
const QUICKSTART_ATTRIBUTE_NAME: &str = "a";

/// Returns whether the example array already exists
fn array_exists() -> bool {
    let tdb = match Context::new() {
        Err(_) => return false,
        Ok(tdb) => tdb,
    };

    Array::exists(&tdb, QUICKSTART_DENSE_ARRAY_URI)
        .expect("Error checking array existence")
}

/// Creates a dense array at URI `QUICKSTART_DENSE_ARRAY_URI()`.
/// The array has two i32 dimensions ["rows", "columns"] with a single int32
/// attribute "a" stored in each cell.
/// Both "rows" and "columns" dimensions range from 1 to 4, and the tiles
/// span all 4 elements on each dimension.
/// Hence we have 16 cells of data and a single tile for the whole array.
fn create_array() -> TileDBResult<()> {
    let tdb = Context::new()?;

    let domain = {
        let rows: Dimension =
            DimensionBuilder::new(&tdb, "rows", Datatype::Int32, ([1, 4], 4))?
                .build();
        let cols: Dimension = DimensionBuilder::new(
            &tdb,
            "columns",
            Datatype::Int32,
            ([1, 4], 4),
        )?
        .build();

        DomainBuilder::new(&tdb)?
            .add_dimension(rows)?
            .add_dimension(cols)?
            .build()
    };

    let attribute_a = AttributeBuilder::new(
        &tdb,
        QUICKSTART_ATTRIBUTE_NAME,
        Datatype::Int32,
    )?
    .build();

    let schema = SchemaBuilder::new(&tdb, ArrayType::Dense, domain)?
        .add_attribute(attribute_a)?
        .build()?;

    Array::create(&tdb, QUICKSTART_DENSE_ARRAY_URI, schema)
}

/// Writes data into the array in row-major order from a 1D-array buffer.
/// After the write, the contents of the array will be:
/// [[ 1,  2,  3,  4],
///  [ 5,  6,  7,  8],
///  [ 9, 10, 11, 12],
///  [13, 14, 15, 16]]
fn write_array() -> TileDBResult<()> {
    let tdb = Context::new()?;

    let array = Array::open(&tdb, QUICKSTART_DENSE_ARRAY_URI, Mode::Write)?;

    let data: Arc<dyn ArrowArray> = Arc::new(Int32Array::from(vec![
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
    ]));

    let mut query = QueryBuilder::new(array, QueryType::Write)
        .with_layout(QueryLayout::RowMajor)
        .start_fields()
        .field_with_buffer(QUICKSTART_ATTRIBUTE_NAME, data)
        .end_fields()
        .build()?;

    query.submit().and_then(|_| query.finalize())?;

    Ok(())
}

/// Query back a slice of our array and print the results to stdout.
/// The slice on "rows" is [1, 2] and on "columns" is [2, 4],
/// so the returned data should look like:
/// [[ _,  2,  3,  4],
///  [ _,  6,  7,  8],
///  [ _,  _,  _,  _],
///  [ _,  _,  _,  _]]]
/// Data is emitted in row-major order, so this will print "2 3 4 6 7 8".
fn read_array() -> TileDBResult<()> {
    let tdb = Context::new()?;

    let array = Array::open(&tdb, QUICKSTART_DENSE_ARRAY_URI, Mode::Read)?;

    let mut query = QueryBuilder::new(array, QueryType::Read)
        .with_layout(QueryLayout::RowMajor)
        .start_fields()
        .field("rows")
        .field("columns")
        .field(QUICKSTART_ATTRIBUTE_NAME)
        .end_fields()
        .start_subarray()
        .add_range("rows", &[1i32, 2])
        .add_range("columns", &[2i32, 4])
        .end_subarray()
        .build()?;

    let status = query.submit()?;
    assert!(status.is_complete());

    let buffers = query.buffers()?;
    let rows = buffers.get::<Int32Array>("rows").unwrap();
    let cols = buffers.get::<Int32Array>("columns").unwrap();
    let attrs = buffers
        .get::<Int32Array>(QUICKSTART_ATTRIBUTE_NAME)
        .unwrap();

    for (row, col, attr) in izip!(rows.values(), cols.values(), attrs.values())
    {
        println!("{} {} {}", row, col, attr);
    }

    Ok(())
}

fn main() {
    if let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR") {
        let _ = std::env::set_current_dir(
            PathBuf::from(manifest_dir).join("examples").join("output"),
        );
    }

    if !array_exists() {
        create_array().expect("Failed to create array");
    }
    write_array().expect("Failed to write array");
    read_array().expect("Failed to read array");
}
