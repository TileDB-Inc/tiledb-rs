extern crate tiledb;

use tiledb::array::{CellOrder, TileOrder};
use tiledb::query::{QueryBuilder, ReadBuilder, ReadQuery, ReadQueryBuilder};
use tiledb::Datatype;
use tiledb::Result as TileDBResult;

const ARRAY_NAME: &str = "reading_incomplete_array";

const INT32_ATTRIBUTE_NAME: &str = "a1";
const CHAR_ATTRIBUTE_NAME: &str = "a2";

/// Returns whether the example array already exists
fn array_exists() -> bool {
    let tdb = match tiledb::Context::new() {
        Err(_) => return false,
        Ok(tdb) => tdb,
    };

    tiledb::array::Array::exists(&tdb, ARRAY_NAME)
        .expect("Error checking array existence")
}

/// Creates a dense array at URI `ARRAY_NAME`.
/// The array has two i32 dimensions ["rows", "columns"] with two
/// attributes in each cell - (a1 INT32, a2 CHAR).
/// Both "rows" and "columns" dimensions range from 1 to 4, and the tiles
/// span all 4 elements on each dimension.
/// Hence we have 16 cells of data and a single tile for the whole array.
fn create_array() -> TileDBResult<()> {
    let tdb = tiledb::Context::new()?;

    let domain = {
        let rows: tiledb::array::Dimension =
            tiledb::array::DimensionBuilder::new::<i32>(
                &tdb,
                "rows",
                Datatype::Int32,
                &[1, 4],
                &4,
            )?
            .build();
        let cols: tiledb::array::Dimension =
            tiledb::array::DimensionBuilder::new::<i32>(
                &tdb,
                "columns",
                Datatype::Int32,
                &[1, 4],
                &4,
            )?
            .build();

        tiledb::array::DomainBuilder::new(&tdb)?
            .add_dimension(rows)?
            .add_dimension(cols)?
            .build()
    };

    let attribute_int32 = tiledb::array::AttributeBuilder::new(
        &tdb,
        INT32_ATTRIBUTE_NAME,
        tiledb::Datatype::Int32,
    )?
    .build();

    let attribute_char = tiledb::array::AttributeBuilder::new(
        &tdb,
        CHAR_ATTRIBUTE_NAME,
        tiledb::Datatype::Char,
    )?
    .var_sized()?
    .build();

    let schema = tiledb::array::SchemaBuilder::new(
        &tdb,
        tiledb::array::ArrayType::Sparse,
        domain,
    )?
    .cell_order(CellOrder::RowMajor)?
    .tile_order(TileOrder::RowMajor)?
    .add_attribute(attribute_int32)?
    .add_attribute(attribute_char)?
    .build()?;

    tiledb::Array::create(&tdb, ARRAY_NAME, schema)
}

/// Writes data into the array in row-major order from a 1D-array buffer.
/// After the write, the contents of the array will be:
/// [[ 1,  2,  3,  4],
///  [ 5,  6,  7,  8],
///  [ 9, 10, 11, 12],
///  [13, 14, 15, 16]]
fn write_array() -> TileDBResult<()> {
    let tdb = tiledb::Context::new()?;

    let array =
        tiledb::Array::open(&tdb, ARRAY_NAME, tiledb::array::Mode::Write)?;

    let coords_rows = vec![1, 2, 2];
    let coords_cols = vec![1, 1, 2];

    let int32_data = vec![1, 2, 3];
    let char_data = vec!["a", "bb", "ccc"];

    let query = tiledb::query::WriteBuilder::new(&tdb, array)?
        .layout(tiledb::query::QueryLayout::Global)?
        .data_typed("rows", &coords_rows)?
        .data_typed("columns", &coords_cols)?
        .data_typed(INT32_ATTRIBUTE_NAME, &int32_data)?
        .data_typed(CHAR_ATTRIBUTE_NAME, &char_data)?
        .build();

    query.submit()
}

/// The goal of this is example is to demonstrate handling incomplete results
/// from a query.  The example wants to print out the query result set.
/// Below are several different ways to implement this functionality.

fn query_builder_start(tdb: &tiledb::Context) -> TileDBResult<ReadBuilder> {
    let array =
        tiledb::Array::open(tdb, ARRAY_NAME, tiledb::array::Mode::Read)?;

    tiledb::query::ReadBuilder::new(tdb, array)?
        .layout(tiledb::query::QueryLayout::RowMajor)?
        .add_subarray()?
        .dimension_range_typed::<i32, _>(0, &[1, 4])?
        .add_subarray()?
        .dimension_range_typed::<i32, _>(1, &[1, 4])
}

/// Handles the incomplete results manually, as might be done with the C API.
/// Buffers are provided for the query to fill in. Each step fills in as much
/// as it can, we print the results and then re-submit the query.
fn read_array_step() -> TileDBResult<()> {
    let tdb = tiledb::context::Context::new()?;

    query_builder_start(&tdb)?;

    unimplemented!()
}

/// Ignores the details of incomplete results by collecting them into a result set
/// and then printing the result set.
fn read_array_collect() -> TileDBResult<()> {
    println!("read_array_collect");

    let tdb = tiledb::context::Context::new()?;

    let mut qq = query_builder_start(&tdb)?
        .add_result::<_, Vec<i32>>("rows")?
        .add_result::<_, Vec<i32>>("columns")?
        .add_result::<_, Vec<i32>>(INT32_ATTRIBUTE_NAME)?
        .add_result::<_, Vec<String>>(CHAR_ATTRIBUTE_NAME)?
        .build();

    let (row, (column, (a1, (a2, _)))) = qq.execute()?;
    for (((row, column), a1), a2) in row.iter().zip(column).zip(a1).zip(a2) {
        println!("Cell ({}, {}) a1: {}, a2: {}", row, column, a1, a2)
    }

    Ok(())
}

fn main() -> TileDBResult<()> {
    if !array_exists() {
        create_array().expect("Failed to create array");
    }
    write_array().expect("Failed to write array");
    read_array_collect().expect("Failed to collect array results");
    Ok(())
}
