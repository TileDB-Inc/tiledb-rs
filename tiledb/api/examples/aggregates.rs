extern crate tiledb;

use crate::tiledb::query::read::AggregateQueryBuilder;
use std::path::PathBuf;
use tiledb::query::{QueryBuilder, ReadQuery};
use tiledb::Datatype;
use tiledb::Result as TileDBResult;

const AGGREGATE_ARRAY_URI: &str = "aggregates";
const AGGREGATE_ATTRIBUTE_NAME: &str = "a";

/// This example creates a dense 4x4 array with the contents:
///
/// [[ 1,  2,  3,  4],
///  [ 5,  6,  7,  8],
///  [ 9, 10, 11, 12],
///  [13, 14, 15, 16]]
///
/// and runs some aggregate functions over
/// various subarrays within the data.
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

    example_count().expect("Failed to count array");
    example_sum().expect("Failed to sum array");
    example_min_max().expect("Failed to min/max array");
    example_mean().expect("Failed to get mean of array.");
}

/// Returns whether the example array already exists
fn array_exists() -> bool {
    let tdb = match tiledb::context::Context::new() {
        Err(_) => return false,
        Ok(tdb) => tdb,
    };

    tiledb::array::Array::exists(&tdb, AGGREGATE_ARRAY_URI)
        .expect("Error checking array existence")
}

/// Creates a dense array at URI `AGGREGATE_ARRAY_URI()`.
/// The array has two i32 dimensions ["rows", "columns"] with a single int32
/// attribute "a" stored in each cell.
/// Both "rows" and "columns" dimensions range from 1 to 4, and the tiles
/// span all 4 elements on each dimension.
/// Hence we have 16 cells of data and a single tile for the whole array.
fn create_array() -> TileDBResult<()> {
    let tdb = tiledb::context::Context::new()?;

    let domain = {
        let rows: tiledb::array::Dimension =
            tiledb::array::DimensionBuilder::new(
                &tdb,
                "rows",
                Datatype::Int32,
                ([1, 4], 4),
            )?
            .build();
        let cols: tiledb::array::Dimension =
            tiledb::array::DimensionBuilder::new(
                &tdb,
                "columns",
                Datatype::Int32,
                ([1, 4], 4),
            )?
            .build();

        tiledb::array::DomainBuilder::new(&tdb)?
            .add_dimension(rows)?
            .add_dimension(cols)?
            .build()
    };

    let attribute_a = tiledb::array::AttributeBuilder::new(
        &tdb,
        AGGREGATE_ATTRIBUTE_NAME,
        tiledb::Datatype::Int32,
    )?
    .build();

    let schema = tiledb::array::SchemaBuilder::new(
        &tdb,
        tiledb::array::ArrayType::Dense,
        domain,
    )?
    .add_attribute(attribute_a)?
    .build()?;

    tiledb::Array::create(&tdb, AGGREGATE_ARRAY_URI, schema)
}

/// Writes data into the array in row-major order from a 1D-array buffer.
/// After the write, the contents of the array will be:
/// [[ 1,  2,  3,  4],
///  [ 5,  6,  7,  8],
///  [ 9, 10, 11, 12],
///  [13, 14, 15, 16]]
fn write_array() -> TileDBResult<()> {
    let tdb = tiledb::context::Context::new()?;

    let array = tiledb::Array::open(
        &tdb,
        AGGREGATE_ARRAY_URI,
        tiledb::array::Mode::Write,
    )?;

    let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

    let query = tiledb::query::WriteBuilder::new(array)?
        .layout(tiledb::query::QueryLayout::RowMajor)?
        .data_typed(AGGREGATE_ATTRIBUTE_NAME, &data)?
        .build();

    query.submit().map(|_| ())
}

/// Query back a slice of our array and print the results to stdout.
/// The slice on "rows" is [1, 2] and on "columns" is [2, 4],
/// so the returned data should look like:
/// [[ _,  2,  3,  4],
///  [ _,  6,  7,  8],
///  [ _,  _,  _,  _],
///  [ _,  _,  _,  _]]]
/// This should print 6, which is the number of elements in the slice.
fn example_count() -> TileDBResult<()> {
    let tdb = tiledb::context::Context::new()?;

    let array = tiledb::Array::open(
        &tdb,
        AGGREGATE_ARRAY_URI,
        tiledb::array::Mode::Read,
    )?;

    let mut query = tiledb::query::ReadBuilder::new(array)?
        .layout(tiledb::query::QueryLayout::RowMajor)?
        .count()?
        .start_subarray()?
        .add_range("rows", &[1i32, 2])?
        .add_range("columns", &[2i32, 4])?
        .finish_subarray()?
        .build();

    let (count, _): (Option<u64>, ()) = query.execute()?;

    let Some(count) = count else {
        unreachable!("Count result is never `None`");
    };
    println!("Count is {}", count);

    Ok(())
}

/// Query back a slice of our array and print the results to stdout.
/// The slice on "rows" is [1, 2] and on "columns" is [1, 4],
/// so the returned data should look like:
/// [[ 1,  2,  3,  4],
///  [ 5,  6,  7,  8],
///  [ _,  _,  _,  _],
///  [ _,  _,  _,  _]]]
/// This should print 36, which is the sum of elements in the slice.
fn example_sum() -> TileDBResult<()> {
    let tdb = tiledb::context::Context::new()?;

    let array = tiledb::Array::open(
        &tdb,
        AGGREGATE_ARRAY_URI,
        tiledb::array::Mode::Read,
    )?;

    let mut query = tiledb::query::ReadBuilder::new(array)?
        .layout(tiledb::query::QueryLayout::RowMajor)?
        .sum(AGGREGATE_ATTRIBUTE_NAME)?
        .start_subarray()?
        .add_range("rows", &[1i32, 2])?
        .add_range("columns", &[1i32, 4])?
        .finish_subarray()?
        .build();

    let (results, _): (Option<i64>, ()) = query.execute()?;

    let Some(sum) = results else {
        unreachable!("Sum is `None` which cannot occur on example input");
    };
    println!("Sum is {}", sum);

    Ok(())
}

/// Query back a slice of our array and print the results to stdout.
/// The slice on "rows" is [2, 3] and on "columns" is [2, 3],
/// so the returned data should look like:
/// [[ _,  _,  _,  _],
///  [ _,  6,  7,  _],
///  [ _,  10,  11,  _],
///  [ _,  _,  _,  _]]]
/// This should print 6 and 11, which are the min and max of the slice.
/// This function also uses the AggregateResultHandle enum to pass the
/// result back.
fn example_min_max() -> TileDBResult<()> {
    let tdb = tiledb::context::Context::new()?;

    let array = tiledb::Array::open(
        &tdb,
        AGGREGATE_ARRAY_URI,
        tiledb::array::Mode::Read,
    )?;

    let mut query = tiledb::query::ReadBuilder::new(array)?
        .layout(tiledb::query::QueryLayout::RowMajor)?
        .max::<i32>(AGGREGATE_ATTRIBUTE_NAME)?
        .min::<i32>(AGGREGATE_ATTRIBUTE_NAME)?
        .start_subarray()?
        .add_range("rows", &[2i32, 3])?
        .add_range("columns", &[2i32, 3])?
        .finish_subarray()?
        .build();

    let (min_res, (max_res, _)) = query.execute()?;

    let (Some(min_res), Some(max_res)) = (min_res, max_res) else {
        unreachable!("Min or max is `None` which cannot occur on example input")
    };
    println!("Min is {}", min_res);
    println!("Max is {}", max_res);

    Ok(())
}

/// Query back a slice of our array and print the results to stdout.
/// The slice on "rows" is [2, 3] and on "columns" is [1, 3],
/// so the returned data should look like:
/// [[ _,  _,  _,  _],
///  [ 5,  6,  7,  _],
///  [ 9,  10,  11,  _],
///  [ _,  _,  _,  _]]]
/// This should print 8, which is the mean of the slice.
fn example_mean() -> TileDBResult<()> {
    let tdb = tiledb::context::Context::new()?;

    let array = tiledb::Array::open(
        &tdb,
        AGGREGATE_ARRAY_URI,
        tiledb::array::Mode::Read,
    )?;

    let mut query = tiledb::query::ReadBuilder::new(array)?
        .layout(tiledb::query::QueryLayout::RowMajor)?
        .mean(AGGREGATE_ATTRIBUTE_NAME)?
        .start_subarray()?
        .add_range("rows", &[2i32, 3])?
        .add_range("columns", &[1i32, 3])?
        .finish_subarray()?
        .build();

    let (mean, ()) = query.execute()?;

    let Some(mean) = mean else {
        unreachable!("Mean is `None` which cannot occur on example input")
    };
    println!("Mean is {}", mean);

    Ok(())
}
