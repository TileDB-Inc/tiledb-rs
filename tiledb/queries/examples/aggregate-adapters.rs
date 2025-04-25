extern crate tiledb_api;
extern crate tiledb_common;
extern crate tiledb_query_adapters;

use std::path::PathBuf;

use tiledb_api::array::{
    Array, AttributeBuilder, Dimension, DimensionBuilder, DomainBuilder,
    SchemaBuilder,
};
use tiledb_api::query::read::AggregateFunction;
use tiledb_api::query::{
    QueryBuilder, QueryLayout, ReadBuilder, ReadQuery, WriteBuilder,
};
use tiledb_api::{Context, Result as TileDBResult};
use tiledb_common::array::{ArrayType, Mode};
use tiledb_common::datatype::{Datatype, PhysicalValue};
use tiledb_query_adapters::AggregateQueryBuilderExt;

const AGGREGATE_ARRAY_URI: &str = "aggregates";
const AGGREGATE_ATTRIBUTE_NAME: &str = "a";

/// This example is the same as `api/examples/aggregates.rs` except
/// it uses the [`PhysicalValue`] adapter instead of querying
/// programmatically-typed result.
///
/// This example runs over a dense 4x4 array with the contents:
///
/// [[ 1,  2,  3,  4],
///  [ 5,  6,  7,  8],
///  [ 9, 10, 11, 12],
///  [13, 14, 15, 16]]
///
/// and runs the same aggregate functions as in `api/examples/aggregates.rs`.
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
    let tdb = match Context::new() {
        Err(_) => return false,
        Ok(tdb) => tdb,
    };

    Array::exists(&tdb, AGGREGATE_ARRAY_URI)
        .expect("Error checking array existence")
}

/// Creates a dense array at URI `AGGREGATE_ARRAY_URI()`.
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

    let attribute_a =
        AttributeBuilder::new(&tdb, AGGREGATE_ATTRIBUTE_NAME, Datatype::Int32)?
            .build();

    let schema = SchemaBuilder::new(&tdb, ArrayType::Dense, domain)?
        .add_attribute(attribute_a)?
        .build()?;

    Array::create(&tdb, AGGREGATE_ARRAY_URI, schema)
}

/// Writes data into the array in row-major order from a 1D-array buffer.
/// After the write, the contents of the array will be:
/// [[ 1,  2,  3,  4],
///  [ 5,  6,  7,  8],
///  [ 9, 10, 11, 12],
///  [13, 14, 15, 16]]
fn write_array() -> TileDBResult<()> {
    let tdb = Context::new()?;

    let array = Array::open(&tdb, AGGREGATE_ARRAY_URI, Mode::Write)?;

    let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

    let query = WriteBuilder::new(array)?
        .layout(QueryLayout::RowMajor)?
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
    let tdb = Context::new()?;

    let array = Array::open(&tdb, AGGREGATE_ARRAY_URI, Mode::Read)?;

    let mut query = ReadBuilder::new(array)?
        .layout(QueryLayout::RowMajor)?
        .aggregate_physical_value(AggregateFunction::Count)?
        .start_subarray()?
        .add_range("rows", &[1i32, 2])?
        .add_range("columns", &[2i32, 4])?
        .finish_subarray()?
        .build();

    let (count, _): (Option<PhysicalValue>, ()) = query.execute()?;

    let Some(count) = count else {
        unreachable!("Count result is never `None`");
    };
    println!("Count is {count}");

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
    let tdb = Context::new()?;

    let array = Array::open(&tdb, AGGREGATE_ARRAY_URI, Mode::Read)?;

    let mut query = ReadBuilder::new(array)?
        .layout(QueryLayout::RowMajor)?
        .aggregate_physical_value(AggregateFunction::Sum(
            AGGREGATE_ATTRIBUTE_NAME.to_owned(),
        ))?
        .start_subarray()?
        .add_range("rows", &[1i32, 2])?
        .add_range("columns", &[1i32, 4])?
        .finish_subarray()?
        .build();

    let (results, _): (Option<PhysicalValue>, ()) = query.execute()?;

    let Some(sum) = results else {
        unreachable!("Sum is `None` which cannot occur on example input");
    };
    println!("Sum is {sum}");

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
    let tdb = Context::new()?;

    let array = Array::open(&tdb, AGGREGATE_ARRAY_URI, Mode::Read)?;

    let mut query = ReadBuilder::new(array)?
        .layout(QueryLayout::RowMajor)?
        .aggregate_physical_value(AggregateFunction::Max(
            AGGREGATE_ATTRIBUTE_NAME.to_owned(),
        ))?
        .aggregate_physical_value(AggregateFunction::Min(
            AGGREGATE_ATTRIBUTE_NAME.to_owned(),
        ))?
        .start_subarray()?
        .add_range("rows", &[2i32, 3])?
        .add_range("columns", &[2i32, 3])?
        .finish_subarray()?
        .build();

    let (min_res, (max_res, _)) = query.execute()?;

    let (Some(min_res), Some(max_res)) = (min_res, max_res) else {
        unreachable!("Min or max is `None` which cannot occur on example input")
    };
    println!("Min is {min_res}");
    println!("Max is {max_res}");

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
    let tdb = Context::new()?;

    let array = Array::open(&tdb, AGGREGATE_ARRAY_URI, Mode::Read)?;

    let mut query = ReadBuilder::new(array)?
        .layout(QueryLayout::RowMajor)?
        .aggregate_physical_value(AggregateFunction::Mean(
            AGGREGATE_ATTRIBUTE_NAME.to_owned(),
        ))?
        .start_subarray()?
        .add_range("rows", &[2i32, 3])?
        .add_range("columns", &[1i32, 3])?
        .finish_subarray()?
        .build();

    let (mean, ()) = query.execute()?;

    let Some(mean) = mean else {
        unreachable!("Mean is `None` which cannot occur on example input")
    };
    println!("Mean is {mean}");

    Ok(())
}
