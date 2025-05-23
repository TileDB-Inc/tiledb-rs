extern crate tiledb_api as tiledb;

use std::path::PathBuf;

use tiledb::Datatype;
use tiledb::Result as TileDBResult;
use tiledb::query::{QueryBuilder, ReadQuery, ReadQueryBuilder};

const QUICKSTART_DENSE_ARRAY_URI: &str = "quickstart_dense";
const QUICKSTART_ATTRIBUTE_NAME: &str = "a";

/// Returns whether the example array already exists
fn array_exists() -> bool {
    let tdb = match tiledb::context::Context::new() {
        Err(_) => return false,
        Ok(tdb) => tdb,
    };

    tiledb::array::Array::exists(&tdb, QUICKSTART_DENSE_ARRAY_URI)
        .expect("Error checking array existence")
}

/// Creates a dense array at URI `QUICKSTART_DENSE_ARRAY_URI()`.
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
        QUICKSTART_ATTRIBUTE_NAME,
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

    tiledb::Array::create(&tdb, QUICKSTART_DENSE_ARRAY_URI, schema)
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
        QUICKSTART_DENSE_ARRAY_URI,
        tiledb::array::Mode::Write,
    )?;

    let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

    let query = tiledb::query::WriteBuilder::new(array)?
        .layout(tiledb::query::QueryLayout::RowMajor)?
        .data_typed(QUICKSTART_ATTRIBUTE_NAME, &data)?
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
/// Data is emitted in row-major order, so this will print "2 3 4 6 7 8".
fn read_array() -> TileDBResult<()> {
    let tdb = tiledb::context::Context::new()?;

    let array = tiledb::Array::open(
        &tdb,
        QUICKSTART_DENSE_ARRAY_URI,
        tiledb::array::Mode::Read,
    )?;

    let mut query = tiledb::query::ReadBuilder::new(array)?
        .layout(tiledb::query::QueryLayout::RowMajor)?
        .register_constructor::<_, Vec<i32>>(
            QUICKSTART_ATTRIBUTE_NAME,
            Default::default(),
        )?
        .start_subarray()?
        .add_range("rows", &[1i32, 2])?
        .add_range("columns", &[2i32, 4])?
        .finish_subarray()?
        .build();

    let (results, _) = query.execute()?;

    for value in results {
        print!("{value} ")
    }
    println!();
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
