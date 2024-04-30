extern crate tiledb;

use itertools::izip;

use tiledb::array::Array;
use tiledb::context::Context;
use tiledb::query::buffer::{ReadBufferCollection, WriteBufferCollection};
use tiledb::query::read::{
    ReadQueryBuilder, ReadQueryField as RQField, ReadQueryFieldAccessor,
    ReadQueryFieldAsIterator,
};
use tiledb::query::traits::QueryBuilder;
use tiledb::query::write::WriteQueryBuilder;
use tiledb::Datatype;
use tiledb::Result as TileDBResult;

const ARRAY_URI: &str = "quickstart_dense_array";

/// Returns whether the example array already exists
fn array_exists(ctx: &Context) -> bool {
    Array::exists(ctx, ARRAY_URI).expect("Error checking array existence")
}

/// Creates a dense array at URI `QUICKSTART_DENSE_ARRAY_URI()`.
/// The array has two i32 dimensions ["rows", "columns"] with a single int32
/// attribute "a" stored in each cell.
/// Both "rows" and "columns" dimensions range from 1 to 4, and the tiles
/// span all 4 elements on each dimension.
/// Hence we have 16 cells of data and a single tile for the whole array.
fn create_array(ctx: &Context) -> TileDBResult<()> {
    let domain = {
        let rows: tiledb::array::Dimension =
            tiledb::array::DimensionBuilder::new::<i32>(
                ctx,
                "rows",
                Datatype::Int32,
                &[1, 4],
                &4,
            )?
            .build();

        let cols: tiledb::array::Dimension =
            tiledb::array::DimensionBuilder::new::<i32>(
                ctx,
                "columns",
                Datatype::Int32,
                &[1, 4],
                &4,
            )?
            .build();

        tiledb::array::DomainBuilder::new(ctx)?
            .add_dimension(rows)?
            .add_dimension(cols)?
            .build()
    };

    let attribute_a = tiledb::array::AttributeBuilder::new(
        ctx,
        "attr",
        tiledb::Datatype::Int32,
    )?
    .build();

    let schema = tiledb::array::SchemaBuilder::new(
        ctx,
        tiledb::array::ArrayType::Dense,
        domain,
    )?
    .add_attribute(attribute_a)?
    .build()?;

    tiledb::Array::create(ctx, ARRAY_URI, schema)
}

/// Writes data into the array in row-major order from a 1D-array buffer.
/// After the write, the contents of the array will be:
/// [[ 1,  2,  3,  4],
///  [ 5,  6,  7,  8],
///  [ 9, 10, 11, 12],
///  [13, 14, 15, 16]]
fn write_array(ctx: &Context) -> TileDBResult<()> {
    let array =
        tiledb::Array::open(ctx, ARRAY_URI, tiledb::array::Mode::Write)?;

    let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

    let mut query = WriteQueryBuilder::new(array)?
        .layout(tiledb::query::QueryLayout::RowMajor)?
        .build();

    let buffers =
        WriteBufferCollection::new().add_buffer("attr", data.as_slice())?;

    let _ = query.submit(&buffers)?;
    query.finalize()?;

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
fn read_array(ctx: &Context) -> TileDBResult<()> {
    let array = tiledb::Array::open(ctx, ARRAY_URI, tiledb::array::Mode::Read)?;

    let mut query = ReadQueryBuilder::new(array)?
        .layout(tiledb::query::QueryLayout::RowMajor)?
        .start_subarray()?
        .add_range("rows", &[1i32, 2])?
        .add_range("columns", &[2i32, 4])?
        .finish_subarray()?
        .build();

    let rows_data = vec![0i32; 16].into_boxed_slice();
    let cols_data = vec![0i32; 16].into_boxed_slice();
    let attr_data = vec![0i32; 16].into_boxed_slice();

    let buffers = ReadBufferCollection::new();
    buffers
        .borrow_mut()
        .add_buffer("rows", rows_data)?
        .add_buffer("columns", cols_data)?
        .add_buffer("attr", attr_data)?;

    let result = query.submit(&buffers)?;
    assert!(result.completed());

    let slices = result.slices()?;
    let row_values: RQField<i32> = slices.field("rows")?;
    let col_values: RQField<i32> = slices.field("columns")?;
    let attr_values: RQField<i32> = slices.field("attr")?;

    for (row, col, attr) in
        izip!(row_values.iter()?, col_values.iter()?, attr_values.iter()?)
    {
        println!("{} {} = {}", row, col, attr);
    }

    Ok(println!())
}

fn main() -> TileDBResult<()> {
    let ctx = Context::new()?;
    if !array_exists(&ctx) {
        create_array(&ctx).expect("Failed to create array");
        write_array(&ctx).expect("Failed to write array");
    }

    read_array(&ctx).expect("Failed to read array");

    Ok(())
}
