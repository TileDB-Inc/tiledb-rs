extern crate tiledb;

use itertools::izip;

use tiledb::array::{CellOrder, TileOrder};
use tiledb::query::buffer::{
    AllocatedWriteBuffer, ReadBufferCollection, WriteBufferCollection,
};
use tiledb::query::read::{
    ReadQueryBuilder, ReadQueryField as RQField, ReadQueryFieldAccessor,
    ReadQueryFieldAsIterator, ReadQueryFieldAsStringIterator,
};
use tiledb::query::traits::QueryBuilder;
use tiledb::query::write::WriteQueryBuilder;
use tiledb::Datatype;
use tiledb::Result as TileDBResult;

const ARRAY_NAME: &str = "reading_incomplete_array";

/// Returns whether the example array already exists
fn array_exists() -> bool {
    let tdb = match tiledb::Context::new() {
        Err(_) => return false,
        Ok(tdb) => tdb,
    };

    tiledb::array::Array::exists(&tdb, ARRAY_NAME)
        .expect("Error checking array existence")
}

/// Creates a sparse array at URI `ARRAY_NAME`.
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
        "a1",
        tiledb::Datatype::Int32,
    )?
    .build();

    let attribute_char = tiledb::array::AttributeBuilder::new(
        &tdb,
        "a2",
        tiledb::Datatype::StringAscii,
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

/// Writes data into the array.
/// After the write, the contents of the array will be:
/// [[ (1, "a"), (2, "bb"),  _, _],
///  [ _,        (3, "ccc"), _, _],
///  [ _,        _,          _, _],
///  [ _,        _,          _, _]]
fn write_array() -> TileDBResult<()> {
    let tdb = tiledb::Context::new()?;

    let array =
        tiledb::Array::open(&tdb, ARRAY_NAME, tiledb::array::Mode::Write)?;

    let rows_data = vec![1, 2, 2];
    let cols_data = vec![1, 1, 2];

    let a1_data = vec![1, 2, 3];
    let a2_data = vec!["a", "bb", "ccc"];
    let a2_data = AllocatedWriteBuffer::from(&a2_data);

    let mut query = WriteQueryBuilder::new(array)?
        .layout(tiledb::query::QueryLayout::Global)?
        .build();

    let buffers = WriteBufferCollection::new()
        .add_buffer("rows", rows_data.as_slice())?
        .add_buffer("columns", cols_data.as_slice())?
        .add_buffer("a1", a1_data.as_slice())?
        .add_buffer("a2", &a2_data)?;

    query.submit(&buffers).and_then(|_| query.finalize())?;
    Ok(())
}

fn read_array() -> TileDBResult<()> {
    let ctx = tiledb::context::Context::new()?;

    let array =
        tiledb::Array::open(&ctx, ARRAY_NAME, tiledb::array::Mode::Read)?;

    let mut query = ReadQueryBuilder::new(array)?
        .layout(tiledb::query::QueryLayout::RowMajor)?
        .start_subarray()?
        .add_range(0, &[1i32, 4])?
        .add_range(1, &[1i32, 4])?
        .finish_subarray()?
        .build();

    // Use a small capacity here to force reallocation.
    let mut curr_capacity = 1;
    let rows_data = vec![0i32; curr_capacity].into_boxed_slice();
    let cols_data = vec![0i32; curr_capacity].into_boxed_slice();
    let a1_data = vec![0i32; curr_capacity].into_boxed_slice();
    let a2_data = vec![0u8; curr_capacity].into_boxed_slice();
    let a2_offsets = vec![0u64; curr_capacity].into_boxed_slice();

    let buffers = ReadBufferCollection::new();
    buffers
        .borrow_mut()
        .add_buffer("rows", rows_data)?
        .add_buffer("columns", cols_data)?
        .add_buffer("a1", a1_data)?
        .add_buffer("a2", (a2_data, a2_offsets))?;

    loop {
        let result = query.submit(&buffers)?;

        if result.nresults()? == 0 && result.details().user_buffer_size() {
            // Not enough space in our buffers to make progress so we have
            // to reallocate them with larger storage capacity.
            println!("Reallocating buffers");

            curr_capacity *= 2;
            let rows_data = vec![0i32; curr_capacity].into_boxed_slice();
            let cols_data = vec![0i32; curr_capacity].into_boxed_slice();
            let a1_data = vec![0i32; curr_capacity].into_boxed_slice();
            let a2_data = vec![0u8; curr_capacity].into_boxed_slice();
            let a2_offsets = vec![0u64; curr_capacity].into_boxed_slice();
            buffers
                .borrow_mut()
                .clear()
                .add_buffer("rows", rows_data)?
                .add_buffer("columns", cols_data)?
                .add_buffer("a1", a1_data)?
                .add_buffer("a2", (a2_data, a2_offsets))?;
            continue;
        }

        let slices = result.slices()?;
        let rows: RQField<i32> = slices.field("rows")?;
        let cols: RQField<i32> = slices.field("columns")?;
        let a1s: RQField<i32> = slices.field("a1")?;
        let a2s: RQField<u8> = slices.field("a2")?;

        for (row, col, a1, a2) in izip!(
            rows.iter()?,
            cols.iter()?,
            a1s.iter()?,
            a2s.lossy_str_iter()?
        ) {
            println!("Cell: {} {} {} {:?}", row, col, a1, a2);
        }

        if result.completed() {
            break;
        }
    }

    Ok(())
}

fn main() -> TileDBResult<()> {
    if !array_exists() {
        create_array().expect("Failed to create array");
        write_array().expect("Failed to write array");
    }
    read_array().expect("Failed to step through array results");
    Ok(())
}
