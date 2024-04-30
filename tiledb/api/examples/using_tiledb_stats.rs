extern crate tiledb;

use tiledb::array::{
    Array, ArrayType, AttributeBuilder, DimensionBuilder, DomainBuilder, Mode,
    SchemaBuilder,
};
use tiledb::context::Context;
use tiledb::datatype::Datatype;
use tiledb::query::buffer::{ReadBufferCollection, WriteBufferCollection};
use tiledb::query::read::ReadQueryBuilder;
use tiledb::query::traits::QueryBuilder;
use tiledb::query::write::WriteQueryBuilder;
use tiledb::vfs::VFS;
use tiledb::Result as TileDBResult;

const ARRAY_URI: &str = "stats_array";

/// Function that takes a vector of tiledb::stats::Metrics struct and prints
/// the data. The Metrics struct has two public fields: a HashMap<String, f64>
/// with relevant timers, and a HashMap<String, u64> with relevant counters.
pub fn print_metrics(metrics: &[tiledb::stats::Metrics]) {
    println!("Printing query metrics...");
    for metric in metrics.iter() {
        for timer in metric.timers.iter() {
            println!("Timer {}: {}", timer.0, timer.1);
        }

        for counter in metric.counters.iter() {
            println!("Counter {}: {}", counter.0, counter.1);
        }
    }
}

/// Creates a dense array at URI `ARRAY_NAME()`.
/// The array has two i32 dimensions ["row", "col"] with a single int32
/// attribute "a" stored in each cell.
/// Both "row" and "col" dimensions range from 1 to 12000, and the tiles
/// span all row_tile_extent elements on the "row" dimension, and
/// col_tile_extent elements on the "col" dimension.
/// Hence, we have 144,000,000 elements in the array. There are
/// 144,000,000/(row_tile_extent * col_tile_extent) tiles in this array.
pub fn create_array(
    ctx: &Context,
    row_tile_extent: u32,
    col_tile_extent: u32,
) -> TileDBResult<()> {
    let vfs: VFS = VFS::new(ctx)?;

    if vfs.is_dir(ARRAY_URI)? {
        vfs.remove_dir(ARRAY_URI)?;
    }

    let domain = {
        let rows = DimensionBuilder::new::<u32>(
            ctx,
            "rows",
            Datatype::UInt32,
            &[1, 12000],
            &row_tile_extent,
        )?
        .build();

        let cols = DimensionBuilder::new::<u32>(
            ctx,
            "cols",
            Datatype::UInt32,
            &[1, 12000],
            &col_tile_extent,
        )?
        .build();

        DomainBuilder::new(ctx)?
            .add_dimension(rows)?
            .add_dimension(cols)?
            .build()
    };

    let attr = AttributeBuilder::new(ctx, "attr", Datatype::Int32)?.build();

    let schema = SchemaBuilder::new(ctx, ArrayType::Dense, domain)?
        .add_attribute(attr)?
        .build()?;

    tiledb::Array::create(ctx, ARRAY_URI, schema)
}

/// Writes data into the array in row-major order from a 1D-array buffer.
/// After the write, the contents of the array will be:
/// [[ 0, 1 ... 11999],
///  [ 12000, 12001, ... 23999],
///  ...
///  [143988000, 143988001 ... 143999999]]
pub fn write_array(ctx: &Context) -> TileDBResult<()> {
    let array = Array::open(ctx, ARRAY_URI, Mode::Write)?;
    let attr_data = Vec::from_iter(0i32..12000 * 12000);

    let mut query = WriteQueryBuilder::new(array)?
        .layout(tiledb::query::QueryLayout::RowMajor)?
        .build();

    let buffers = WriteBufferCollection::new()
        .add_buffer("attr", attr_data.as_slice())?;

    let _ = query.submit(&buffers)?;
    query.finalize()?;
    Ok(())
}

/// Query back a slice of our array and print the stats collected on the query.
/// The argument json will determine whether the stats are printed in JSON
/// format or in string format.
///
/// For the read query, the slice on "row" is [1, 3000] and on "col" is [1, 12000],
/// so the returned data should look like:
/// [[ 0, 1 ... 11999],
///  [ 12000, 12001, ... 23999],
///  ...
///  [35988000, 35988001, ... 35999999],
///  [_, _, ... , _],
/// ...
/// [_, _, ... , _]]
pub fn read_array(ctx: &Context, json: bool) -> TileDBResult<()> {
    let array = Array::open(ctx, ARRAY_URI, Mode::Read)?;

    let mut query = ReadQueryBuilder::new(array)?
        .layout(tiledb::query::QueryLayout::RowMajor)?
        .start_subarray()?
        .add_range("rows", &[1u32, 12000])?
        .add_range("cols", &[1u32, 3000])?
        .finish_subarray()?
        .build();

    let row_data = vec![0u32; 12000 * 3000].into_boxed_slice();
    let col_data = vec![0u32; 12000 * 3000].into_boxed_slice();
    let attr_data = vec![0i32; 12000 * 3000].into_boxed_slice();

    let buffers = ReadBufferCollection::new();
    buffers
        .borrow_mut()
        .add_buffer("rows", row_data)?
        .add_buffer("cols", col_data)?
        .add_buffer("attr", attr_data)?;

    tiledb::stats::enable()?;

    let result = query.submit(&buffers)?;
    assert!(result.completed());

    if json {
        let stats = tiledb::stats::dump_json()?;
        match stats {
            Some(stats_json) => print_metrics(&stats_json),
            None => println!("No stats associated with this query."),
        }
    } else {
        let stats = tiledb::stats::dump()?;
        match stats {
            Some(stats_str) => println!("{}", &stats_str),
            None => println!("No stats associated with this query."),
        }
    }
    tiledb::stats::disable()?;
    Ok(())
}

fn main() -> TileDBResult<()> {
    let ctx = Context::new()?;
    create_array(&ctx, 1, 12000)?;
    write_array(&ctx)?;
    read_array(&ctx, false)?;
    read_array(&ctx, true)?;
    Ok(())
}
