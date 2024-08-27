extern crate tiledb;

use std::path::PathBuf;

use tiledb::config::Config;
use tiledb::query::{QueryBuilder, ReadQuery, ReadQueryBuilder};
use tiledb::vfs::VFS;
use tiledb::Datatype;
use tiledb::{Array, Result as TileDBResult};

const ARRAY_NAME: &str = "using_tiledb_stats";
const ATTRIBUTE_NAME: &str = "a";

/// Prints tiledb statistics.
///
/// The `Metrics` struct has two public fields: a `HashMap<String, f64>`
/// with relevant timers, and a `HashMap<String, u64>` with relevant counters.
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
///
/// The array has two i32 dimensions ["row", "col"] with a single int32
/// attribute "a" stored in each cell.
/// Both "row" and "col" dimensions range from 1 to 12000, and the tiles
/// span all row_tile_extent elements on the "row" dimension, and
/// col_tile_extent elements on the "col" dimension.
/// Hence, we have 144,000,000 elements in the array. There are
/// 144,000,000/(row_tile_extent * col_tile_extent) tiles in this array.
pub fn create_array(
    row_tile_extent: u32,
    col_tile_extent: u32,
) -> TileDBResult<()> {
    let tdb = tiledb::context::Context::new()?;
    let config: Config = tiledb::config::Config::new()?;
    let vfs: VFS = tiledb::vfs::VFS::new(&tdb, &config)?;

    let is_cur_dir = vfs.is_dir(ARRAY_NAME)?;
    if is_cur_dir {
        vfs.remove_dir(ARRAY_NAME)?;
    }

    let domain = {
        let rows: tiledb::array::Dimension =
            tiledb::array::DimensionBuilder::new(
                &tdb,
                "row",
                Datatype::UInt32,
                ([1, 12000], row_tile_extent),
            )?
            .build();

        let cols: tiledb::array::Dimension =
            tiledb::array::DimensionBuilder::new(
                &tdb,
                "col",
                Datatype::UInt32,
                ([1, 12000], col_tile_extent),
            )?
            .build();

        tiledb::array::DomainBuilder::new(&tdb)?
            .add_dimension(rows)?
            .add_dimension(cols)?
            .build()
    };

    let attribute_a = tiledb::array::AttributeBuilder::new(
        &tdb,
        ATTRIBUTE_NAME,
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

    tiledb::Array::create(&tdb, ARRAY_NAME, schema)
}

/// Writes data into the array in row-major order from a 1D-array buffer.
///
/// After the write, the contents of the array will be:
/// [[ 0, 1 ... 11999],
///  [ 12000, 12001, ... 23999],
///  ...
///  [143988000, 143988001 ... 143999999]]
pub fn write_array() -> TileDBResult<()> {
    let tdb = tiledb::context::Context::new()?;
    let array: Array =
        tiledb::Array::open(&tdb, ARRAY_NAME, tiledb::array::Mode::Write)?;
    let data: Vec<i32> = Vec::from_iter(0..12000 * 12000);

    let query = tiledb::query::WriteBuilder::new(array)?
        .layout(tiledb::query::QueryLayout::RowMajor)?
        .data_typed(ATTRIBUTE_NAME, &data)?
        .build();

    query.submit()?;
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
pub fn read_array(json: bool) -> TileDBResult<()> {
    let tdb = tiledb::context::Context::new()?;

    let array =
        tiledb::Array::open(&tdb, ARRAY_NAME, tiledb::array::Mode::Read)?;

    let mut query = tiledb::query::ReadBuilder::new(array)?
        .layout(tiledb::query::QueryLayout::RowMajor)?
        .register_constructor::<_, Vec<i32>>(
            ATTRIBUTE_NAME,
            Default::default(),
        )?
        .start_subarray()?
        .add_range(0, &[1u32, 3000])?
        .add_range(1, &[1u32, 12000])?
        .finish_subarray()?
        .build();

    tiledb::stats::enable()?;
    let (_results, _) = query.execute()?;

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
    if let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR") {
        let _ = std::env::set_current_dir(
            PathBuf::from(manifest_dir).join("examples").join("output"),
        );
    }

    create_array(1, 12000)?;
    write_array()?;
    read_array(false)?;
    read_array(true)?;
    Ok(())
}
