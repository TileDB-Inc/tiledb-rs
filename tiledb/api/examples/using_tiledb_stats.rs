extern crate tiledb;
use tiledb::config::Config;
use tiledb::vfs::VFS;
use tiledb::Datatype;
use tiledb::{Array, Result as TileDBResult};

const ARRAY_NAME: &str = "stats_array";
const ATTRIBUTE_NAME: &str = "a";

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
            tiledb::array::DimensionBuilder::new::<u32>(
                &tdb,
                "row",
                Datatype::UInt32,
                &[1, 12000],
                &row_tile_extent,
            )?
            .build();

        let cols: tiledb::array::Dimension =
            tiledb::array::DimensionBuilder::new::<u32>(
                &tdb,
                "col",
                Datatype::UInt32,
                &[1, 12000],
                &col_tile_extent,
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
    .build();

    tiledb::Array::create(&tdb, ARRAY_NAME, schema)
}

pub fn write_array() -> TileDBResult<()> {
    let tdb = tiledb::context::Context::new()?;
    let array: Array =
        tiledb::Array::open(&tdb, ARRAY_NAME, tiledb::array::Mode::Write)?;
    let mut data: Vec<i32> = Vec::from_iter(0..12000 * 12000);

    let query =
        tiledb::QueryBuilder::new(&tdb, array, tiledb::QueryType::Write)?
            .layout(tiledb::array::Layout::RowMajor)?
            .dimension_buffer_typed(ATTRIBUTE_NAME, data.as_mut_slice())?
            .build();

    query.submit()?;
    Ok(())
}

pub fn read_array() -> TileDBResult<()> {
    let tdb = tiledb::context::Context::new()?;

    let array =
        tiledb::Array::open(&tdb, ARRAY_NAME, tiledb::array::Mode::Read)?;

    let mut results = vec![0; 3000 * 12000];

    let query =
        tiledb::QueryBuilder::new(&tdb, array, tiledb::QueryType::Read)?
            .layout(tiledb::array::Layout::RowMajor)?
            .dimension_buffer_typed(ARRAY_NAME, results.as_mut_slice())?
            .add_subarray()?
            .dimension_range_typed::<i32>(0, &[1, 3000])?
            .add_subarray()?
            .dimension_range_typed::<i32>(1, &[1, 12000])?
            .build();

    tiledb::stats::enable()?;
    query.submit()?;
    let stats: Option<String> = tiledb::stats::dump()?;
    match stats {
        Some(stats_str) => println!("{}", &stats_str),
        None => println!("No stats associated with this query."),
    }
    tiledb::stats::disable()?;
    Ok(())
}

fn main() -> TileDBResult<()> {
    create_array(1, 12000)?;
    write_array()?;
    read_array()?;
    Ok(())
}
