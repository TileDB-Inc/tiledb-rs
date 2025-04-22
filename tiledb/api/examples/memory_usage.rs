extern crate tiledb_api as tiledb;

use tiledb::Result as TileDBResult;
use tiledb::query::{QueryBuilder, ReadQuery, ReadQueryBuilder};

const ARRAY_URI: &str =
    "s3://tiledb-davisp/benchmarks/tpch/25G/tables/lineitem_array";

fn run() -> TileDBResult<()> {
    let tdb = tiledb::context::Context::new()?;

    let array =
        tiledb::Array::open(&tdb, ARRAY_URI, tiledb::array::Mode::Read)?;

    let mut query = tiledb::query::ReadBuilder::new(array)?
        .register_constructor::<_, Vec<i64>>("l_orderkey", Default::default())?
        .register_constructor::<_, Vec<f64>>("l_quantity", Default::default())?
        .build();

    let (a, (b, ())) = query.execute()?;

    eprintln!("A: {} B: {}", a.len(), b.len());

    Ok(())
}

fn main() -> TileDBResult<()> {
    let res = run();

    let dur = std::time::Duration::from_secs(300);
    std::thread::sleep(dur);

    res
}
