use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

use proptest::prelude::*;
use tempfile::TempDir;

use tiledb::array::Array;
use tiledb::context::Context;
use tiledb::{Factory, Result as TileDBResult};

use tiledb_proptests::schema;

const NUM_ITERS: u64 = 100_000u64;

#[test]
fn schema_creation() -> TileDBResult<()> {
    let cfg = ProptestConfig::with_cases(NUM_ITERS as u32);
    let ctx = Context::new()?;

    let iter: Rc<RefCell<u64>> = Rc::new(RefCell::new(0));
    let start = Instant::now();

    let schemas = schema::SchemaStrategy::new();

    proptest!(cfg, move |(schema in schemas)| {
        println!("Generated test: {:#?}", schema);
        let mut tmp = iter.borrow_mut();
        *tmp += 1;
        if *tmp % 10_000 == 0 {
            let now = Instant::now();
            let time_elapsed = now - start;

            let current_rate = (*tmp) as f64 / time_elapsed.as_secs() as f64;

            println!("Iter: {} {:?} {}", *tmp, time_elapsed, current_rate);
        }
        let tmp_dir = TempDir::new()?;
        let arr_dir = tmp_dir.path().join("test_array");
        let schema = schema.create(&ctx)?;
        println!("Creating array!");
        Array::create(&ctx, arr_dir.to_string_lossy(), schema)?;
    });

    Ok(())
}
