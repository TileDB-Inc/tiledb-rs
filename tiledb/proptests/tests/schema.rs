use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

use proptest::prelude::*;
use tempfile::TempDir;

use tiledb::array::Array;
use tiledb::context::Context;
use tiledb::{Factory, Result as TileDBResult};

use tiledb_proptests::schema;

const NUM_ITERS: u64 = 1_000u64;

#[test]
fn schema_creation() -> TileDBResult<()> {
    let cfg = ProptestConfig::with_cases(NUM_ITERS as u32);
    let ctx = Context::new()?;
    let uris = proptest::string::string_regex("[a-z]+")
        .expect("Error creating URI property.");
    let schemas = schema::SchemaStrategy::new();
    let iter: Rc<RefCell<u64>> = Rc::new(RefCell::new(0));
    let start = Instant::now();

    proptest!(cfg, move |(uri in uris, schema in schemas)| {
        let mut tmp = iter.borrow_mut();
        *tmp += 1;
        if *tmp % 10_000 == 0 {
            let now = Instant::now();
            let time_elapsed = now - start;

            let current_rate = (*tmp) as f64 / time_elapsed.as_secs() as f64;

            println!("Iter: {} {:?} {}", *tmp, time_elapsed, current_rate);
        }
        let tmp_dir = TempDir::new()?;
        let arr_dir = tmp_dir.path().join(uri);
        let schema = schema.create(&ctx)?;
        Array::create(&ctx, arr_dir.to_string_lossy(), schema)?;
    });

    Ok(())
}
