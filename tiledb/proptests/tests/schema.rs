use proptest::prelude::*;
use tempfile::TempDir;

use tiledb::array::Array;
use tiledb::context::Context;
use tiledb::{Factory, Result as TileDBResult};

use tiledb_proptests::schema as pt_schema;

#[test]
fn schema_creation() -> TileDBResult<()> {
    let cfg = ProptestConfig::with_cases(1000);
    let ctx = Context::new()?;
    let uris = proptest::string::string_regex("[a-z]+")
        .expect("Error creating URI property.");

    proptest!(cfg, |(uri in uris, data in pt_schema::prop_schema_data())| {
        let tmp_dir = TempDir::new()?;
        let arr_dir = tmp_dir.path().join(uri);
        let schema = data.create(&ctx)?;
        Array::create(&ctx, arr_dir.to_string_lossy(), schema)?;
    });

    Ok(())
}
