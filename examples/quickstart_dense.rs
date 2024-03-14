extern crate tiledb;

use tiledb::Result as TileDBResult;

const QUICKSTART_DENSE_ARRAY_NAME: &'static str = "quickstart_dense_array";

fn create_array() -> TileDBResult<()> {
    let tdb = tiledb::context::Context::new()?;

    let domain = {
        let rows: tiledb::array::Dimension =
            tiledb::array::DimensionBuilder::new::<i32>(
                &tdb,
                "rows",
                &[1, 4],
                &4,
            )?
            .build();
        let cols: tiledb::array::Dimension =
            tiledb::array::DimensionBuilder::new::<i32>(
                &tdb,
                "columns",
                &[1, 4],
                &4,
            )?
            .build();

        tiledb::array::DomainBuilder::new(&tdb)?
            .add_dimension(rows)?
            .add_dimension(cols)?
            .build()
    };

    let attribute_a =
        tiledb::array::Attribute::new(&tdb, "a", tiledb::Datatype::Int32)?;

    let schema = tiledb::array::SchemaBuilder::new(
        &tdb,
        tiledb::array::ArrayType::Dense,
    )?
    .domain(domain)?
    .add_attribute(attribute_a)?
    .build();

    tiledb::Array::create(&tdb, QUICKSTART_DENSE_ARRAY_NAME, schema)
}

fn main() {
    create_array().expect("Failed to create array")
}
