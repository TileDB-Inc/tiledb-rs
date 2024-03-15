extern crate tiledb;

use tiledb::Result as TileDBResult;

const QUICKSTART_DENSE_ARRAY_URI: &'static str = "quickstart_dense_array";
const QUICKSTART_ATTRIBUTE_NAME: &'static str = "a";

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

    let attribute_a = tiledb::array::Attribute::new(
        &tdb,
        QUICKSTART_ATTRIBUTE_NAME,
        tiledb::Datatype::Int32,
    )?;

    let schema = tiledb::array::SchemaBuilder::new(
        &tdb,
        tiledb::array::ArrayType::Dense,
        domain,
    )?
    .add_attribute(attribute_a)?
    .build();

    tiledb::Array::create(&tdb, QUICKSTART_DENSE_ARRAY_URI, schema)
}

fn write_array() -> TileDBResult<()> {
    let tdb = tiledb::context::Context::new()?;

    let array = tiledb::Array::open(
        &tdb,
        QUICKSTART_DENSE_ARRAY_URI,
        tiledb::array::Mode::Write,
    )?;

    let mut data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

    let query =
        tiledb::QueryBuilder::new(&tdb, array, tiledb::QueryType::Write)?
            .layout(tiledb::array::Layout::RowMajor)?
            .dimension_buffer_typed(
                QUICKSTART_ATTRIBUTE_NAME,
                data.as_mut_slice(),
            )?
            .build();

    query.submit().map(|_| ())
}

fn main() {
    create_array().expect("Failed to create array")
}
