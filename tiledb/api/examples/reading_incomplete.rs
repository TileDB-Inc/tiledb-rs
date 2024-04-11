extern crate tiledb;

use std::collections::HashMap;

use tiledb::Datatype;
use tiledb::Result as TileDBResult;

const ARRAY_URI: &str = "reading_incomplete_array";

fn main() -> TileDBResult<()> {
    if !array_exists()? {
        create_array()?;
        write_array()?;
    }

    read_array()?;

    Ok(())
}

fn read_array() -> TileDBResult<()> {
    let tdb = tiledb::context::Context::new()?;

    let array =
        tiledb::Array::open(&tdb, ARRAY_URI, tiledb::array::Mode::Read)?;

    let mut row_data = vec![0u32; 1];
    let mut col_data = vec![0u32; 1];
    let mut a1_data = vec![0u32; 1];
    let mut a2_data = vec![0u8; 1];
    let mut a2_offsets = vec![0u64; 1];

    println!("Submitting read query");
    let query =
        tiledb::QueryBuilder::new(&tdb, array, tiledb::QueryType::Read)?
            .layout(tiledb::array::CellOrder::RowMajor)?
            .read_all()?
            .build();

    for i in 1.. {
        let result = query
            .executor()
            .set_data_buffer("rows", row_data.as_mut_slice())?
            .set_data_buffer("cols", col_data.as_mut_slice())?
            .set_data_buffer("a1", a1_data.as_mut_slice())?
            .set_data_buffer("a2", a2_data.as_mut_slice())?
            .set_offsets_buffer("a2", a2_offsets.as_mut_slice())?
            .submit()?;

        let status = result.status();
        let num_values = result.sizes().get("a1").unwrap().0;

        println!("Iteration {}: {:?} {}", i, status, num_values);

        // If the query status is incomplete and we have no elements returned,
        // it means that our buffers are not large enough to store a single
        // cell. Thus we need to increase our buffer sizes.
        if status == tiledb::query::QueryStatus::Incomplete && num_values == 0 {
            println!("No results, reallocating buffers.");
            resize_buffers(
                &mut row_data,
                &mut col_data,
                &mut a1_data,
                &mut a2_data,
                &mut a2_offsets,
            );
            continue;
        } else if num_values > 0 {
            println!("Values:");
            print_results(
                &row_data,
                &col_data,
                &a1_data,
                &a2_data,
                &a2_offsets,
                result.sizes(),
            );
        }

        if status != tiledb::query::QueryStatus::Incomplete {
            println!("Finished with status: {:?}", status);
            break;
        }
    }

    Ok(())
}

/// Returns whether the example array already exists
fn array_exists() -> TileDBResult<bool> {
    let tdb = tiledb::context::Context::new()?;
    tiledb::array::Array::exists(&tdb, ARRAY_URI)
}

fn create_array() -> TileDBResult<()> {
    let tdb = tiledb::context::Context::new()?;

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
                "cols",
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

    let attr_a1 = tiledb::array::AttributeBuilder::new(
        &tdb,
        "a1",
        tiledb::Datatype::Int32,
    )?
    .build();

    let attr_a2 = tiledb::array::AttributeBuilder::new(
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
    .add_attribute(attr_a1)?
    .add_attribute(attr_a2)?
    .build()?;

    tiledb::Array::create(&tdb, ARRAY_URI, schema)
}

fn write_array() -> TileDBResult<()> {
    let tdb = tiledb::context::Context::new()?;

    let array =
        tiledb::Array::open(&tdb, ARRAY_URI, tiledb::array::Mode::Write)?;

    let mut row_data = vec![1u32, 2, 3];
    let mut col_data = vec![1u32, 1, 2];
    let mut a1_data = vec![1u32, 2, 3];
    let mut a2_data = "abbccc".bytes().collect::<Vec<u8>>();
    let mut a2_offsets = vec![0u64, 1, 3];

    println!("Writing array data");
    let query =
        tiledb::QueryBuilder::new(&tdb, array, tiledb::QueryType::Write)?
            .layout(tiledb::array::CellOrder::Global)?
            .build();

    let result = query
        .executor()
        .set_data_buffer("rows", row_data.as_mut_slice())?
        .set_data_buffer("cols", col_data.as_mut_slice())?
        .set_data_buffer("a1", a1_data.as_mut_slice())?
        .set_data_buffer("a2", a2_data.as_mut_slice())?
        .set_offsets_buffer("a2", a2_offsets.as_mut_slice())?
        .submit()?;

    assert!(result.completed());

    query.executor().finalize()?;

    Ok(())
}

fn resize_buffers(
    row_data: &mut Vec<u32>,
    col_data: &mut Vec<u32>,
    a1_data: &mut Vec<u32>,
    a2_data: &mut Vec<u8>,
    a2_offsets: &mut Vec<u64>,
) {
    println!("Reallocating buffers.");

    // A naive doubling of the buffer lengths. Real life situations will likely
    // be more complicated.
    row_data.resize(row_data.len() * 2, 0);
    col_data.resize(col_data.len() * 2, 0);
    a1_data.resize(a1_data.len() * 2, 0);
    a2_data.resize(a2_data.len() * 2, 0);
    a2_offsets.resize(a2_offsets.len() * 2, 0);
}

fn print_results(
    row_data: &[u32],
    col_data: &[u32],
    a1_data: &[u32],
    a2_data: &[u8],
    a2_offsets: &[u64],
    sizes: &HashMap<String, (u64, Option<u64>)>,
) {
    let a2_info = sizes.get("a2").unwrap();
    let num_a2_offsets = a2_info.0 as usize;

    let mut a2_values = Vec::<String>::new();

    for i in 0..num_a2_offsets {
        let len = if i < num_a2_offsets - 1 {
            a2_offsets[i + 1] - a2_offsets[i]
        } else {
            a2_info.1.unwrap() - a2_offsets[i]
        };
        let val =
            &a2_data[(a2_offsets[i] as usize)..(a2_offsets[i] + len) as usize];
        a2_values.push(String::from_utf8_lossy(val).to_string())
    }

    for i in 0..num_a2_offsets {
        println!(
            "Cell({}, {}, {}, {})",
            row_data[i], col_data[i], a1_data[i], a2_values[i]
        );
    }
}
