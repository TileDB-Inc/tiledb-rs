extern crate tiledb;
use tiledb::group::Group;
use tiledb::vfs::VFS;
use tiledb::{Datatype, Result as TileDBResult};

/// This program creates a hierarchy as shown below. Specifically, it creates
/// groups `my_group` and `sparse_arrays`, and
/// then some dense/sparse arrays.
///
/// my_group/
/// ├── dense_arrays/array_A
/// ├── dense_arrays/array_B
/// └── sparse_arrays
///     ├── array_C
///     └── array_D
///
/// The program then shows how to group these together using the TileDB Group API.

fn create_array<S>(
    array_uri: S,
    array_type: tiledb::array::ArrayType,
) -> TileDBResult<()>
where
    S: AsRef<str>,
{
    let tdb = tiledb::context::Context::new()?;

    // The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
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

    // Create a single attribute "a" so each (i,j) cell can store an integer
    let attribute_a = tiledb::array::AttributeBuilder::new(
        &tdb,
        "a",
        tiledb::Datatype::Int32,
    )?
    .build();

    // Create array schema
    let schema = tiledb::array::SchemaBuilder::new(&tdb, array_type, domain)?
        .tile_order(tiledb::array::TileOrder::RowMajor)?
        .cell_order(tiledb::array::CellOrder::RowMajor)?
        .add_attribute(attribute_a)?
        .build()?;

    // Create array
    tiledb::Array::create(&tdb, array_uri, schema)?;
    Ok(())
}

fn create_arrays_groups() -> TileDBResult<()> {
    let tdb = tiledb::context::Context::new()?;

    // Create groups
    Group::create(&tdb, "my_group")?;
    Group::create(&tdb, "my_group/sparse_arrays")?;

    // Create dense_arrays folder
    let cfg = tiledb::config::Config::new()?;
    let vfs = VFS::new(&tdb, &cfg)?;
    vfs.create_dir("my_group/dense_arrays")?;

    // Create arrays
    create_array(
        "my_group/dense_arrays/array_A",
        tiledb::array::ArrayType::Dense,
    )?;
    create_array(
        "my_group/dense_arrays/array_B",
        tiledb::array::ArrayType::Dense,
    )?;
    create_array(
        "my_group/sparse_arrays/array_C",
        tiledb::array::ArrayType::Sparse,
    )?;
    create_array(
        "my_group/sparse_arrays/array_D",
        tiledb::array::ArrayType::Sparse,
    )?;

    // Add members to groups
    let mut group = Group::open(&tdb, "my_group", tiledb::query::QueryType::Write)?;

    group.add_member(
        "dense_arrays/array_A",
        true,
        None as Option<String>,
    )?;
    group.add_member("dense_arrays/array_B", true, Some("array_b"))?;

    group.add_member(
        "sparse_arrays",
        true,
        None as Option<String> //Some("sparse_arrays_group"),
    )?;

    let mut sparse_group = Group::open(
        &tdb,
        "my_group/sparse_arrays",
        tiledb::query::QueryType::Write,
    )?;
    sparse_group.add_member("array_C", true, None as Option<String>)?;
    sparse_group.add_member("array_D", true, None as Option<String>)?;
    Ok(())
}

fn print_group() -> TileDBResult<()> {
    let tdb = tiledb::context::Context::new()?;
    let group = Group::open(&tdb, "my_group", tiledb::query::QueryType::Read)?;
    let dump_str = group.dump(true)?;

    if let Some(s) = dump_str {
        println!("{}", s)
    }
    Ok(())
}

fn cleanup() -> TileDBResult<()> {
    let tdb = tiledb::context::Context::new()?;
    let group = Group::open(&tdb, "my_group", tiledb::query::QueryType::ModifyExclusive)?;
    group.delete_group("my_group", true)?;
    Ok(())
}

fn main() -> TileDBResult<()> {
    create_arrays_groups()?;
    print_group()?;
    cleanup()?;
    Ok(())
}
