fn main() {
    println!("Hello, world!");
}

// extern crate tiledb;
//
// use tiledb::query::QueryBuilder;
// use tiledb::Datatype;
// use tiledb::Result as TileDBResult;
//
// const FRAGMENT_INFO_ARRAY_URI: &str = "fragment_info_example_array";
// const FRAGMENT_INFO_ATTRIBUTE_NAME: &str = "a";
//
// fn main() {
//     if !array_exists() {
//         create_array().expect("Failed to create array");
//
//         // Multiple writes to demonstrate multiple fragments in the array
//         write_array().expect("Failed first write to array");
//         write_array().expect("Failed second write to array");
//     }
//
//     read_fragment_info().expect("Error reading fragment info");
// }
//
// /// Read the FragmentInfo and print some of the various bits.
// fn read_fragment_info() -> TileDBResult<()> {
//     let tdb = tiledb::context::Context::new()?;
//
//     let frag_infos =
//         tiledb::array::FragmentInfoBuilder::new(&tdb, FRAGMENT_INFO_ARRAY_URI)?
//             .build()?;
//     let num_frags = frag_infos.num_fragments()?;
//
//     println!("Number of fragments: {}", num_frags);
//     println!("To Vacuum Num: {}", frag_infos.num_to_vacuum()?);
//     println!("Total Cell Count: {}", frag_infos.total_cell_count()?);
//     println!(
//         "Unconsolidated Metadata Num: {}",
//         frag_infos.unconsolidated_metadata_num()?
//     );
//     for (i, frag_info) in frag_infos.iter()?.enumerate() {
//         println!("Name {}: {}", i, frag_info.name()?);
//         println!("URI  {}: {}", i, frag_info.uri()?);
//         println!("Size {}: {}", i, frag_info.size()?);
//         println!("Type {}: {:?}", i, frag_info.fragment_type()?);
//         println!("Timestamp Range {}: {:?}", i, frag_info.timestamp_range()?);
//         println!("Cell Num {}: {}", i, frag_info.num_cells()?);
//         println!("Version {}: {}", i, frag_info.version()?);
//         println!(
//             "Has Consolidated Metadata {}: {}",
//             i,
//             frag_info.has_consolidated_metadata()?
//         );
//
//         frag_info.to_vacuum_uri().expect_err("No vacuums to vacuum");
//
//         println!("Schema {}: {:?}", i, frag_info.schema()?);
//         println!("Schema Name {}: {}", i, frag_info.schema_name()?);
//         println!(
//             "Non-empty domain: {}: {:?}",
//             i,
//             frag_info.non_empty_domain()?
//         );
//
//         // Dense arrays don't have MBRs, but the num_mbrs method is hard
//         // coded to return 0 in libtiledb.
//         println!("Num MBRs {}: {}", i, frag_info.num_mbrs()?);
//         frag_info.mbr(0).expect_err("Dense arrays don't have MBRs");
//     }
//
//     Ok(())
// }
//
// /// Returns whether the example array already exists
// fn array_exists() -> bool {
//     let tdb = match tiledb::context::Context::new() {
//         Err(_) => return false,
//         Ok(tdb) => tdb,
//     };
//
//     tiledb::array::Array::exists(&tdb, FRAGMENT_INFO_ARRAY_URI)
//         .expect("Error checking array existence")
// }
//
// /// Creates a dense array at URI `QUICKSTART_DENSE_ARRAY_URI()`.
// /// The array has two i32 dimensions ["rows", "columns"] with a single int32
// /// attribute "a" stored in each cell.
// /// Both "rows" and "columns" dimensions range from 1 to 4, and the tiles
// /// span all 4 elements on each dimension.
// /// Hence we have 16 cells of data and a single tile for the whole array.
// fn create_array() -> TileDBResult<()> {
//     let tdb = tiledb::context::Context::new()?;
//
//     let domain = {
//         let rows: tiledb::array::Dimension =
//             tiledb::array::DimensionBuilder::new::<i32>(
//                 &tdb,
//                 "rows",
//                 Datatype::Int32,
//                 &[1, 4],
//                 &4,
//             )?
//             .build();
//
//         let cols: tiledb::array::Dimension =
//             tiledb::array::DimensionBuilder::new::<i32>(
//                 &tdb,
//                 "columns",
//                 Datatype::Int32,
//                 &[1, 4],
//                 &4,
//             )?
//             .build();
//
//         tiledb::array::DomainBuilder::new(&tdb)?
//             .add_dimension(rows)?
//             .add_dimension(cols)?
//             .build()
//     };
//
//     let attribute_a = tiledb::array::AttributeBuilder::new(
//         &tdb,
//         FRAGMENT_INFO_ATTRIBUTE_NAME,
//         tiledb::Datatype::Int32,
//     )?
//     .build();
//
//     let schema = tiledb::array::SchemaBuilder::new(
//         &tdb,
//         tiledb::array::ArrayType::Dense,
//         domain,
//     )?
//     .add_attribute(attribute_a)?
//     .build()?;
//
//     tiledb::Array::create(&tdb, FRAGMENT_INFO_ARRAY_URI, schema)
// }
//
// /// Writes data into the array in row-major order from a 1D-array buffer.
// /// After the write, the contents of the array will be:
// /// [[ 1,  2,  3,  4],
// ///  [ 5,  6,  7,  8],
// ///  [ 9, 10, 11, 12],
// ///  [13, 14, 15, 16]]
// fn write_array() -> TileDBResult<()> {
//     let tdb = tiledb::context::Context::new()?;
//
//     let array = tiledb::Array::open(
//         &tdb,
//         FRAGMENT_INFO_ARRAY_URI,
//         tiledb::array::Mode::Write,
//     )?;
//
//     let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
//
//     let query = tiledb::query::WriteBuilder::new(array)?
//         .layout(tiledb::query::QueryLayout::RowMajor)?
//         .data_typed(FRAGMENT_INFO_ATTRIBUTE_NAME, &data)?
//         .build();
//
//     query.submit().map(|_| ())
// }
