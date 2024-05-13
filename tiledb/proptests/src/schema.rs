use proptest::collection::vec;
use proptest::prelude::*;

use tiledb::array::schema::{CellValNum, SchemaData};
use tiledb::array::{ArrayType, CellOrder, TileOrder};
use tiledb::datatype::Datatype;

use crate::attribute as pt_attribute;
use crate::domain as pt_domain;
use crate::filter::list as pt_list;

fn prop_array_type() -> impl Strategy<Value = ArrayType> {
    prop_oneof![Just(ArrayType::Dense), Just(ArrayType::Sparse)]
}

fn add_capacity(schema: SchemaData) -> impl Strategy<Value = SchemaData> {
    (Just(schema), 0u64..4096).prop_flat_map(|(mut schema, capacity)| {
        schema.capacity = Some(capacity);
        Just(schema)
    })
}

fn add_cell_order(schema: SchemaData) -> impl Strategy<Value = SchemaData> {
    let prop = prop_oneof![
        Just(CellOrder::Unordered),
        Just(CellOrder::Global),
        Just(CellOrder::RowMajor),
        Just(CellOrder::ColumnMajor),
    ];

    (Just(schema), prop).prop_flat_map(|(mut schema, cell_order)| {
        schema.cell_order = Some(cell_order);
        Just(schema)
    })
}

fn add_tile_order(schema: SchemaData) -> impl Strategy<Value = SchemaData> {
    let prop =
        prop_oneof![Just(TileOrder::RowMajor), Just(TileOrder::ColumnMajor),];

    (Just(schema), prop).prop_flat_map(|(mut schema, tile_order)| {
        schema.tile_order = Some(tile_order);
        Just(schema)
    })
}

fn add_allow_duplicates(
    schema: SchemaData,
) -> impl Strategy<Value = SchemaData> {
    (Just(schema), any::<bool>()).prop_flat_map(|(mut schema, allow_dups)| {
        let allow_dups =
            allow_dups && matches!(schema.array_type, ArrayType::Sparse);
        schema.allow_duplicates = Some(allow_dups);
        Just(schema)
    })
}

fn add_attributes(schema: SchemaData) -> impl Strategy<Value = SchemaData> {
    let attrs = vec(pt_attribute::prop_attribute_for(&schema), 1..32);
    (Just(schema), attrs).prop_flat_map(|(mut schema, attrs)| {
        schema.attributes = attrs;
        Just(schema)
    })
}

fn add_coordinate_filters(
    schema: SchemaData,
) -> impl Strategy<Value = SchemaData> {
    let filters =
        pt_list::prop_filter_list(Datatype::Any, CellValNum::single(), 6);
    (Just(schema), filters).prop_flat_map(|(mut schema, filters)| {
        schema.coordinate_filters = filters;
        Just(schema)
    })
}

fn add_offsets_filters(
    schema: SchemaData,
) -> impl Strategy<Value = SchemaData> {
    let filters =
        pt_list::prop_filter_list(Datatype::UInt64, CellValNum::single(), 6);
    (Just(schema), filters).prop_flat_map(|(mut schema, filters)| {
        schema.coordinate_filters = filters;
        Just(schema)
    })
}

fn add_nullity_filters(
    schema: SchemaData,
) -> impl Strategy<Value = SchemaData> {
    let filters =
        pt_list::prop_filter_list(Datatype::UInt8, CellValNum::single(), 6);
    (Just(schema), filters).prop_flat_map(|(mut schema, filters)| {
        schema.coordinate_filters = filters;
        Just(schema)
    })
}

pub fn prop_schema_data() -> BoxedStrategy<SchemaData> {
    prop_array_type()
        .prop_flat_map(|array_type| {
            let domain = pt_domain::prop_domain_data(array_type);
            (Just(array_type), domain).prop_flat_map(|(array_type, domain)| {
                let schema = SchemaData {
                    array_type,
                    domain,
                    ..Default::default()
                };

                add_capacity(schema)
                    .prop_flat_map(add_cell_order)
                    .prop_flat_map(add_tile_order)
                    .prop_flat_map(add_allow_duplicates)
                    .prop_flat_map(add_attributes)
                    .prop_flat_map(add_coordinate_filters)
                    .prop_flat_map(add_offsets_filters)
                    .prop_flat_map(add_nullity_filters)
            })
        })
        .boxed()
}
