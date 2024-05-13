use proptest::collection::vec;
use proptest::prelude::*;
use proptest::strategy::{NewTree, Strategy, ValueTree};
use proptest::test_runner::{TestRng, TestRunner};

use tiledb::array::attribute::AttributeData;
use tiledb::array::schema::{CellValNum, SchemaData};
use tiledb::array::{ArrayType, CellOrder, TileOrder};
use tiledb::datatype::Datatype;

use crate::attribute;
use crate::domain;
use crate::filter::list;
use crate::util;

fn gen_array_type(rng: &mut TestRng) -> ArrayType {
    if rng.gen_bool(0.5) {
        ArrayType::Dense
    } else {
        ArrayType::Sparse
    }
}

fn gen_capacity(rng: &mut TestRng) -> Option<u64> {
    if rng.gen_bool(0.5) {
        Some(rng.gen_range(0u64..4096))
    } else {
        None
    }
}

fn gen_cell_order(rng: &mut TestRng) -> Option<CellOrder> {
    if rng.gen_bool(0.5) {
        Some(util::choose(
            rng,
            &[
                CellOrder::Unordered,
                CellOrder::Global,
                CellOrder::RowMajor,
                CellOrder::ColumnMajor,
            ],
        ))
    } else {
        None
    }
}

fn gen_tile_order(rng: &mut TestRng) -> Option<TileOrder> {
    if rng.gen_bool(0.5) {
        Some(util::choose(
            rng,
            &[TileOrder::RowMajor, TileOrder::ColumnMajor],
        ))
    } else {
        None
    }
}

fn gen_allow_duplicates(
    rng: &mut TestRng,
    schema: &SchemaData,
) -> Option<bool> {
    if rng.gen_bool(0.5) {
        Some(
            rng.gen_bool(0.5) && matches!(schema.array_type, ArrayType::Sparse),
        )
    } else {
        None
    }
}

fn gen_attributes(
    rng: &mut TestRng,
    schema: &SchemaData,
) -> Vec<AttributeData> {
    let num_attrs = rng.gen_range(1..32);
    let mut ret = Vec::new();
    for _ in 0..num_attrs {
        ret.push(attribute::generate(rng, schema))
    }
    ret
}

fn gen_coordinate_filters(
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

pub struct SchemaValueTree {
    data: SchemaData,
}

impl SchemaValueTree {
    pub fn new(data: SchemaData) -> Self {
        Self { data }
    }
}

impl ValueTree for SchemaValueTree {
    type Value = SchemaData;

    fn current(&self) -> Self::Value {
        self.data.clone()
    }

    fn simplify(&mut self) -> bool {
        false
    }

    fn complicate(&mut self) -> bool {
        false
    }
}

#[derive(Debug)]
pub struct SchemaStrategy {}

impl SchemaStrategy {
    pub fn new() -> Self {
        Self {}
    }
}

// pub struct SchemaData {
//     pub array_type: ArrayType,
//     pub domain: DomainData,
//     pub capacity: Option<u64>,
//     pub cell_order: Option<CellOrder>,
//     pub tile_order: Option<TileOrder>,
//     pub allow_duplicates: Option<bool>,
//     pub attributes: Vec<AttributeData>,
//     pub coordinate_filters: FilterListData,
//     pub offsets_filters: FilterListData,
//     pub nullity_filters: FilterListData,
// }

impl Strategy for SchemaStrategy {
    type Tree = SchemaValueTree;
    type Value = SchemaData;
    fn new_tree(&self, runner: &mut TestRunner) -> NewTree<Self> {
        let array_type = gen_array_type(runner.rng());
        let domain = domain::generate(runner.rng(), array_type);
        let data = SchemaData {
            array_type,
            domain,
            capacity: gen_capacity(runner.rng()),
            ..Default::default()
        };

        Ok(FilterListDataValueTree::new(filters))
    }
}

// pub fn prop_schema_data() -> impl Strategy<Value = SchemaData> {
//     prop_array_type()
//         .prop_flat_map(|array_type| {
//             let domain = pt_domain::prop_domain_data(array_type);
//             (Just(array_type), domain).prop_flat_map(|(array_type, domain)| {
//                 let schema = SchemaData {
//                     array_type,
//                     domain,
//                     ..Default::default()
//                 };
//
//                 add_capacity(schema)
//                     .prop_flat_map(add_cell_order)
//                     .prop_flat_map(add_tile_order)
//                     .prop_flat_map(add_allow_duplicates)
//                     .prop_flat_map(add_attributes)
//                     .prop_flat_map(add_coordinate_filters)
//                     .prop_flat_map(add_offsets_filters)
//                     .prop_flat_map(add_nullity_filters)
//             })
//         })
//         .boxed()
// }
