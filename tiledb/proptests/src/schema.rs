use std::collections::HashSet;

use anyhow::anyhow;
use proptest::prelude::*;
use proptest::strategy::{NewTree, Strategy, ValueTree};
use proptest::test_runner::{TestRng, TestRunner};

use tiledb::array::attribute::AttributeData;
use tiledb::array::schema::{CellValNum, SchemaData};
use tiledb::array::{ArrayType, CellOrder, TileOrder};
use tiledb::datatype::Datatype;
use tiledb::Result as TileDBResult;

use crate::attribute;
use crate::domain;
use crate::filter_list::{self, FilterListContextKind};
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
        Some(rng.gen_range(1u64..4096))
    } else {
        None
    }
}

fn gen_cell_order(rng: &mut TestRng, schema: &SchemaData) -> Option<CellOrder> {
    if rng.gen_bool(0.5) {
        // Hilbert cell order is only valid for sparse arrays.
        if schema.array_type == ArrayType::Dense {
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
            Some(util::choose(
                rng,
                &[
                    CellOrder::Unordered,
                    CellOrder::Global,
                    CellOrder::RowMajor,
                    CellOrder::ColumnMajor,
                    CellOrder::Hilbert,
                ],
            ))
        }
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
    field_names: &mut HashSet<String>,
) -> TileDBResult<Vec<AttributeData>> {
    let num_attrs = rng.gen_range(1..32);
    let mut ret = Vec::new();
    for _ in 0..num_attrs {
        ret.push(attribute::generate(rng, schema, field_names)?)
    }
    Ok(ret)
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

#[repr(u8)]
#[derive(Clone, Debug)]
enum SimplifyState {
    Domain,
    Capacity,
    CellOrder,
    TileOrder,
    AllowDuplicates,
    Attributes,
    CoordinateFilters,
    OffsetFilters,
    NullityFilters,
    Done,
}

impl SimplifyState {
    fn next(&self) -> Self {
        let val = self.clone() as u8;
        SimplifyState::from(val + 1)
    }
}

impl From<u8> for SimplifyState {
    fn from(value: u8) -> SimplifyState {
        match value {
            0 => SimplifyState::Domain,
            1 => SimplifyState::Capacity,
            2 => SimplifyState::CellOrder,
            3 => SimplifyState::TileOrder,
            4 => SimplifyState::AllowDuplicates,
            5 => SimplifyState::Attributes,
            6 => SimplifyState::CoordinateFilters,
            7 => SimplifyState::OffsetFilters,
            8 => SimplifyState::NullityFilters,
            _ => SimplifyState::Done,
        }
    }
}

#[derive(Debug)]
pub struct SchemaValueTree {
    state: SimplifyState,
    schema: SchemaData,
    domain: domain::DomainValueTree,
}

impl SchemaValueTree {
    pub fn new(schema: SchemaData) -> Self {
        let domain = domain::DomainValueTree::new(schema.domain.clone());
        Self {
            state: SimplifyState::Domain,
            schema,
            domain,
        }
    }
}

impl ValueTree for SchemaValueTree {
    type Value = SchemaData;

    fn current(&self) -> Self::Value {
        let ret = self.schema.clone();
        SchemaData {
            domain: self.domain.current(),
            ..ret
        }
    }

    fn simplify(&mut self) -> bool {
        match self.state {
            SimplifyState::Domain => {
                if self.domain.simplify() {
                    return true;
                }

                self.state = self.state.next();
                self.simplify()
            }
            _ => false,
        }
    }

    fn complicate(&mut self) -> bool {
        println!("Complicate!");
        false
    }
}

#[derive(Default, Debug)]
pub struct SchemaStrategy {}

impl SchemaStrategy {
    pub fn new() -> Self {
        Self {}
    }
}

impl Strategy for SchemaStrategy {
    type Tree = SchemaValueTree;
    type Value = SchemaData;

    fn new_tree(&self, runner: &mut TestRunner) -> NewTree<Self> {
        println!("New tree!");
        let mut field_names: HashSet<String> = HashSet::new();
        let array_type = gen_array_type(runner.rng());

        let mut schema = SchemaData {
            array_type,
            ..Default::default()
        };

        schema.domain =
            domain::generate(runner.rng(), &schema, &mut field_names).map_err(
                |e| {
                    format!(
                        "{}",
                        anyhow!("Error creating domain for schema").context(e)
                    )
                },
            )?;

        schema.capacity = gen_capacity(runner.rng());
        schema.cell_order = gen_cell_order(runner.rng(), &schema);
        schema.tile_order = gen_tile_order(runner.rng());
        schema.allow_duplicates = gen_allow_duplicates(runner.rng(), &schema);

        schema.attributes =
            gen_attributes(runner.rng(), &schema, &mut field_names).map_err(
                |e| {
                    format!(
                        "{}",
                        anyhow!("Error creating attributes for schema.")
                            .context(e)
                    )
                },
            )?;

        schema.coordinate_filters = filter_list::generate(
            runner.rng(),
            FilterListContextKind::Coordinates,
            &schema,
            Datatype::Any,
            CellValNum::single(),
        )
        .map_err(|e| {
            format!(
                "{}",
                anyhow!("Error creating coordinate filters for schema")
                    .context(e)
            )
        })?;

        schema.offsets_filters = filter_list::generate(
            runner.rng(),
            FilterListContextKind::Offsets,
            &schema,
            Datatype::UInt64,
            CellValNum::single(),
        )
        .map_err(|e| {
            format!(
                "{}",
                anyhow!("Error creating offsets filters for schema").context(e)
            )
        })?;

        schema.nullity_filters = filter_list::generate(
            runner.rng(),
            FilterListContextKind::Nullity,
            &schema,
            Datatype::UInt8,
            CellValNum::single(),
        )
        .map_err(|e| {
            format!(
                "{}",
                anyhow!("Error creating nullity filters for schema").context(e)
            )
        })?;

        Ok(SchemaValueTree::new(schema))
    }
}
