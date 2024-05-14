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
    field_names: &mut HashSet<String>,
) -> TileDBResult<Vec<AttributeData>> {
    let num_attrs = rng.gen_range(1..32);
    let mut ret = Vec::new();
    for _ in 0..num_attrs {
        ret.push(attribute::generate(rng, schema, field_names)?)
    }
    Ok(ret)
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
        println!("SIMPLIFY!");
        false
    }

    fn complicate(&mut self) -> bool {
        println!("COMPLICATE!");
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
        schema.cell_order = gen_cell_order(runner.rng());
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
