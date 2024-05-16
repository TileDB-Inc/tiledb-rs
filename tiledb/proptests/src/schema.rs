use std::collections::HashSet;

use anyhow::anyhow;
use proptest::prelude::*;
use proptest::strategy::{NewTree, Strategy};
use proptest::test_runner::{TestRng, TestRunner};

use tiledb::array::attribute::AttributeData;
use tiledb::array::schema::{CellValNum, SchemaData};
use tiledb::array::{ArrayType, CellOrder, TileOrder};
use tiledb::datatype::Datatype;
use tiledb::Result as TileDBResult;

use crate::attribute;
use crate::domain;
use crate::explorer::{ValueTreeExplorer, ValueTreeExplorerAdaptor};
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

#[derive(Debug)]
enum Phase {
    Dimensions,
    Attributes,
    Finished,
}

#[derive(Debug)]
pub struct SchemaValueTree {
    root: SchemaData,
    current: SchemaData,
    schema: SchemaData,
    phase: Phase,
}

impl SchemaValueTree {
    pub fn new(schema: SchemaData) -> Self {
        let mut root = schema.clone();
        if !schema.domain.dimension.is_empty() {
            root.domain.dimension = vec![schema.domain.dimension[0].clone()];
        }
        if !schema.attributes.is_empty() {
            root.attributes = vec![schema.attributes[0].clone()];
        }

        Self {
            root: root.clone(),
            current: root.clone(),
            schema,
            phase: Phase::Dimensions,
        }
    }
}

impl ValueTreeExplorer for SchemaValueTree {
    type Value = SchemaData;

    fn root(&self) -> Self::Value {
        self.root.clone()
    }

    fn current(&self) -> Self::Value {
        self.current.clone()
    }

    fn explore(
        &mut self,
    ) -> Result<
        Option<Box<dyn ValueTreeExplorer<Value = Self::Value>>>,
        TestCaseError,
    > {
        println!("Exploring: {:?}", self.current);
        // We explore the schema error state in two phases. First, we extend
        // out all dimensions, then attributes.
        match self.phase {
            Phase::Dimensions => {
                let curr_dims = self.current.domain.dimension.len();
                let schema_dims = self.schema.domain.dimension.len();

                if curr_dims < schema_dims {
                    self.current
                        .domain
                        .dimension
                        .push(self.schema.domain.dimension[curr_dims].clone());
                }

                if curr_dims + 1 >= schema_dims {
                    self.phase = Phase::Attributes;
                }

                Ok(None)
            }
            Phase::Attributes => {
                let curr_attrs = self.current.attributes.len();
                let schema_attrs = self.schema.attributes.len();

                if curr_attrs < schema_attrs {
                    self.current
                        .attributes
                        .push(self.schema.attributes[curr_attrs].clone());
                }

                if curr_attrs + 1 >= schema_attrs {
                    self.phase = Phase::Finished;
                }

                Ok(None)
            }
            Phase::Finished => Err(TestCaseError::Fail(
                "Failed to find error.".to_string().into(),
            )),
        }
    }

    fn refine(&mut self) -> bool {
        // Ignore for now. We'll skip efficient searching until I can prove
        // this isn't all a terrible idea.
        println!("Refining: {:?}", self.current);
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
    type Tree = ValueTreeExplorerAdaptor<SchemaValueTree>;
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

        let explorer = Box::new(SchemaValueTree::new(schema));
        let adaptor = ValueTreeExplorerAdaptor::new(explorer);
        Ok(adaptor)
    }
}
