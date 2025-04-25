use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::ops::RangeInclusive;
use std::rc::Rc;

use proptest::prelude::*;
use proptest::strategy::{NewTree, ValueTree};
use proptest::test_runner::TestRunner;

use tiledb_common::array::{ArrayType, CellOrder, CellValNum};
use tiledb_common::range::{Range, SingleValueRange};
use tiledb_common::single_value_range_go;
use tiledb_pod::array::schema::SchemaData;

use super::*;
use crate::strategy::{
    CellsParameters, CellsStrategySchema, FieldDataParameters,
};
use crate::{Cells, StructuredCells};

type BoxedValueTree<T> = Box<dyn ValueTree<Value = T>>;

#[derive(Clone, Debug)]
pub struct DenseWriteParameters {
    pub schema: Option<Rc<SchemaData>>,
    pub layout: Option<CellOrder>,
    pub memory_limit: usize,
}

impl DenseWriteParameters {
    pub fn memory_limit_default() -> usize {
        **tiledb_proptest_config::TILEDB_STRATEGY_DENSE_WRITE_PARAMETERS_MEMORY_LIMIT
    }
}

impl Default for DenseWriteParameters {
    fn default() -> Self {
        DenseWriteParameters {
            schema: Default::default(),
            layout: Default::default(),
            memory_limit: Self::memory_limit_default(),
        }
    }
}

pub struct DenseWriteValueTree {
    layout: CellOrder,
    field_order: Vec<String>,
    bounding_subarray: Vec<RangeInclusive<i128>>,
    subarray: Vec<BoxedValueTree<SingleValueRange>>,
    cells: StructuredCells,
    prev_shrink: Option<usize>,
}

impl DenseWriteValueTree {
    pub fn new(
        layout: CellOrder,
        bounding_subarray: Vec<SingleValueRange>,
        subarray: Vec<BoxedValueTree<SingleValueRange>>,
        cells: Cells,
    ) -> Self {
        let field_order =
            cells.fields().keys().cloned().collect::<Vec<String>>();

        let cells = {
            let dimension_len = bounding_subarray
                .iter()
                .map(|r| {
                    usize::try_from(r.num_cells().unwrap())
                        .expect("Too many cells to fit in memory")
                })
                .collect::<Vec<usize>>();
            StructuredCells::new(dimension_len, cells)
        };

        let bounding_subarray = bounding_subarray
            .into_iter()
            .map(|range| {
                let r = RangeInclusive::<i128>::try_from(range).unwrap();
                assert!(r.start() <= r.end());
                r
            })
            .collect::<Vec<RangeInclusive<i128>>>();

        DenseWriteValueTree {
            layout,
            field_order,
            bounding_subarray,
            subarray,
            cells,
            prev_shrink: None,
        }
    }

    fn subarray_current(&self) -> Vec<SingleValueRange> {
        self.subarray
            .iter()
            .map(|tree| tree.current())
            .collect::<Vec<SingleValueRange>>()
    }

    fn cells_for_subarray(
        &self,
        subarray: &[SingleValueRange],
    ) -> StructuredCells {
        let slices = self
            .bounding_subarray
            .iter()
            .zip(subarray.iter())
            .map(|(complete, current)| {
                let current =
                    RangeInclusive::<i128>::try_from(current.clone()).unwrap();

                assert!(current.start() <= current.end());

                assert!(
                    complete.start() <= current.start(),
                    "complete = {complete:?}, current = {current:?}",
                );
                assert!(
                    current.end() <= complete.end(),
                    "complete = {complete:?}, current = {current:?}",
                );

                let start = current.start() - complete.start();
                let end = current.end() - complete.start() + 1;
                let ustart = usize::try_from(start)
                    .expect("Current range is narrower than bounding range");
                let uend = usize::try_from(end)
                    .expect("Current range is narrower than bounding range");
                ustart..uend
            })
            .collect::<Vec<std::ops::Range<usize>>>();

        self.cells.slice(slices)
    }
}

impl Debug for DenseWriteValueTree {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        f.debug_struct("DenseWriteValueTree")
            .field("layout", &self.layout)
            .field("field_order", &self.field_order)
            .field("bounding_subarray", &self.bounding_subarray)
            .field("subarray", &self.subarray_current())
            .field("prev_shrink", &self.prev_shrink)
            .finish()
    }
}

impl ValueTree for DenseWriteValueTree {
    type Value = DenseWriteInput;

    fn current(&self) -> Self::Value {
        let subarray = self.subarray_current();
        let cells = self.cells_for_subarray(&subarray);

        DenseWriteInput {
            layout: self.layout,
            data: cells.into_inner(),
            subarray,
        }
    }

    fn simplify(&mut self) -> bool {
        // try shrinking each dimension in round-robin order,
        // beginning with the dimension after whichever we
        // previously shrunk
        let start = self.prev_shrink.map(|d| d + 1).unwrap_or(0);

        for i in 0..self.subarray.len() {
            let idx = (start + i) % self.subarray.len();
            if self.subarray[idx].simplify() {
                self.prev_shrink = Some(idx);
                return true;
            }
        }

        self.prev_shrink = None;
        false
    }

    fn complicate(&mut self) -> bool {
        // complicate whichever dimension we previously simplified
        if let Some(d) = self.prev_shrink {
            if self.subarray[d].complicate() {
                // we may be able to complicate again, keep prev_shrink
                true
            } else {
                self.prev_shrink = None;
                false
            }
        } else {
            false
        }
    }
}

#[derive(Debug)]
pub struct DenseWriteStrategy {
    schema: Rc<SchemaData>,
    layout: CellOrder,
    params: DenseWriteParameters,
}

impl DenseWriteStrategy {
    pub fn new(
        schema: Rc<SchemaData>,
        layout: CellOrder,
        params: DenseWriteParameters,
    ) -> Self {
        DenseWriteStrategy {
            schema,
            layout,
            params,
        }
    }
}

impl Strategy for DenseWriteStrategy {
    type Tree = DenseWriteValueTree;
    type Value = DenseWriteInput;

    fn new_tree(&self, runner: &mut TestRunner) -> NewTree<Self> {
        /*
         * For simplicity, we will bound the memory used at each dimension
         * rather than keeping a moving product of the accumulated memory
         */
        let memory_limit =
            { self.params.memory_limit / self.schema.domain.dimension.len() };

        if matches!(self.layout, CellOrder::Global) {
            // necessary to align to tile boundaries
            unimplemented!()
        }

        let est_cell_size: usize = self
            .schema
            .fields()
            .map(|field| {
                match field.cell_val_num().unwrap_or(CellValNum::single()) {
                    CellValNum::Fixed(nz) => {
                        /* exact */
                        nz.get() as usize * field.datatype().size()
                    }
                    CellValNum::Var => {
                        /* estimate */
                        let params =
                            <FieldDataParameters as Default>::default();
                        let est_nvalues = (params.value_min_var_size
                            + params.value_max_var_size)
                            / 2;
                        est_nvalues * field.datatype().size()
                    }
                }
            })
            .sum();

        let cell_limit: usize = memory_limit / est_cell_size;

        /* choose maximal subarray for the write, we will shrink within this window */
        let strat_subarray_bounds = self
            .schema
            .domain
            .dimension
            .iter()
            .map(|d| {
                d.subarray_strategy(Some(cell_limit)).expect("Dense dimension subarray not found")
                    .prop_map(|r| {
                        let Range::Single(s) = r else {
                            unreachable!("Dense dimension subarray is not `Range::Single`: {r:?}")
                        };
                        s
                    }).boxed()
            })
            .collect::<Vec<BoxedStrategy<SingleValueRange>>>();

        let bounding_subarray = strat_subarray_bounds
            .into_iter()
            .map(|strat| strat.new_tree(runner).unwrap().current())
            .collect::<Vec<SingleValueRange>>();

        /* prepare tree for each subarray dimension */
        let strat_subarray = bounding_subarray
            .iter()
            .cloned()
            .map(|dim| {
                single_value_range_go!(
                    dim,
                    _DT: Integral,
                    start,
                    end,
                    {
                        (start..=end)
                            .prop_flat_map(move |lower| {
                                (Just(lower), lower..=end).prop_map(
                                    move |(lower, upper)| {
                                        SingleValueRange::from(&[lower, upper])
                                    },
                                )
                            })
                            .boxed()
                    },
                    unreachable!()
                )
            })
            .collect::<Vec<BoxedStrategy<SingleValueRange>>>();

        let mut subarray: Vec<BoxedValueTree<SingleValueRange>> = vec![];
        for range in strat_subarray {
            subarray.push(range.new_tree(runner).unwrap());
        }

        let cells = {
            let ncells = bounding_subarray
                .iter()
                .map(|range| {
                    usize::try_from(range.num_cells().unwrap())
                        .expect("Too many cells to fit in memory")
                })
                .product();
            assert!(ncells > 0);
            let params = CellsParameters {
                schema: Some(CellsStrategySchema::WriteSchema(Rc::clone(
                    &self.schema,
                ))),
                min_records: ncells,
                max_records: ncells,
                ..Default::default()
            };
            any_with::<Cells>(params).new_tree(runner)?.current()
        };

        Ok(DenseWriteValueTree::new(
            self.layout,
            bounding_subarray,
            subarray,
            cells,
        ))
    }
}

impl Arbitrary for DenseWriteInput {
    type Parameters = DenseWriteParameters;
    type Strategy = BoxedStrategy<DenseWriteInput>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        let mut args = args;
        let strat_schema = match args.schema.take() {
            None => any::<SchemaData>().prop_map(Rc::new).boxed(),
            Some(schema) => Just(schema).boxed(),
        };
        let strat_layout = match args.layout.take() {
            None => prop_oneof![
                Just(CellOrder::RowMajor),
                Just(CellOrder::ColumnMajor),
                /* TODO: CellOrder::Global is possible but has more constraints */
            ].boxed(),
            Some(layout) => Just(layout).boxed()
        };

        (strat_schema, strat_layout)
            .prop_flat_map(move |(schema, layout)| {
                DenseWriteStrategy::new(schema, layout, args.clone())
            })
            .boxed()
    }
}

pub type SparseWriteParameters = DenseWriteParameters; // TODO: determine if this should be different

impl Arbitrary for SparseWriteInput {
    type Parameters = SparseWriteParameters;
    type Strategy = BoxedStrategy<SparseWriteInput>;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        if let Some(schema) = params.schema.as_ref() {
            let schema = Rc::clone(schema);
            let cells_params = CellsParameters {
                schema: Some(CellsStrategySchema::WriteSchema(Rc::clone(
                    &schema,
                ))),
                ..Default::default()
            };
            any_with::<Cells>(cells_params)
                .prop_map(move |data| {
                    let dimensions = schema
                        .domain
                        .dimension
                        .iter()
                        .map(|d| (d.name.clone(), d.cell_val_num()))
                        .collect::<Vec<(String, CellValNum)>>();
                    SparseWriteInput { dimensions, data }
                })
                .boxed()
        } else {
            any::<Cells>()
                .prop_flat_map(|data| {
                    (0..data.fields().len(), Just(data)).prop_map(
                        |(ndim, data)| SparseWriteInput {
                            dimensions: data
                                .fields()
                                .iter()
                                .take(ndim)
                                .map(|(fname, fdata)| {
                                    (
                                        fname.clone(),
                                        if fdata.is_cell_single() {
                                            CellValNum::single()
                                        } else {
                                            CellValNum::Var
                                        },
                                    )
                                })
                                .collect::<Vec<(String, CellValNum)>>(),
                            data,
                        },
                    )
                })
                .boxed()
        }
    }
}

impl Arbitrary for DenseWriteSequence {
    type Parameters = DenseWriteSequenceParameters;
    type Strategy = BoxedStrategy<DenseWriteSequence>;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        fn prop_write_sequence(
            schema: Rc<SchemaData>,
            seq_params: DenseWriteSequenceParameters,
        ) -> BoxedStrategy<DenseWriteSequence> {
            let write_params = DenseWriteParameters {
                schema: Some(schema),
                ..seq_params.write.as_ref().clone()
            };
            proptest::collection::vec(
                any_with::<DenseWriteInput>(write_params),
                seq_params.min_writes..=seq_params.max_writes,
            )
            .prop_map(|writes| DenseWriteSequence { writes })
            .boxed()
        }

        if let Some(schema) = params.write.schema.as_ref() {
            prop_write_sequence(Rc::clone(schema), params)
        } else {
            any::<SchemaData>()
                .prop_flat_map(move |schema| {
                    prop_write_sequence(Rc::new(schema), params.clone())
                })
                .boxed()
        }
    }
}

impl Arbitrary for SparseWriteSequence {
    type Parameters = SparseWriteSequenceParameters;
    type Strategy = BoxedStrategy<SparseWriteSequence>;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        pub fn prop_write_sequence(
            schema: Rc<SchemaData>,
            seq_params: SparseWriteSequenceParameters,
        ) -> impl Strategy<Value = SparseWriteSequence> {
            let write_params = SparseWriteParameters {
                schema: Some(schema),
                ..seq_params.write.as_ref().clone()
            };
            proptest::collection::vec(
                any_with::<SparseWriteInput>(write_params),
                seq_params.min_writes..=seq_params.max_writes,
            )
            .prop_map(|writes| SparseWriteSequence { writes })
        }

        if let Some(schema) = params.write.schema.as_ref() {
            prop_write_sequence(Rc::clone(schema), params).boxed()
        } else {
            any::<SchemaData>()
                .prop_flat_map(move |schema| {
                    prop_write_sequence(Rc::new(schema), params.clone())
                })
                .boxed()
        }
    }
}

#[derive(Debug)]
pub enum WriteParameters {
    Dense(DenseWriteParameters),
    Sparse(SparseWriteParameters),
}

impl WriteParameters {
    pub fn default_for(schema: Rc<SchemaData>) -> Self {
        match schema.array_type {
            ArrayType::Dense => Self::Dense(DenseWriteParameters {
                schema: Some(schema),
                ..Default::default()
            }),
            ArrayType::Sparse => Self::Sparse(SparseWriteParameters {
                schema: Some(schema),
                ..Default::default()
            }),
        }
    }
}

impl Default for WriteParameters {
    fn default() -> Self {
        Self::Dense(DenseWriteParameters::default())
    }
}

impl Arbitrary for WriteInput {
    type Parameters = WriteParameters;
    type Strategy = BoxedStrategy<WriteInput>;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        match params {
            WriteParameters::Dense(d) => any_with::<DenseWriteInput>(d)
                .prop_map(WriteInput::Dense)
                .boxed(),
            WriteParameters::Sparse(s) => any_with::<SparseWriteInput>(s)
                .prop_map(WriteInput::Sparse)
                .boxed(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct WriteSequenceParametersImpl<W> {
    pub write: Rc<W>,
    pub min_writes: usize,
    pub max_writes: usize,
}

pub type DenseWriteSequenceParameters =
    WriteSequenceParametersImpl<DenseWriteParameters>;
pub type SparseWriteSequenceParameters =
    WriteSequenceParametersImpl<SparseWriteParameters>;

impl<W> WriteSequenceParametersImpl<W> {
    pub fn min_writes_default() -> usize {
        **tiledb_proptest_config::TILEDB_STRATEGY_WRITE_SEQUENCE_PARAMETERS_MIN_WRITES
    }

    pub fn max_writes_default() -> usize {
        **tiledb_proptest_config::TILEDB_STRATEGY_WRITE_SEQUENCE_PARAMETERS_MAX_WRITES
    }
}

impl<W> Default for WriteSequenceParametersImpl<W>
where
    W: Default,
{
    fn default() -> Self {
        WriteSequenceParametersImpl {
            write: Rc::new(Default::default()),
            min_writes: Self::min_writes_default(),
            max_writes: Self::max_writes_default(),
        }
    }
}

#[derive(Debug)]
pub enum WriteSequenceParameters {
    Dense(DenseWriteSequenceParameters),
    Sparse(SparseWriteSequenceParameters),
}

impl WriteSequenceParameters {
    pub fn default_for(schema: Rc<SchemaData>) -> Self {
        match schema.array_type {
            ArrayType::Dense => Self::Dense(DenseWriteSequenceParameters {
                write: Rc::new(DenseWriteParameters {
                    schema: Some(schema),
                    ..Default::default()
                }),
                min_writes: DenseWriteSequenceParameters::min_writes_default(),
                max_writes: DenseWriteSequenceParameters::max_writes_default(),
            }),
            ArrayType::Sparse => Self::Sparse(SparseWriteSequenceParameters {
                write: Rc::new(SparseWriteParameters {
                    schema: Some(schema),
                    ..Default::default()
                }),
                min_writes: SparseWriteSequenceParameters::min_writes_default(),
                max_writes: SparseWriteSequenceParameters::max_writes_default(),
            }),
        }
    }
}

impl Default for WriteSequenceParameters {
    fn default() -> Self {
        Self::Dense(Default::default())
    }
}

impl Arbitrary for WriteSequence {
    type Parameters = WriteSequenceParameters;
    type Strategy = BoxedStrategy<WriteSequence>;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        match params {
            WriteSequenceParameters::Dense(d) => {
                any_with::<DenseWriteSequence>(d)
                    .prop_map(Self::Dense)
                    .boxed()
            }
            WriteSequenceParameters::Sparse(s) => {
                any_with::<SparseWriteSequence>(s)
                    .prop_map(Self::Sparse)
                    .boxed()
            }
        }
    }
}
