use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::ops::RangeInclusive;
use std::rc::Rc;

use proptest::prelude::*;
use proptest::strategy::{NewTree, ValueTree};
use proptest::test_runner::TestRunner;
use serde_json::json;

use crate::array::{ArrayType, CellOrder, CellValNum, SchemaData};
use crate::datatype::LogicalType;
use crate::filter::strategy::Requirements as FilterRequirements;
use crate::query::read::{
    CallbackVarArgReadBuilder, FieldMetadata, ManagedBuffer, MapAdapter,
    RawReadHandle, TypedReadHandle,
};
use crate::query::strategy::{
    Cells, CellsConstructor, CellsParameters, CellsStrategySchema,
    FieldDataParameters, RawResultCallback, StructuredCells,
};
use crate::query::{QueryBuilder, ReadQuery, ReadQueryBuilder, WriteBuilder};
use crate::range::{Range, SingleValueRange};
use crate::{fn_typed, single_value_range_go, Result as TileDBResult};

type BoxedValueTree<T> = Box<dyn ValueTree<Value = T>>;

// now that we're actually writing data we will hit the fun bugs.
// there are several in the filter pipeline, so we must heavily
// restrict what is allowed until the bugs are fixed.
fn query_write_filter_requirements() -> FilterRequirements {
    FilterRequirements {
        allow_bit_reduction: false,  // SC-47560
        allow_positive_delta: false, // nothing yet to ensure sort order
        allow_scale_float: false,
        allow_xor: false,               // SC-47328
        allow_compression_rle: false, // probably can be enabled but nontrivial
        allow_compression_dict: false, // probably can be enabled but nontrivial
        allow_compression_delta: false, // SC-47328
        ..Default::default()
    }
}

#[derive(Debug)]
pub struct DenseWriteInput {
    pub layout: CellOrder,
    pub data: Cells,
    pub subarray: Vec<SingleValueRange>,
}

impl DenseWriteInput {
    pub fn attach_write<'data>(
        &'data self,
        b: WriteBuilder<'data>,
    ) -> TileDBResult<WriteBuilder<'data>> {
        let mut subarray = self.data.attach_write(b)?.start_subarray()?;

        for i in 0..self.subarray.len() {
            subarray = subarray.add_range(i, self.subarray[i].clone())?;
        }

        subarray.finish_subarray()?.layout(self.layout)
    }

    pub fn attach_read<'data, B>(
        &'data self,
        b: B,
    ) -> TileDBResult<
        CallbackVarArgReadBuilder<
            'data,
            MapAdapter<CellsConstructor, RawResultCallback>,
            B,
        >,
    >
    where
        B: ReadQueryBuilder<'data>,
    {
        let mut subarray = b.start_subarray()?;

        for i in 0..self.subarray.len() {
            subarray = subarray.add_range(i, self.subarray[i].clone())?;
        }

        let b: B = subarray.finish_subarray()?.layout(self.layout)?;

        Ok(self.data.attach_read(b)?.map(CellsConstructor::new()))
    }
}

#[derive(Clone, Debug, Default)]
pub struct DenseWriteParameters {
    schema: Option<Rc<SchemaData>>,
    layout: Option<CellOrder>,
    memory_limit: Option<usize>,
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
                    "complete = {:?}, current = {:?}",
                    complete,
                    current
                );
                assert!(
                    current.end() <= complete.end(),
                    "complete = {:?}, current = {:?}",
                    complete,
                    current
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
        let json = json!({
            "layout": self.layout,
            "field_order": self.field_order,
            "bounding_subarray": self.bounding_subarray,
            "subarray": self.subarray_current(),
            "prev_shrink": self.prev_shrink
        });
        write!(f, "{}", json)
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
        let memory_limit: usize = {
            const MEMORY_LIMIT_DEFAULT: usize = 16 * 1024; // chosen arbitrarily
            let memory_limit =
                self.params.memory_limit.unwrap_or(MEMORY_LIMIT_DEFAULT);
            memory_limit / self.schema.domain.dimension.len()
        };

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
                        nz.get() as usize * field.datatype().size() as usize
                    }
                    CellValNum::Var => {
                        /* estimate */
                        let params =
                            <FieldDataParameters as Default>::default();
                        let est_nvalues = (params.value_min_var_size
                            + params.value_max_var_size)
                            / 2;
                        est_nvalues * field.datatype().size() as usize
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
            .map(|d| d.subarray_strategy(Some(cell_limit)).unwrap())
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
            None => {
                let schema_req = crate::array::schema::strategy::Requirements {
                    domain: Some(Rc::new(
                        crate::array::domain::strategy::Requirements {
                            array_type: Some(ArrayType::Dense),
                            num_dimensions: 1..=1,
                            ..Default::default()
                        },
                    )),
                    num_attributes: 1..=1,
                    attribute_filters: Some(Rc::new(
                        query_write_filter_requirements(),
                    )),
                    offsets_filters: Some(Rc::new(
                        query_write_filter_requirements(),
                    )),
                    validity_filters: Some(Rc::new(
                        query_write_filter_requirements(),
                    )),
                };
                any_with::<SchemaData>(Rc::new(schema_req))
                    .prop_map(Rc::new)
                    .boxed()
            }
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

pub struct SparseWriteInput {
    pub data: Cells,
}

#[derive(Debug)]
pub struct DenseWriteSequence {
    writes: Vec<DenseWriteInput>,
}

impl Arbitrary for DenseWriteSequence {
    type Parameters = DenseWriteParameters;
    type Strategy = BoxedStrategy<DenseWriteSequence>;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        if let Some(schema) = params.schema.as_ref() {
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

impl IntoIterator for DenseWriteSequence {
    type Item = DenseWriteInput;
    type IntoIter = <Vec<Self::Item> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.writes.into_iter()
    }
}

pub fn prop_write_sequence(
    schema: Rc<SchemaData>,
    params: DenseWriteParameters,
) -> impl Strategy<Value = DenseWriteSequence> {
    let params = DenseWriteParameters {
        schema: Some(schema),
        ..params
    };

    const MAX_WRITES: usize = 8;
    proptest::collection::vec(
        any_with::<DenseWriteInput>(params),
        0..MAX_WRITES,
    )
    .prop_map(|writes| DenseWriteSequence { writes })
}

#[derive(Debug)]
pub enum WriteInput {
    Dense(DenseWriteInput),
}

impl WriteInput {
    pub fn cells(&self) -> &Cells {
        let Self::Dense(ref dense) = self;
        &dense.data
    }

    pub fn domain(&self) -> Vec<Range> {
        let Self::Dense(ref dense) = self;
        dense
            .subarray
            .clone()
            .into_iter()
            .map(Range::from)
            .collect::<Vec<Range>>()
    }

    pub fn unwrap_cells(self) -> Cells {
        let Self::Dense(dense) = self;
        dense.data
    }

    pub fn attach_write<'data>(
        &'data self,
        b: WriteBuilder<'data>,
    ) -> TileDBResult<WriteBuilder<'data>> {
        let Self::Dense(ref d) = self;
        d.attach_write(b)
    }

    pub fn attach_read<'data, B>(
        &'data self,
        b: B,
    ) -> TileDBResult<
        CallbackVarArgReadBuilder<
            'data,
            MapAdapter<CellsConstructor, RawResultCallback>,
            B,
        >,
    >
    where
        B: ReadQueryBuilder<'data>,
    {
        let Self::Dense(ref d) = self;
        d.attach_read(b)
    }
}

#[derive(Debug)]
pub enum WriteParameters {
    Dense(DenseWriteParameters),
}

impl WriteParameters {
    pub fn default_for(schema: Rc<SchemaData>) -> Self {
        match schema.array_type {
            ArrayType::Dense => Self::Dense(DenseWriteParameters {
                schema: Some(schema),
                ..Default::default()
            }),
            ArrayType::Sparse => unimplemented!(),
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
        let WriteParameters::Dense(d) = params;
        any_with::<DenseWriteInput>(d)
            .prop_map(WriteInput::Dense)
            .boxed()
    }
}

#[derive(Debug)]
pub enum WriteSequence {
    Dense(DenseWriteSequence),
}

impl From<WriteInput> for WriteSequence {
    fn from(value: WriteInput) -> Self {
        let WriteInput::Dense(dense) = value;
        Self::Dense(DenseWriteSequence {
            writes: vec![dense],
        })
    }
}

impl IntoIterator for WriteSequence {
    type Item = WriteInput;
    type IntoIter = WriteSequenceIter;

    fn into_iter(self) -> Self::IntoIter {
        let Self::Dense(dense) = self;
        WriteSequenceIter::Dense(dense.into_iter())
    }
}

impl Arbitrary for WriteSequence {
    type Parameters = Option<Rc<SchemaData>>;
    type Strategy = BoxedStrategy<WriteSequence>;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        let strat_schema = |schema: Rc<SchemaData>| match schema.array_type {
            ArrayType::Dense => {
                let params = DenseWriteParameters {
                    schema: Some(schema),
                    ..Default::default()
                };

                any_with::<DenseWriteSequence>(params)
                    .prop_map(WriteSequence::Dense)
                    .boxed()
            }
            ArrayType::Sparse => unimplemented!(),
        };

        if let Some(schema) = params {
            strat_schema(schema)
        } else {
            any::<SchemaData>()
                .prop_flat_map(move |schema| strat_schema(Rc::new(schema)))
                .boxed()
        }
    }
}

pub enum WriteSequenceIter {
    Dense(<DenseWriteSequence as IntoIterator>::IntoIter),
}

impl Iterator for WriteSequenceIter {
    type Item = WriteInput;

    fn next(&mut self) -> Option<Self::Item> {
        let Self::Dense(ref mut dense) = self;
        dense.next().map(WriteInput::Dense)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::array::{Array, Mode};
    use crate::query::{
        Query, QueryBuilder, ReadBuilder, ReadQuery, WriteBuilder,
    };
    use crate::{Context, Factory};

    fn do_write_readback(
        ctx: &Context,
        schema_spec: Rc<SchemaData>,
        write_sequence: WriteSequence,
    ) {
        let tempdir = TempDir::new().expect("Error creating temp dir");
        let uri = String::from("file:///")
            + tempdir.path().join("array").to_str().unwrap();

        let schema_in = schema_spec
            .create(ctx)
            .expect("Error constructing arbitrary schema");
        Array::create(ctx, &uri, schema_in).expect("Error creating array");

        let mut array =
            Array::open(ctx, &uri, Mode::Write).expect("Error opening array");

        let mut accumulated_domain: Option<Vec<Range>> = None;
        let mut accumulated_write: Option<Cells> = None;

        for write in write_sequence {
            /* write data and preserve ranges for sanity check */
            let write_ranges = {
                let write = write
                    .attach_write(
                        WriteBuilder::new(array)
                            .expect("Error building write query"),
                    )
                    .expect("Error building write query")
                    .build();
                write.submit().expect("Error running write query");

                let write_ranges = write.subarray().unwrap().ranges().unwrap();

                array = write.finalize().expect("Error finalizing write query");

                write_ranges
            };

            array = Array::open(ctx, array.uri(), Mode::Read).unwrap();

            /* NB: results are not read back in a defined order, so we must sort and compare */

            /* first, read back what we just wrote */
            {
                let mut read = write
                    .attach_read(ReadBuilder::new(array).unwrap())
                    .unwrap()
                    .build();

                {
                    let read_ranges =
                        read.subarray().unwrap().ranges().unwrap();
                    assert_eq!(write_ranges, read_ranges);
                }

                let (mut cells, _) = read.execute().unwrap();

                /* `cells` should match the write */
                {
                    let write_sorted = write.cells().sorted();
                    cells.sort();
                    assert_eq!(cells, write_sorted);
                }

                array = read.finalize().unwrap();
            }

            /* the most recent fragment info should match what we just wrote */
            {
                let write_domain = write.domain();

                let fi = array.fragment_info().unwrap();
                let this_fragment =
                    fi.get_fragment(fi.num_fragments().unwrap() - 1).unwrap();
                let nonempty_domain = this_fragment
                    .non_empty_domain()
                    .unwrap()
                    .into_iter()
                    .map(|typed| typed.range)
                    .collect::<Vec<_>>();

                assert_eq!(write_domain, nonempty_domain);
            }

            /* then check array non-empty domain */
            if accumulated_domain.as_mut().is_some() {
                /* TODO: range extension, when we update test for a write sequence */
                unimplemented!()
            } else {
                accumulated_domain = Some(write.domain());
            }

            if let Some(acc) = accumulated_domain.as_ref() {
                let nonempty = array
                    .nonempty_domain()
                    .unwrap()
                    .unwrap()
                    .into_iter()
                    .map(|typed| typed.range)
                    .collect::<Vec<_>>();
                assert_eq!(*acc, nonempty);
            }

            /* update accumulated expected array data */
            if let Some(acc) = accumulated_write.as_mut() {
                acc.copy_from(write.unwrap_cells())
            } else {
                accumulated_write = Some(write.unwrap_cells());
            }

            /* TODO: read all ranges and check against accumulated writes */
        }
    }

    /// Test that a single write can be read back correctly
    #[test]
    fn write_once_readback() {
        let ctx = Context::new().expect("Error creating context");

        let requirements = crate::array::schema::strategy::Requirements {
            domain: Some(Rc::new(
                crate::array::domain::strategy::Requirements {
                    array_type: Some(ArrayType::Dense),
                    num_dimensions: 1..=1,
                    ..Default::default()
                },
            )),
            num_attributes: 1..=1,
            attribute_filters: Some(Rc::new(query_write_filter_requirements())),
            offsets_filters: Some(Rc::new(query_write_filter_requirements())),
            validity_filters: Some(Rc::new(query_write_filter_requirements())),
        };

        let strategy = any_with::<SchemaData>(Rc::new(requirements))
            .prop_flat_map(|schema| {
                let schema = Rc::new(schema);
                (
                    Just(Rc::clone(&schema)),
                    any_with::<WriteInput>(WriteParameters::default_for(
                        schema,
                    ))
                    .prop_map(WriteSequence::from),
                )
            });

        proptest!(|((schema_spec, write_sequence) in strategy)| {
            do_write_readback(&ctx, schema_spec, write_sequence)
        })
    }

    /// Test that each write in the sequence can be read back correctly at the right timestamp
    #[test]
    #[ignore]
    fn write_sequence_readback() {
        let ctx = Context::new().expect("Error creating context");

        let strategy = any::<SchemaData>().prop_flat_map(|schema| {
            let schema = Rc::new(schema);
            (
                Just(Rc::clone(&schema)),
                any_with::<WriteSequence>(Some(Rc::clone(&schema))),
            )
        });

        proptest!(|((schema_spec, write_sequence) in strategy)| {
            do_write_readback(&ctx, schema_spec, write_sequence)
        })
    }
}
