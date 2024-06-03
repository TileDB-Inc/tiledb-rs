use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::ops::{Deref, RangeInclusive};
use std::rc::Rc;

use proptest::prelude::*;
use proptest::strategy::{NewTree, ValueTree};
use proptest::test_runner::TestRunner;
use serde_json::json;

use crate::array::{ArrayType, CellOrder, CellValNum, SchemaData};
use crate::datatype::physical::BitsOrd;
use crate::filter::strategy::Requirements as FilterRequirements;
use crate::query::read::{CallbackVarArgReadBuilder, MapAdapter};
use crate::query::strategy::{
    Cells, CellsConstructor, CellsParameters, CellsStrategySchema,
    FieldDataParameters, RawResultCallback, StructuredCells,
};
use crate::query::{QueryBuilder, ReadQueryBuilder, WriteBuilder};
use crate::range::{NonEmptyDomain, Range, SingleValueRange};
use crate::{
    single_value_range_go, typed_field_data_go, Result as TileDBResult,
};

type BoxedValueTree<T> = Box<dyn ValueTree<Value = T>>;

/// Returns a base set of requirements for filters to be used
/// in write queries. Requirements are chosen to either avoid
/// constraints on input (e.g. positive delta filtering requires
/// sorted input, float scale filtering is not invertible)
/// or to avoid issues in the tiledb core library in as
/// many scenarios as possible.
// now that we're actually writing data we will hit the fun bugs.
// there are several in the filter pipeline, so we must heavily
// restrict what is allowed until the bugs are fixed.
pub fn query_write_filter_requirements() -> FilterRequirements {
    FilterRequirements {
        allow_bit_reduction: false,     // SC-47560
        allow_positive_delta: false,    // nothing yet to ensure sort order
        allow_scale_float: false,       // not invertible due to precision loss
        allow_xor: false,               // SC-47328
        allow_compression_rle: false, // probably can be enabled but nontrivial
        allow_compression_dict: false, // probably can be enabled but nontrivial
        allow_compression_delta: false, // SC-47328
        ..Default::default()
    }
}

/// Returns a base set of schema requirements for running a query.
/// Requirements are chosen to either avoid constraints on write input
/// or to avoid issues in the tiledb core library in as many scenarios as possible.
pub fn query_write_schema_requirements(
    array_type: Option<ArrayType>,
) -> crate::array::schema::strategy::Requirements {
    crate::array::schema::strategy::Requirements {
        domain: Some(Rc::new(crate::array::domain::strategy::Requirements {
            array_type,
            num_dimensions: 1..=1,
            dimension: Some(crate::array::dimension::strategy::Requirements {
                filters: Some(Rc::new(query_write_filter_requirements())),
                ..Default::default()
            }),
            ..Default::default()
        })),
        num_attributes: 1..=1,
        attribute_filters: Some(Rc::new(query_write_filter_requirements())),
        coordinates_filters: Some(Rc::new(query_write_filter_requirements())),
        offsets_filters: Some(Rc::new(query_write_filter_requirements())),
        validity_filters: Some(Rc::new(query_write_filter_requirements())),
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
    /// Prepares a write query to insert data from this write.
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

    /// Prepares a read query to read the fields written by this operation
    /// restricted to the subarray represented by this write.
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
    pub schema: Option<Rc<SchemaData>>,
    pub layout: Option<CellOrder>,
    pub memory_limit: Option<usize>,
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
                let schema_req =
                    query_write_schema_requirements(Some(ArrayType::Dense));
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

pub type SparseWriteParameters = DenseWriteParameters; // TODO: determine if this should be different

#[derive(Debug)]
pub struct SparseWriteInput {
    pub dimensions: Vec<(String, CellValNum)>,
    pub data: Cells,
}

impl SparseWriteInput {
    /// Returns the minimum bounding rectangle containing all
    /// the coordinates of this write operation.
    pub fn domain(&self) -> Option<NonEmptyDomain> {
        self.dimensions
            .iter()
            .map(|(dim, cell_val_num)| {
                let dim_cells = self.data.fields().get(dim).unwrap();
                Some(typed_field_data_go!(
                    dim_cells,
                    _DT,
                    ref dim_cells,
                    {
                        let min =
                            *dim_cells.iter().min_by(|l, r| l.bits_cmp(r))?;
                        let max =
                            *dim_cells.iter().max_by(|l, r| l.bits_cmp(r))?;
                        Range::from(&[min, max])
                    },
                    {
                        let min = dim_cells
                            .iter()
                            .min_by(|l, r| l.bits_cmp(r))?
                            .clone()
                            .into_boxed_slice();
                        let max = dim_cells
                            .iter()
                            .max_by(|l, r| l.bits_cmp(r))?
                            .clone()
                            .into_boxed_slice();
                        match cell_val_num {
                            CellValNum::Fixed(_) => {
                                Range::try_from((*cell_val_num, min, max))
                                    .unwrap()
                            }
                            CellValNum::Var => Range::from((min, max)),
                        }
                    }
                ))
            })
            .collect::<Option<NonEmptyDomain>>()
    }

    /// Prepares a write query to insert data from this write operation.
    pub fn attach_write<'data>(
        &'data self,
        b: WriteBuilder<'data>,
    ) -> TileDBResult<WriteBuilder<'data>> {
        self.data.attach_write(b)
    }

    /// Prepares a read query to read the fields written by this operation.
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
        Ok(self.data.attach_read(b)?.map(CellsConstructor::new()))
    }
}

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
                        .map(|d| {
                            (
                                d.name.clone(),
                                d.cell_val_num.unwrap_or(CellValNum::single()),
                            )
                        })
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

#[derive(Debug)]
pub struct DenseWriteSequence {
    writes: Vec<DenseWriteInput>,
}

impl Deref for DenseWriteSequence {
    type Target = Vec<DenseWriteInput>;
    fn deref(&self) -> &Self::Target {
        &self.writes
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

impl IntoIterator for DenseWriteSequence {
    type Item = DenseWriteInput;
    type IntoIter = <Vec<Self::Item> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.writes.into_iter()
    }
}

#[derive(Debug)]
pub struct SparseWriteSequence {
    writes: Vec<SparseWriteInput>,
}

impl Deref for SparseWriteSequence {
    type Target = Vec<SparseWriteInput>;
    fn deref(&self) -> &Self::Target {
        &self.writes
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

impl IntoIterator for SparseWriteSequence {
    type Item = SparseWriteInput;
    type IntoIter = <Vec<Self::Item> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.writes.into_iter()
    }
}

#[derive(Debug)]
pub enum WriteInput {
    Dense(DenseWriteInput),
    Sparse(SparseWriteInput),
}

impl WriteInput {
    /// Returns a reference to the cells of input of this write operation.
    pub fn cells(&self) -> &Cells {
        match self {
            Self::Dense(ref dense) => &dense.data,
            Self::Sparse(ref sparse) => &sparse.data,
        }
    }

    /// Returns the minimum bounding rectangle containing
    /// the coordinates of this write operation.
    pub fn domain(&self) -> Option<NonEmptyDomain> {
        match self {
            Self::Dense(ref dense) => Some(
                dense
                    .subarray
                    .clone()
                    .into_iter()
                    .map(Range::from)
                    .collect::<NonEmptyDomain>(),
            ),
            Self::Sparse(ref sparse) => sparse.domain(),
        }
    }

    /// Returns the subarray for this write operation,
    /// if it is a dense write. Returns `None` otherwise.
    pub fn subarray(&self) -> Option<NonEmptyDomain> {
        if let Self::Dense(_) = self {
            self.domain()
        } else {
            None
        }
    }

    /// Consumes `self` and returns the underlying test data.
    pub fn unwrap_cells(self) -> Cells {
        match self {
            Self::Dense(dense) => dense.data,
            Self::Sparse(sparse) => sparse.data,
        }
    }

    /// Prepares a write queryto insert data from this write operation.
    pub fn attach_write<'data>(
        &'data self,
        b: WriteBuilder<'data>,
    ) -> TileDBResult<WriteBuilder<'data>> {
        match self {
            Self::Dense(ref d) => d.attach_write(b),
            Self::Sparse(ref s) => s.attach_write(b),
        }
    }

    /// Prepares a read query to read the fields written by this operation.
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
        match self {
            Self::Dense(ref d) => d.attach_read(b),
            Self::Sparse(ref s) => s.attach_read(b),
        }
    }
}

pub enum WriteInputRef<'a> {
    Dense(&'a DenseWriteInput),
    Sparse(&'a SparseWriteInput),
}

impl<'a> WriteInputRef<'a> {
    /// Returns a reference to the cells of input of this write operation.
    pub fn cells(&self) -> &Cells {
        match self {
            Self::Dense(dense) => &dense.data,
            Self::Sparse(sparse) => &sparse.data,
        }
    }

    /// Returns the minimum bounding rectangle containing
    /// the coordinates of this write operation.
    pub fn domain(&self) -> Option<NonEmptyDomain> {
        match self {
            Self::Dense(dense) => Some(
                dense
                    .subarray
                    .clone()
                    .into_iter()
                    .map(Range::from)
                    .collect::<NonEmptyDomain>(),
            ),
            Self::Sparse(sparse) => sparse.domain(),
        }
    }

    /// Returns the subarray for this write operation,
    /// if it is a dense write. Returns `None` otherwise.
    pub fn subarray(&self) -> Option<NonEmptyDomain> {
        if let Self::Dense(_) = self {
            self.domain()
        } else {
            None
        }
    }

    /// Prepares a write queryto insert data from this write operation.
    pub fn attach_write<'data>(
        &'data self,
        b: WriteBuilder<'data>,
    ) -> TileDBResult<WriteBuilder<'data>> {
        match self {
            Self::Dense(d) => d.attach_write(b),
            Self::Sparse(s) => s.attach_write(b),
        }
    }

    /// Prepares a read query to read the fields written by this operation.
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
        match self {
            Self::Dense(d) => d.attach_read(b),
            Self::Sparse(s) => s.attach_read(b),
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
    pub const DEFAULT_MIN_WRITES: usize = 1;
    pub const DEFAULT_MAX_WRITES: usize = 8;
}

impl<W> Default for WriteSequenceParametersImpl<W>
where
    W: Default,
{
    fn default() -> Self {
        WriteSequenceParametersImpl {
            write: Rc::new(Default::default()),
            min_writes: Self::DEFAULT_MIN_WRITES,
            max_writes: Self::DEFAULT_MAX_WRITES,
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
                min_writes: DenseWriteSequenceParameters::DEFAULT_MIN_WRITES,
                max_writes: DenseWriteSequenceParameters::DEFAULT_MAX_WRITES,
            }),
            ArrayType::Sparse => Self::Sparse(SparseWriteSequenceParameters {
                write: Rc::new(SparseWriteParameters {
                    schema: Some(schema),
                    ..Default::default()
                }),
                min_writes: SparseWriteSequenceParameters::DEFAULT_MIN_WRITES,
                max_writes: SparseWriteSequenceParameters::DEFAULT_MAX_WRITES,
            }),
        }
    }
}

impl Default for WriteSequenceParameters {
    fn default() -> Self {
        Self::Dense(Default::default())
    }
}

#[derive(Debug)]
pub enum WriteSequence {
    Dense(DenseWriteSequence),
    Sparse(SparseWriteSequence),
}

impl WriteSequence {
    pub fn iter(&self) -> WriteSequenceRefIter {
        self.into_iter()
    }
}

impl From<WriteInput> for WriteSequence {
    fn from(value: WriteInput) -> Self {
        match value {
            WriteInput::Dense(dense) => Self::Dense(DenseWriteSequence {
                writes: vec![dense],
            }),
            WriteInput::Sparse(sparse) => Self::Sparse(SparseWriteSequence {
                writes: vec![sparse],
            }),
        }
    }
}

impl IntoIterator for WriteSequence {
    type Item = WriteInput;
    type IntoIter = WriteSequenceIter;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::Dense(dense) => WriteSequenceIter::Dense(dense.into_iter()),
            Self::Sparse(sparse) => {
                WriteSequenceIter::Sparse(sparse.into_iter())
            }
        }
    }
}

impl<'a> IntoIterator for &'a WriteSequence {
    type Item = WriteInputRef<'a>;
    type IntoIter = WriteSequenceRefIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match *self {
            WriteSequence::Dense(ref dense) => {
                WriteSequenceRefIter::Dense(dense.iter())
            }
            WriteSequence::Sparse(ref sparse) => {
                WriteSequenceRefIter::Sparse(sparse.iter())
            }
        }
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

pub enum WriteSequenceIter {
    Dense(<DenseWriteSequence as IntoIterator>::IntoIter),
    Sparse(<SparseWriteSequence as IntoIterator>::IntoIter),
}

impl Iterator for WriteSequenceIter {
    type Item = WriteInput;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Dense(ref mut dense) => dense.next().map(WriteInput::Dense),
            Self::Sparse(ref mut sparse) => {
                sparse.next().map(WriteInput::Sparse)
            }
        }
    }
}

pub enum WriteSequenceRefIter<'a> {
    Dense(<&'a Vec<DenseWriteInput> as IntoIterator>::IntoIter),
    Sparse(<&'a Vec<SparseWriteInput> as IntoIterator>::IntoIter),
}

impl<'a> Iterator for WriteSequenceRefIter<'a> {
    type Item = WriteInputRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Dense(ref mut dense) => {
                dense.next().map(WriteInputRef::Dense)
            }
            Self::Sparse(ref mut sparse) => {
                sparse.next().map(WriteInputRef::Sparse)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tiledb_test_utils::{self, TestArrayUri};

    use super::*;
    use crate::array::{Array, ArrayOpener, Mode};
    use crate::error::Error;
    use crate::query::{
        Query, QueryBuilder, ReadBuilder, ReadQuery, WriteBuilder,
    };
    use crate::{Context, Factory};

    struct DenseCellsAccumulator {
        // TODO: implement accepting more than one write for dense write sequence
        write: Option<DenseWriteInput>,
    }

    impl DenseCellsAccumulator {
        pub fn new(_: &SchemaData) -> Self {
            DenseCellsAccumulator { write: None }
        }

        pub fn cells(&self) -> &Cells {
            // will not be called until first cells are written
            &self.write.as_ref().unwrap().data
        }

        pub fn accumulate(&mut self, write: DenseWriteInput) {
            if self.write.is_some() {
                unimplemented!()
            }
            self.write = Some(write)
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
            // TODO: this is not correct as we accumulate multiple writes
            self.write.as_ref().unwrap().attach_read(b)
        }
    }

    struct SparseCellsAccumulator {
        cells: Option<Cells>,
        dedup_keys: Option<Vec<String>>,
    }

    impl SparseCellsAccumulator {
        pub fn new(schema: &SchemaData) -> Self {
            let dedup_keys = if schema.allow_duplicates.unwrap_or(false) {
                None
            } else {
                Some(
                    schema
                        .domain
                        .dimension
                        .iter()
                        .map(|d| d.name.clone())
                        .collect::<Vec<String>>(),
                )
            };
            SparseCellsAccumulator {
                cells: None,
                dedup_keys,
            }
        }

        pub fn cells(&self) -> &Cells {
            // will not be called until first cells arrive
            self.cells.as_ref().unwrap()
        }

        /// Update state representing what we expect to see in the array.
        /// For a sparse array this means adding this write's coordinates,
        /// overwriting the old coordinates if they overlap.
        pub fn accumulate(&mut self, mut write: SparseWriteInput) {
            if let Some(cells) = self.cells.take() {
                write.data.extend(cells);
                if let Some(dedup_keys) = self.dedup_keys.as_ref() {
                    self.cells = Some(write.data.dedup(dedup_keys));
                } else {
                    self.cells = Some(write.data);
                }
            } else {
                self.cells = Some(write.data);
            }
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
            Ok(self.cells().attach_read(b)?.map(CellsConstructor::new()))
        }
    }

    enum CellsAccumulator {
        Dense(DenseCellsAccumulator),
        Sparse(SparseCellsAccumulator),
    }

    impl CellsAccumulator {
        pub fn new(schema: &SchemaData) -> Self {
            match schema.array_type {
                ArrayType::Dense => {
                    Self::Dense(DenseCellsAccumulator::new(schema))
                }
                ArrayType::Sparse => {
                    Self::Sparse(SparseCellsAccumulator::new(schema))
                }
            }
        }

        pub fn cells(&self) -> &Cells {
            match self {
                Self::Dense(ref d) => d.cells(),
                Self::Sparse(ref s) => s.cells(),
            }
        }

        pub fn accumulate(&mut self, write: WriteInput) {
            match write {
                WriteInput::Sparse(w) => {
                    let Self::Sparse(ref mut sparse) = self else {
                        unreachable!()
                    };
                    sparse.accumulate(w)
                }
                WriteInput::Dense(w) => {
                    let Self::Dense(ref mut dense) = self else {
                        unreachable!()
                    };
                    dense.accumulate(w)
                }
            }
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
            match self {
                Self::Dense(ref d) => d.attach_read(b),
                Self::Sparse(ref s) => s.attach_read(b),
            }
        }
    }

    fn do_write_readback(
        ctx: &Context,
        schema_spec: Rc<SchemaData>,
        write_sequence: WriteSequence,
    ) -> TileDBResult<()> {
        let test_uri = tiledb_test_utils::get_uri_generator()
            .map_err(|e| Error::Other(e.to_string()))?;
        let uri = test_uri
            .with_path("array")
            .map_err(|e| Error::Other(e.to_string()))?;

        let schema_in = schema_spec
            .create(ctx)
            .expect("Error constructing arbitrary schema");
        Array::create(ctx, &uri, schema_in).expect("Error creating array");

        let mut accumulated_domain: Option<NonEmptyDomain> = None;
        let mut accumulated_write = CellsAccumulator::new(&schema_spec);

        /*
         * Results do not come back in a defined order, so we must sort and
         * compare. Writes currently have to write all fields.
         */
        let sort_keys = match write_sequence {
            WriteSequence::Dense(_) => schema_spec
                .attributes
                .iter()
                .map(|f| f.name.clone())
                .collect::<Vec<String>>(),
            WriteSequence::Sparse(_) => schema_spec
                .fields()
                .map(|f| f.name().to_owned())
                .collect::<Vec<String>>(),
        };

        for write in write_sequence {
            /* write data and preserve ranges for sanity check */
            let write_ranges = {
                let array = Array::open(ctx, &uri, Mode::Write)
                    .expect("Error opening array");

                let write_query = write
                    .attach_write(
                        WriteBuilder::new(array)
                            .expect("Error building write query"),
                    )
                    .expect("Error building write query")
                    .build();
                write_query.submit().expect("Error running write query");

                let write_ranges = if let Some(ranges) = write.subarray() {
                    let generic_ranges = ranges
                        .iter()
                        .cloned()
                        .map(|r| vec![r])
                        .collect::<Vec<Vec<Range>>>();
                    assert_eq!(
                        generic_ranges,
                        write_query.subarray().unwrap().ranges().unwrap()
                    );
                    Some(generic_ranges)
                } else {
                    None
                };

                let _ = write_query
                    .finalize()
                    .expect("Error finalizing write query");

                write_ranges
            };

            if write.cells().is_empty() {
                // in this case, writing and finalizing does not create a new fragment
                // TODO
                continue;
            }

            /* NB: results are not read back in a defined order, so we must sort and compare */

            let mut array = ArrayOpener::new(ctx, &uri, Mode::Read)
                .unwrap()
                .open()
                .unwrap();

            /*
             * First check fragment - its domain should match what we just wrote, and we need the
             * timestamp so we can read back only this fragment
             */
            let [timestamp_min, timestamp_max] = {
                let fi = array.fragment_info().unwrap();
                let nf = fi.num_fragments().unwrap();
                assert!(nf > 0);

                let this_fragment = fi.get_fragment(nf - 1).unwrap();

                if let Some(write_domain) = write.domain() {
                    let nonempty_domain =
                        this_fragment.non_empty_domain().unwrap().untyped();
                    assert_eq!(write_domain, nonempty_domain);
                } else {
                    // most recent fragment should be empty,
                    // what does that look like if no data was written?
                }

                this_fragment.timestamp_range().unwrap()
            };

            let safety_write_start = std::time::Instant::now();

            /*
             * Then re-open the array to read back what we just wrote
             * into the most recent fragment only
             */
            {
                array = array
                    .reopen()
                    .start_timestamp(timestamp_min)
                    .unwrap()
                    .end_timestamp(timestamp_max)
                    .unwrap()
                    .open()
                    .unwrap();

                let mut read = write
                    .attach_read(ReadBuilder::new(array).unwrap())
                    .unwrap()
                    .build();

                if let Some(write_ranges) = write_ranges {
                    let read_ranges =
                        read.subarray().unwrap().ranges().unwrap();
                    assert_eq!(write_ranges, read_ranges);
                }

                let (mut cells, _) = read.execute().unwrap();

                /* `cells` should match the write */
                {
                    let write_sorted = write.cells().sorted(&sort_keys);
                    cells.sort(&sort_keys);
                    assert_eq!(write_sorted, cells);
                }

                array = read.finalize().unwrap();
            }

            /* finally, check that everything written up until now is correct */
            array = array.reopen().start_timestamp(0).unwrap().open().unwrap();

            /* check array non-empty domain */
            if let Some(accumulated_domain) = accumulated_domain.as_mut() {
                let Some(write_domain) = write.domain() else {
                    unreachable!()
                };
                *accumulated_domain = accumulated_domain.union(&write_domain);
            } else {
                accumulated_domain = write.domain();
            }
            {
                let Some(acc) = accumulated_domain.as_ref() else {
                    unreachable!()
                };
                let nonempty =
                    array.nonempty_domain().unwrap().unwrap().untyped();
                assert_eq!(*acc, nonempty);
            }

            /* update accumulated expected array data */
            accumulated_write.accumulate(write);
            {
                let acc = accumulated_write.cells().sorted(&sort_keys);

                let cells = {
                    let mut read = accumulated_write
                        .attach_read(ReadBuilder::new(array).unwrap())
                        .unwrap()
                        .build();

                    let (mut cells, _) = read.execute().unwrap();
                    cells.sort(&sort_keys);
                    cells
                };

                assert_eq!(acc, cells);
            }

            // safety valve to ensure we don't write two fragments in the same millisecond
            if safety_write_start.elapsed()
                < std::time::Duration::from_millis(1)
            {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }

        Ok(())
    }

    /// Test that a single write can be read back correctly
    #[test]
    fn write_once_readback() -> TileDBResult<()> {
        let ctx = Context::new().expect("Error creating context");

        let schema_req = query_write_schema_requirements(None);

        let strategy = any_with::<SchemaData>(Rc::new(schema_req))
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
            do_write_readback(&ctx, schema_spec, write_sequence)?;
        });

        Ok(())
    }

    /// Test that each write in the sequence can be read back correctly at the right timestamp
    #[test]
    fn write_sequence_readback() -> TileDBResult<()> {
        let ctx = Context::new().expect("Error creating context");

        let schema_req =
            query_write_schema_requirements(Some(ArrayType::Sparse));

        let strategy = any_with::<SchemaData>(Rc::new(schema_req))
            .prop_flat_map(|schema| {
                let schema = Rc::new(schema);
                (
                    Just(Rc::clone(&schema)),
                    any_with::<WriteSequence>(
                        WriteSequenceParameters::default_for(Rc::clone(
                            &schema,
                        )),
                    ),
                )
            });

        proptest!(|((schema_spec, write_sequence) in strategy)| {
            do_write_readback(&ctx, schema_spec, write_sequence)?;
        });

        Ok(())
    }
}
