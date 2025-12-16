use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::RangeInclusive;
use std::rc::Rc;

use proptest::collection::SizeRange;
use proptest::prelude::*;
use proptest::strategy::{NewTree, ValueTree};
use proptest::test_runner::TestRunner;
use strategy_ext::records::RecordsValueTree;
use tiledb_common::array::schema::EnumerationKey;
use tiledb_common::array::{ArrayType, CellValNum};
use tiledb_common::datatype::physical::BitsKeyAdapter;
use tiledb_common::datatype::{Datatype, PhysicalType};
use tiledb_common::query::condition::strategy::{
    QueryConditionField, QueryConditionSchema,
};
use tiledb_common::query::condition::{Field as ASTField, *};
use tiledb_common::range::{Range, SingleValueRange, VarValueRange};
use tiledb_common::{
    dimension_constraints_go, physical_type_go, set_members_go,
};
use tiledb_pod::array::schema::{FieldData as SchemaField, SchemaData};

use super::Cells;
use super::field::FieldData;
use crate::typed_field_data_go;

trait IntegralType: Eq + Ord + PhysicalType {}

impl IntegralType for u8 {}
impl IntegralType for u16 {}
impl IntegralType for u32 {}
impl IntegralType for u64 {}
impl IntegralType for i8 {}
impl IntegralType for i16 {}
impl IntegralType for i32 {}
impl IntegralType for i64 {}

#[derive(Clone, Debug)]
pub enum FieldStrategyDatatype {
    Datatype(Datatype, CellValNum),
    SchemaField(SchemaField),
}

#[derive(Clone, Debug)]
pub struct FieldDataParameters {
    pub nrecords: SizeRange,
    pub datatype: Option<FieldStrategyDatatype>,
    pub value_min_var_size: usize,
    pub value_max_var_size: usize,
}

impl Default for FieldDataParameters {
    fn default() -> Self {
        FieldDataParameters {
            nrecords: (0..=1024).into(),
            datatype: None,
            value_min_var_size: 1, /* SC-48409 and SC-48428 workaround */
            value_max_var_size: 8, /* TODO */
        }
    }
}

trait ArbitraryFieldData: Sized {
    fn arbitrary(
        params: FieldDataParameters,
        cell_val_num: CellValNum,
        value_strat: BoxedStrategy<Self>,
    ) -> BoxedStrategy<FieldData>;
}

impl<DT> ArbitraryFieldData for DT
where
    DT: IntegralType,
    FieldData: From<Vec<DT>> + From<Vec<Vec<DT>>>,
{
    fn arbitrary(
        params: FieldDataParameters,
        cell_val_num: CellValNum,
        value_strat: BoxedStrategy<Self>,
    ) -> BoxedStrategy<FieldData> {
        if cell_val_num == 1u32 {
            proptest::collection::vec(value_strat, params.nrecords)
                .prop_map(FieldData::from)
                .boxed()
        } else {
            let (min, max) = if cell_val_num.is_var_sized() {
                (params.value_min_var_size, params.value_max_var_size)
            } else {
                let fixed_bound = Into::<u32>::into(cell_val_num) as usize;
                (fixed_bound, fixed_bound)
            };

            let cell_strat = proptest::collection::vec(value_strat, min..=max);

            proptest::collection::vec(cell_strat, params.nrecords)
                .prop_map(FieldData::from)
                .boxed()
        }
    }
}
impl ArbitraryFieldData for f32 {
    fn arbitrary(
        params: FieldDataParameters,
        cell_val_num: CellValNum,
        value_strat: BoxedStrategy<Self>,
    ) -> BoxedStrategy<FieldData> {
        let value_strat = value_strat.prop_map(|float| float.to_bits()).boxed();

        fn transform(v: Vec<u32>) -> Vec<f32> {
            v.into_iter().map(f32::from_bits).collect::<Vec<f32>>()
        }

        <u32 as ArbitraryFieldData>::arbitrary(
            params,
            cell_val_num,
            value_strat,
        )
        .prop_map(|field_data| match field_data {
            FieldData::UInt32(values) => FieldData::Float32(transform(values)),
            FieldData::VecUInt32(values) => FieldData::VecFloat32(
                values.into_iter().map(transform).collect::<Vec<Vec<f32>>>(),
            ),
            _ => unreachable!(),
        })
        .boxed()
    }
}

impl ArbitraryFieldData for f64 {
    fn arbitrary(
        params: FieldDataParameters,
        cell_val_num: CellValNum,
        value_strat: BoxedStrategy<Self>,
    ) -> BoxedStrategy<FieldData> {
        let value_strat = value_strat.prop_map(|float| float.to_bits()).boxed();

        fn transform(v: Vec<u64>) -> Vec<f64> {
            v.into_iter().map(f64::from_bits).collect::<Vec<f64>>()
        }

        <u64 as ArbitraryFieldData>::arbitrary(
            params,
            cell_val_num,
            value_strat,
        )
        .prop_map(|field_data| match field_data {
            FieldData::UInt64(values) => FieldData::Float64(transform(values)),
            FieldData::VecUInt64(values) => FieldData::VecFloat64(
                values.into_iter().map(transform).collect::<Vec<Vec<f64>>>(),
            ),
            _ => unreachable!(),
        })
        .boxed()
    }
}

impl Arbitrary for FieldData {
    type Strategy = BoxedStrategy<FieldData>;
    type Parameters = FieldDataParameters;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        match params.datatype.clone() {
            Some(FieldStrategyDatatype::SchemaField(
                SchemaField::Dimension(d),
            )) => {
                let value_strat = d.value_strategy();
                let cell_val_num = d.cell_val_num();

                dimension_constraints_go!(
                    d.constraints,
                    DT,
                    _,
                    _,
                    {
                        <DT as ArbitraryFieldData>::arbitrary(
                            params,
                            cell_val_num,
                            value_strat.try_into().unwrap(),
                        )
                    },
                    {
                        <u8 as ArbitraryFieldData>::arbitrary(
                            params,
                            cell_val_num,
                            value_strat.try_into().unwrap(),
                        )
                    }
                )
            }
            Some(FieldStrategyDatatype::SchemaField(
                SchemaField::Attribute(a),
            )) => {
                let value_strat = a.value_strategy();
                let cell_val_num =
                    a.cell_val_num.unwrap_or(CellValNum::single());

                physical_type_go!(a.datatype, DT, {
                    <DT as ArbitraryFieldData>::arbitrary(
                        params,
                        cell_val_num,
                        value_strat.try_into().unwrap(),
                    )
                })
            }
            Some(FieldStrategyDatatype::Datatype(datatype, cell_val_num)) => {
                physical_type_go!(datatype, DT, {
                    let value_strat = any::<DT>().boxed();
                    <DT as ArbitraryFieldData>::arbitrary(
                        params,
                        cell_val_num,
                        value_strat,
                    )
                })
            }
            None => (any::<Datatype>(), any::<CellValNum>())
                .prop_flat_map(move |(datatype, cell_val_num)| {
                    physical_type_go!(datatype, DT, {
                        let value_strat = any::<DT>().boxed();
                        <DT as ArbitraryFieldData>::arbitrary(
                            params.clone(),
                            cell_val_num,
                            value_strat,
                        )
                    })
                })
                .boxed(),
        }
    }
}

/// Mask for whether a field should be included in a write query.
// As of this writing, core does not support default values being filled in,
// so this construct is not terribly useful. But someday that may change
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum FieldMask {
    /// This field must appear in the write set
    Include,
    /// This field appears in the write set but simplification may change that
    TentativelyInclude,
    /// This field may appear in the write set again after complication
    _TentativelyExclude,
    /// This field may not appear in the write set again
    Exclude,
}

impl FieldMask {
    pub fn is_included(&self) -> bool {
        matches!(self, FieldMask::Include | FieldMask::TentativelyInclude)
    }
}

/// Value tree to shrink cells.
/// For a failing test which writes N records, there are 2^N possible
/// candidate subsets and we want to find the smallest one which fails the test
/// in the shortest number of iterations.
/// That would be ideal but really finding any input that's small enough
/// to be human readable sounds good enough. We divide the record space
/// into CELLS_VALUE_TREE_EXPLORE_PIECES chunks and identify which
/// of those chunks are necessary for the failure.
/// Recur until all of the chunks are necessary for failure, or there
/// is only one record.
///
/// TODO: for var sized attributes, follow up by shrinking the values.
struct CellsValueTree {
    _field_masks: HashMap<String, FieldMask>,
    field_data_tree: RecordsValueTree<HashMap<String, FieldData>>,
}

impl CellsValueTree {
    pub fn new(
        params: CellsParameters,
        field_data: HashMap<String, (FieldMask, Option<FieldData>)>,
    ) -> Self {
        // sanity check
        {
            let mut nrecords = None;
            for f in field_data.values() {
                if let Some(f) = f.1.as_ref() {
                    if let Some(nrecords) = nrecords {
                        assert_eq!(nrecords, f.len())
                    } else {
                        nrecords = Some(f.len())
                    }
                }
            }
        }

        let field_masks = field_data
            .iter()
            .map(|(fname, &(fmask, _))| (fname.clone(), fmask))
            .collect::<HashMap<String, FieldMask>>();
        let field_data = field_data
            .into_iter()
            .filter(|&(_, (fmask, _))| fmask.is_included())
            .map(|(fname, (_, fdata))| (fname, fdata.unwrap()))
            .collect::<HashMap<String, FieldData>>();

        let field_data_tree =
            RecordsValueTree::new(params.min_records, field_data);

        CellsValueTree {
            _field_masks: field_masks,
            field_data_tree,
        }
    }
}

impl ValueTree for CellsValueTree {
    type Value = Cells;

    fn current(&self) -> Self::Value {
        Cells::new(self.field_data_tree.current())
    }

    fn simplify(&mut self) -> bool {
        self.field_data_tree.simplify()
    }

    fn complicate(&mut self) -> bool {
        self.field_data_tree.complicate()
    }
}

#[derive(Clone, Debug)]
pub enum CellsStrategySchema {
    /// Quick-and-dirty set of fields to write to
    Fields(HashMap<String, (Datatype, CellValNum)>),
    /// Schema for writing
    WriteSchema(Rc<SchemaData>),
    /// Schema for reading
    ReadSchema(Rc<SchemaData>),
}

impl CellsStrategySchema {
    pub fn array_schema(&self) -> Option<&SchemaData> {
        match self {
            Self::WriteSchema(s) | Self::ReadSchema(s) => Some(s.as_ref()),
            _ => None,
        }
    }

    fn new_field_tree(
        &self,
        runner: &mut TestRunner,
        nrecords: RangeInclusive<usize>,
    ) -> HashMap<String, (FieldMask, Option<FieldData>)> {
        let field_data_parameters_base = FieldDataParameters::default();

        match self {
            Self::Fields(fields) => {
                let nrecords = nrecords.new_tree(runner).unwrap().current();

                let field_mask = fields
                    .iter()
                    .map(|(k, v)| {
                        (k.to_string(), (FieldMask::TentativelyInclude, v))
                    })
                    .collect::<HashMap<_, _>>();

                field_mask
                    .into_iter()
                    .map(|(field, (mask, (datatype, cell_val_num)))| {
                        let field_data = if mask.is_included() {
                            let params = FieldDataParameters {
                                nrecords: (nrecords..=nrecords).into(),
                                datatype: Some(
                                    FieldStrategyDatatype::Datatype(
                                        *datatype,
                                        *cell_val_num,
                                    ),
                                ),
                                ..field_data_parameters_base.clone()
                            };
                            Some(
                                any_with::<FieldData>(params)
                                    .new_tree(runner)
                                    .unwrap()
                                    .current(),
                            )
                        } else {
                            None
                        };
                        (field, (mask, field_data))
                    })
                    .collect::<HashMap<String, (FieldMask, Option<FieldData>)>>(
                    )
            }
            Self::WriteSchema(schema) => {
                let field_mask = {
                    let dimensions_mask = {
                        let mask = match schema.array_type {
                            ArrayType::Dense => {
                                /* dense array coordinates are handled by a subarray */
                                FieldMask::Exclude
                            }
                            ArrayType::Sparse => {
                                /* sparse array must write coordinates */
                                FieldMask::Include
                            }
                        };
                        schema
                            .domain
                            .dimension
                            .iter()
                            .map(|d| (SchemaField::from(d.clone()), mask))
                            .collect::<Vec<(SchemaField, FieldMask)>>()
                    };

                    /* as of this writing, write queries must write to all attributes */
                    let attributes_mask = schema
                        .attributes
                        .iter()
                        .map(|a| {
                            (SchemaField::from(a.clone()), FieldMask::Include)
                        })
                        .collect::<Vec<(SchemaField, FieldMask)>>();

                    dimensions_mask
                        .into_iter()
                        .chain(attributes_mask)
                        .collect::<Vec<(SchemaField, FieldMask)>>()
                };

                if schema.array_type == ArrayType::Sparse
                    && !schema.allow_duplicates.unwrap_or(false)
                {
                    // dimension coordinates must be unique, generate them first
                    let unique_keys = schema
                        .domain
                        .dimension
                        .iter()
                        .map(|d| d.name.clone())
                        .collect::<Vec<String>>();
                    let dimension_data = schema
                        .domain
                        .dimension
                        .iter()
                        .map(|d| {
                            let params = FieldDataParameters {
                                nrecords: (*nrecords.end()..=*nrecords.end())
                                    .into(),
                                datatype: Some(
                                    FieldStrategyDatatype::SchemaField(
                                        SchemaField::Dimension(d.clone()),
                                    ),
                                ),
                                ..field_data_parameters_base.clone()
                            };
                            (
                                d.name.clone(),
                                any_with::<FieldData>(params)
                                    .new_tree(runner)
                                    .unwrap()
                                    .current(),
                            )
                        })
                        .collect::<HashMap<String, FieldData>>();

                    let mut dedup_fields =
                        Cells::new(dimension_data).dedup(&unique_keys);

                    // choose the number of records
                    let nrecords = {
                        /*
                         * TODO: not really accurate but in practice nrecords.start
                         * is probably zero so this is the easy lazy thing to do
                         */
                        assert!(*nrecords.start() <= dedup_fields.len());

                        (*nrecords.start()..=dedup_fields.len())
                            .new_tree(runner)
                            .unwrap()
                            .current()
                    };

                    field_mask.into_iter()
                        .map(|(field, mask)| {
                            let field_name = field.name().to_owned();
                            let field_data = if let Some(mut dim) = dedup_fields.fields.remove(&field_name) {
                                assert!(field.is_dimension());
                                dim.truncate(nrecords);
                                dim
                            } else {
                                assert!(field.is_attribute());
                                let params = FieldDataParameters {
                                    nrecords: (nrecords..=nrecords).into(),
                                    datatype: Some(FieldStrategyDatatype::SchemaField(field)),
                                    ..field_data_parameters_base.clone()
                                };
                                any_with::<FieldData>(params)
                                    .new_tree(runner)
                                    .unwrap()
                                    .current()
                            };
                            assert_eq!(nrecords, field_data.len());
                            (field_name, (mask, Some(field_data)))
                        })
                    .collect::<HashMap<String, (FieldMask, Option<FieldData>)>>()
                } else {
                    let nrecords = nrecords.new_tree(runner).unwrap().current();
                    field_mask
                        .into_iter()
                        .map(|(field, mask)| {
                            let field_name = field.name().to_string();
                            let field_data = if mask.is_included() {
                                let params = FieldDataParameters {
                                    nrecords: (nrecords..=nrecords).into(),
                                    datatype: Some(
                                        FieldStrategyDatatype::SchemaField(field),
                                    ),
                                    ..field_data_parameters_base.clone()
                                };
                                Some(
                                    any_with::<FieldData>(params)
                                    .new_tree(runner)
                                    .unwrap()
                                    .current(),
                                )
                            } else {
                                None
                            };
                            (field_name, (mask, field_data))
                        })
                    .collect::<HashMap<String, (FieldMask, Option<FieldData>)>>(
                    )
                }
            }
            Self::ReadSchema(_) => {
                /* presumably any subset of the fields */
                unimplemented!()
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct CellsParameters {
    pub schema: Option<CellsStrategySchema>,
    pub min_records: usize,
    pub max_records: usize,
    pub cell_min_var_size: usize,
    pub cell_max_var_size: usize,
}

impl CellsParameters {
    pub fn min_records_default() -> usize {
        **tiledb_proptest_config::TILEDB_STRATEGY_CELLS_PARAMETERS_NUM_RECORDS_MIN
    }

    pub fn max_records_default() -> usize {
        **tiledb_proptest_config::TILEDB_STRATEGY_CELLS_PARAMETERS_NUM_RECORDS_MAX
    }

    pub fn cell_min_var_size_default() -> usize {
        **tiledb_proptest_config::TILEDB_STRATEGY_CELLS_PARAMETERS_CELL_VAR_SIZE_MIN
    }

    pub fn cell_max_var_size_default() -> usize {
        **tiledb_proptest_config::TILEDB_STRATEGY_CELLS_PARAMETERS_CELL_VAR_SIZE_MAX
    }
}

impl Default for CellsParameters {
    fn default() -> Self {
        CellsParameters {
            schema: None,
            min_records: Self::min_records_default(),
            max_records: Self::max_records_default(),
            cell_min_var_size: Self::cell_min_var_size_default(),
            cell_max_var_size: Self::cell_max_var_size_default(),
        }
    }
}

#[derive(Debug)]
struct CellsStrategy {
    schema: CellsStrategySchema,
    params: CellsParameters,
}

impl CellsStrategy {
    pub fn new(schema: CellsStrategySchema, params: CellsParameters) -> Self {
        CellsStrategy { schema, params }
    }

    /// Returns an upper bound on the number of cells which can possibly be produced
    fn nrecords_limit(&self) -> Option<usize> {
        if let Some(schema) = self.schema.array_schema() {
            if !schema.allow_duplicates.unwrap_or(true) {
                return schema.domain.num_cells();
            }
        }
        None
    }
}

impl Strategy for CellsStrategy {
    type Tree = CellsValueTree;
    type Value = Cells;

    fn new_tree(&self, runner: &mut TestRunner) -> NewTree<Self> {
        /* Choose the maximum number of records */
        let strat_nrecords = if let Some(limit) = self.nrecords_limit() {
            if limit < self.params.min_records {
                let r = format!(
                    "Schema and parameters are not satisfiable: schema.domain.num_cells() = {}, self.params.min_records = {}",
                    limit, self.params.min_records
                );
                return Err(proptest::test_runner::Reason::from(r));
            } else {
                let max_records = std::cmp::min(self.params.max_records, limit);
                self.params.min_records..=max_records
            }
        } else {
            self.params.min_records..=self.params.max_records
        };

        /* generate an initial set of fields to write */
        let field_tree = self.schema.new_field_tree(runner, strat_nrecords);

        Ok(CellsValueTree::new(self.params.clone(), field_tree))
    }
}

impl Arbitrary for Cells {
    type Parameters = CellsParameters;
    type Strategy = BoxedStrategy<Cells>;

    fn arbitrary_with(mut args: Self::Parameters) -> Self::Strategy {
        if let Some(schema) = args.schema.take() {
            CellsStrategy::new(schema, args).boxed()
        } else {
            let keys =
                tiledb_pod::array::attribute::strategy::prop_attribute_name();
            let values = (any::<Datatype>(), any::<CellValNum>());
            proptest::collection::hash_map(keys, values, 1..16)
                .prop_flat_map(move |values| {
                    CellsStrategy::new(
                        CellsStrategySchema::Fields(values),
                        args.clone(),
                    )
                })
                .boxed()
        }
    }
}

/// Enables [Cells] to be used in parameters for [QueryCondition].
pub struct CellsAsQueryConditionSchema {
    fields: Vec<CellsQueryConditionField>,
}

struct CellsQueryConditionField {
    cells: Rc<Cells>,
    name: String,
}

impl CellsAsQueryConditionSchema {
    pub fn new(cells: Rc<Cells>) -> CellsAsQueryConditionSchema {
        Self {
            fields: cells
                .fields
                .keys()
                .map(|name| CellsQueryConditionField {
                    cells: Rc::clone(&cells),
                    name: name.to_owned(),
                })
                .collect(),
        }
    }
}

impl QueryConditionSchema for CellsAsQueryConditionSchema {
    /// Returns a list of fields which can have query conditions applied to them.
    fn fields(&self) -> Vec<&dyn QueryConditionField> {
        self.fields
            .iter()
            .map(|f| f as &dyn QueryConditionField)
            .collect::<Vec<&dyn QueryConditionField>>()
    }
}

impl QueryConditionField for CellsQueryConditionField {
    fn name(&self) -> &str {
        &self.name
    }

    fn equality_ops(&self) -> Option<Vec<EqualityOp>> {
        // all ops are supported
        None
    }

    fn domain(&self) -> Option<tiledb_common::range::Range> {
        self.cells.fields.get(&self.name).unwrap().domain()
    }

    fn set_members(&self) -> Option<SetMembers> {
        self.cells.enumeration_values.get(&self.name).and_then(|e| {
            typed_field_data_go!(
                e,
                _DT,
                _members,
                Some(_members.clone().into_iter().collect::<SetMembers>()),
                {
                    // NB: this is Var, we could do String but it's just to test the test code
                    // so we will skip
                    None
                }
            )
        })
    }
}

pub struct SchemaWithDomain {
    fields: Vec<FieldWithDomain>,
}

impl SchemaWithDomain {
    pub fn new(schema: Rc<SchemaData>, cells: &Cells) -> Self {
        Self {
            fields: cells
                .domain()
                .into_iter()
                .map(|(f, domain)| FieldWithDomain {
                    schema: Rc::clone(&schema),
                    field: schema.field(f).unwrap(),
                    domain,
                })
                .collect::<Vec<_>>(),
        }
    }
}

pub struct FieldWithDomain {
    schema: Rc<SchemaData>,
    field: SchemaField,
    domain: Option<Range>,
}

impl QueryConditionSchema for SchemaWithDomain {
    fn fields(&self) -> Vec<&dyn QueryConditionField> {
        self.fields
            .iter()
            .map(|f| f as &dyn QueryConditionField)
            .collect::<Vec<_>>()
    }
}

impl QueryConditionField for FieldWithDomain {
    fn name(&self) -> &str {
        self.field.name()
    }

    fn equality_ops(&self) -> Option<Vec<EqualityOp>> {
        match self.field {
            SchemaField::Dimension(_) => None,
            SchemaField::Attribute(ref a) => {
                if let Some(edata) = self
                    .schema
                    .enumeration(EnumerationKey::AttributeName(&a.name))
                {
                    if !ASTField::is_allowed_type(
                        edata.datatype,
                        edata.cell_val_num.unwrap_or(CellValNum::single()),
                    ) {
                        // only null test allowed for these
                        Some(vec![])
                    } else if matches!(edata.ordered, Some(true)) {
                        // anything goes
                        None
                    } else {
                        Some(vec![EqualityOp::Equal, EqualityOp::NotEqual])
                    }
                } else if !ASTField::is_allowed_type(
                    a.datatype,
                    a.cell_val_num.unwrap_or(CellValNum::single()),
                ) {
                    // only null test allowed for these
                    Some(vec![])
                } else {
                    None
                }
            }
        }
    }

    fn domain(&self) -> Option<Range> {
        #[allow(clippy::collapsible_if)]
        if let SchemaField::Attribute(ref a) = self.field {
            if let Some(edata) = self
                .schema
                .enumeration(EnumerationKey::AttributeName(&a.name))
            {
                // query condition domain is in terms of the enumerated values,
                // not the attribute values domaion (which are indexes into the enumerated values)
                let members = edata.query_condition_set_members()?;
                return Some(set_members_go!(
                    members,
                    _DT,
                    ref members,
                    {
                        let min = *members.iter().min()?;
                        let max = *members.iter().max()?;
                        Range::Single(SingleValueRange::from(min..=max))
                    },
                    {
                        let min = *members.iter().map(BitsKeyAdapter).min()?.0;
                        let max = *members.iter().map(BitsKeyAdapter).max()?.0;
                        Range::Single(SingleValueRange::from(min..=max))
                    },
                    {
                        let min = members.iter().min()?.clone();
                        let max = members.iter().max()?.clone();
                        Range::Var(VarValueRange::from((
                            min.into_bytes().into_boxed_slice(),
                            max.into_bytes().into_boxed_slice(),
                        )))
                    }
                ));
            }
        }

        // see query_ast.cc
        if matches!(
            self.field.datatype(),
            Datatype::Any
                | Datatype::StringUtf16
                | Datatype::StringUtf32
                | Datatype::StringUcs2
                | Datatype::StringUcs4
                | Datatype::Blob
                | Datatype::GeometryWkb
                | Datatype::GeometryWkt
        ) {
            None
        } else if matches!(self.domain, Some(Range::Single(_)))
            || (matches!(
                self.field.datatype(),
                Datatype::StringAscii | Datatype::StringUtf8
            ) && matches!(
                self.field.cell_val_num(),
                None | Some(CellValNum::Var)
            ))
        {
            self.domain.clone()
        } else {
            None
        }
    }

    fn set_members(&self) -> Option<SetMembers> {
        match self.field {
            SchemaField::Dimension(_) => None,
            SchemaField::Attribute(ref a) => {
                let edata = self
                    .schema
                    .enumeration(EnumerationKey::AttributeName(&a.name))?;
                edata.query_condition_set_members()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use proptest::prelude::*;
    use tiledb_common::query::condition::strategy::Parameters as QueryConditionParameters;

    use super::*;
    use crate::strategy::{CellsParameters, CellsStrategySchema};

    fn strat_schema_arbitrary_query_condition()
    -> impl Strategy<Value = (Rc<SchemaData>, Rc<Cells>, Vec<QueryConditionExpr>)>
    {
        any::<SchemaData>()
            .prop_flat_map(|schema| {
                let schema = Rc::new(schema);
                let strat_cells = any_with::<Cells>(CellsParameters {
                    schema: Some(CellsStrategySchema::WriteSchema(Rc::clone(
                        &schema,
                    ))),
                    ..Default::default()
                });
                (Just(schema), strat_cells)
            })
            .prop_flat_map(|(schema, cells)| {
                let cells = Rc::new(cells);
                let qc_params = QueryConditionParameters {
                    domain: Some(Rc::new(SchemaWithDomain::new(
                        Rc::clone(&schema),
                        &cells,
                    ))),
                    ..Default::default()
                };
                (
                    Just(schema),
                    Just(cells),
                    proptest::collection::vec(
                        any_with::<QueryConditionExpr>(qc_params),
                        1..=32,
                    ),
                )
            })
    }

    proptest! {
        /// Tests that we don't panic while running the strategy.
        /// This should give some confidence in the correctness of the trait implementations
        /// of `SchemaWithDomain`.
        #[test]
        fn schema_arbitrary_query_condition((_, _, _) in strat_schema_arbitrary_query_condition())
        {
        }
    }
}
