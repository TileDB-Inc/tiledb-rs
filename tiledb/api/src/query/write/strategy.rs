use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::rc::Rc;

use paste::paste;
use proptest::bits::{BitSetLike, VarBitSet};
use proptest::prelude::*;
use proptest::strategy::{NewTree, ValueTree};
use proptest::test_runner::TestRunner;

use super::*;
use crate::array::{ArrayType, CellValNum, SchemaData};
use crate::query::read::output::{
    FixedDataIterator, RawReadOutput, TypedRawReadOutput, VarDataIterator,
};
use crate::query::read::{
    CallbackVarArgReadBuilder, ManagedBuffer, RawReadHandle,
    ReadCallbackVarArg, TypedReadHandle,
};
use crate::{fn_typed, typed_query_buffers_go};

/// Represents the write query input for a single field.
/// For each variant, the outer Vec is the collection of records, and the interior is value in the
/// cell for the record. Fields with cell val num of 1 are flat, and other cell values use the
/// inner Vec. For fixed-size attributes, the inner Vecs shall all have the same length; for
/// var-sized attributes that is obviously not required.
#[derive(Clone, Debug, PartialEq)]
pub enum FieldData {
    UInt8(Vec<u8>),
    UInt16(Vec<u16>),
    UInt32(Vec<u32>),
    UInt64(Vec<u64>),
    Int8(Vec<i8>),
    Int16(Vec<i16>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    VecUInt8(Vec<Vec<u8>>),
    VecUInt16(Vec<Vec<u16>>),
    VecUInt32(Vec<Vec<u32>>),
    VecUInt64(Vec<Vec<u64>>),
    VecInt8(Vec<Vec<i8>>),
    VecInt16(Vec<Vec<i16>>),
    VecInt32(Vec<Vec<i32>>),
    VecInt64(Vec<Vec<i64>>),
    VecFloat32(Vec<Vec<f32>>),
    VecFloat64(Vec<Vec<f64>>),
}

macro_rules! typed_field_data {
    ($($V:ident : $U:ty),+) => {
        $(
            impl From<Vec<$U>> for FieldData {
                fn from(value: Vec<$U>) -> Self {
                    FieldData::$V(value)
                }
            }

            impl From<Vec<Vec<$U>>> for FieldData {
                fn from(value: Vec<Vec<$U>>) -> Self {
                    paste! {
                        FieldData::[< Vec $V >](value)
                    }
                }
            }
        )+
    };
}

typed_field_data!(UInt8: u8, UInt16: u16, UInt32: u32, UInt64: u64);
typed_field_data!(Int8: i8, Int16: i16, Int32: i32, Int64: i64);
typed_field_data!(Float32: f32, Float64: f64);

impl From<Vec<String>> for FieldData {
    fn from(value: Vec<String>) -> Self {
        FieldData::from(
            value
                .into_iter()
                .map(|s| s.into_bytes())
                .collect::<Vec<Vec<u8>>>(),
        )
    }
}

impl From<&TypedRawReadOutput<'_>> for FieldData {
    fn from(value: &TypedRawReadOutput) -> Self {
        typed_query_buffers_go!(value.buffers, DT, ref handle, {
            let rr = RawReadOutput {
                nvalues: value.nvalues,
                nbytes: value.nbytes,
                input: handle.borrow(),
            };
            if rr.input.cell_structure.is_var() {
                Self::from(
                    VarDataIterator::try_from(rr)
                        .unwrap()
                        .map(|s| s.to_vec())
                        .collect::<Vec<Vec<DT>>>(),
                )
            } else {
                Self::from(
                    FixedDataIterator::try_from(rr)
                        .unwrap()
                        .collect::<Vec<DT>>(),
                )
            }
        })
    }
}

#[macro_export]
macro_rules! typed_field_data_go {
    ($field:expr, $DT:ident, $data:pat, $fixed:expr, $var:expr) => {{
        use $crate::query::write::strategy::FieldData;
        match $field {
            FieldData::UInt8($data) => {
                type $DT = u8;
                $fixed
            }
            FieldData::UInt16($data) => {
                type $DT = u16;
                $fixed
            }
            FieldData::UInt32($data) => {
                type $DT = u32;
                $fixed
            }
            FieldData::UInt64($data) => {
                type $DT = u64;
                $fixed
            }
            FieldData::Int8($data) => {
                type $DT = i8;
                $fixed
            }
            FieldData::Int16($data) => {
                type $DT = i16;
                $fixed
            }
            FieldData::Int32($data) => {
                type $DT = i32;
                $fixed
            }
            FieldData::Int64($data) => {
                type $DT = i64;
                $fixed
            }
            FieldData::Float32($data) => {
                type $DT = f32;
                $fixed
            }
            FieldData::Float64($data) => {
                type $DT = f64;
                $fixed
            }
            FieldData::VecUInt8($data) => {
                type $DT = u8;
                $var
            }
            FieldData::VecUInt16($data) => {
                type $DT = u16;
                $var
            }
            FieldData::VecUInt32($data) => {
                type $DT = u32;
                $var
            }
            FieldData::VecUInt64($data) => {
                type $DT = u64;
                $var
            }
            FieldData::VecInt8($data) => {
                type $DT = i8;
                $var
            }
            FieldData::VecInt16($data) => {
                type $DT = i16;
                $var
            }
            FieldData::VecInt32($data) => {
                type $DT = i32;
                $var
            }
            FieldData::VecInt64($data) => {
                type $DT = i64;
                $var
            }
            FieldData::VecFloat32($data) => {
                type $DT = f32;
                $var
            }
            FieldData::VecFloat64($data) => {
                type $DT = f64;
                $var
            }
        }
    }};
    ($field:expr, $data:pat, $then:expr) => {
        typed_field_data_go!($field, _DT, $data, $then, $then)
    };
    ($lexpr:expr, $rexpr:expr, $DT:ident, $lpat:pat, $rpat:pat, $same_type:expr, $else:expr) => {{
        use $crate::query::write::strategy::FieldData;
        match ($lexpr, $rexpr) {
            (FieldData::UInt8($lpat), FieldData::UInt8($rpat)) => {
                type $DT = u8;
                $same_type
            }
            (FieldData::UInt16($lpat), FieldData::UInt16($rpat)) => {
                type $DT = u16;
                $same_type
            }
            (FieldData::UInt32($lpat), FieldData::UInt32($rpat)) => {
                type $DT = u32;
                $same_type
            }
            (FieldData::UInt64($lpat), FieldData::UInt64($rpat)) => {
                type $DT = u64;
                $same_type
            }
            (FieldData::Int8($lpat), FieldData::Int8($rpat)) => {
                type $DT = i8;
                $same_type
            }
            (FieldData::Int16($lpat), FieldData::Int16($rpat)) => {
                type $DT = i16;
                $same_type
            }
            (FieldData::Int32($lpat), FieldData::Int32($rpat)) => {
                type $DT = i32;
                $same_type
            }
            (FieldData::Int64($lpat), FieldData::Int64($rpat)) => {
                type $DT = i64;
                $same_type
            }
            (FieldData::Float32($lpat), FieldData::Float32($rpat)) => {
                type $DT = f32;
                $same_type
            }
            (FieldData::Float64($lpat), FieldData::Float64($rpat)) => {
                type $DT = f64;
                $same_type
            }
            (FieldData::VecUInt8($lpat), FieldData::VecUInt8($rpat)) => {
                type $DT = u8;
                $same_type
            }
            (FieldData::VecUInt16($lpat), FieldData::VecUInt16($rpat)) => {
                type $DT = u16;
                $same_type
            }
            (FieldData::VecUInt32($lpat), FieldData::VecUInt32($rpat)) => {
                type $DT = u32;
                $same_type
            }
            (FieldData::VecUInt64($lpat), FieldData::VecUInt64($rpat)) => {
                type $DT = u64;
                $same_type
            }
            (FieldData::VecInt8($lpat), FieldData::VecInt8($rpat)) => {
                type $DT = i8;
                $same_type
            }
            (FieldData::VecInt16($lpat), FieldData::VecInt16($rpat)) => {
                type $DT = i16;
                $same_type
            }
            (FieldData::VecInt32($lpat), FieldData::VecInt32($rpat)) => {
                type $DT = i32;
                $same_type
            }
            (FieldData::VecInt64($lpat), FieldData::VecInt64($rpat)) => {
                type $DT = i64;
                $same_type
            }
            (FieldData::VecFloat32($lpat), FieldData::VecFloat32($rpat)) => {
                type $DT = f32;
                $same_type
            }
            (FieldData::VecFloat64($lpat), FieldData::VecFloat64($rpat)) => {
                type $DT = f64;
                $same_type
            }
            _ => $else,
        }
    }};
}

impl FieldData {
    pub fn is_empty(&self) -> bool {
        typed_field_data_go!(self, v, v.is_empty())
    }

    pub fn len(&self) -> usize {
        typed_field_data_go!(self, v, v.len())
    }

    pub fn filter(&self, set: &VarBitSet) -> FieldData {
        typed_field_data_go!(self, ref values, {
            FieldData::from(
                values
                    .clone()
                    .into_iter()
                    .enumerate()
                    .filter(|&(i, _)| set.test(i))
                    .map(|(_, e)| e)
                    .collect::<Vec<_>>(),
            )
        })
    }
}

pub struct RawReadQueryResult(pub HashMap<String, FieldData>);

pub struct RawResultCallback {
    field_order: Vec<String>,
}

impl ReadCallbackVarArg for RawResultCallback {
    type Intermediate = RawReadQueryResult;
    type Final = RawReadQueryResult;
    type Error = std::convert::Infallible;

    fn intermediate_result(
        &mut self,
        args: Vec<TypedRawReadOutput>,
    ) -> Result<Self::Intermediate, Self::Error> {
        Ok(RawReadQueryResult(
            self.field_order
                .iter()
                .zip(args.iter())
                .map(|(f, a)| (f.clone(), FieldData::from(a)))
                .collect::<HashMap<String, FieldData>>(),
        ))
    }

    fn final_result(
        mut self,
        args: Vec<TypedRawReadOutput>,
    ) -> Result<Self::Intermediate, Self::Error> {
        self.intermediate_result(args)
    }
}

#[derive(Clone, Debug)]
pub struct WriteQueryData {
    pub fields: HashMap<String, FieldData>,
}

impl WriteQueryData {
    pub fn attach_write<'ctx, 'data>(
        &'data self,
        b: WriteBuilder<'ctx, 'data>,
    ) -> TileDBResult<WriteBuilder<'ctx, 'data>> {
        let mut b = b;
        for f in self.fields.iter() {
            b = typed_field_data_go!(f.1, data, b.data_typed(f.0, data))?;
        }
        Ok(b)
    }

    pub fn attach_read<'ctx, 'data, B>(
        &self,
        b: B,
    ) -> TileDBResult<CallbackVarArgReadBuilder<'data, RawResultCallback, B>>
    where
        B: ReadQueryBuilder<'ctx, 'data>,
    {
        let field_order = self.fields.keys().cloned().collect::<Vec<_>>();
        let handles = {
            let schema = b.base().array().schema().unwrap();

            field_order
                .iter()
                .map(|name| {
                    let field = schema.field(name.clone()).unwrap();
                    fn_typed!(field.datatype().unwrap(), DT, {
                        let managed: ManagedBuffer<DT> = ManagedBuffer::new(
                            field.query_scratch_allocator().unwrap(),
                        );
                        let rr = RawReadHandle::managed(name, managed);
                        TypedReadHandle::from(rr)
                    })
                })
                .collect::<Vec<TypedReadHandle>>()
        };

        b.register_callback_var(handles, RawResultCallback { field_order })
    }

    pub fn accumulate(&mut self, next_write: Self) {
        for (field, data) in next_write.fields.into_iter() {
            match self.fields.entry(field) {
                Entry::Vacant(v) => {
                    v.insert(data);
                }
                Entry::Occupied(mut o) => {
                    let prev_write_data = o.get_mut();
                    typed_field_data_go!(
                        prev_write_data,
                        data,
                        _DT,
                        ref mut mine,
                        theirs,
                        {
                            if mine.len() <= theirs.len() {
                                *mine = theirs;
                            } else {
                                mine[0..theirs.len()]
                                    .clone_from_slice(theirs.as_slice());
                            }
                        },
                        unreachable!()
                    );
                }
            }
        }
    }
}

/// Mask for whether a field should be included in a write query.
// As of this writing, core does not support default values being filled in,
// so this construct is not terribly useful. But someday that may change
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum WriteFieldMask {
    /// This field must appear in the write set
    Include,
    /// This field appears in the write set but simplification may change that
    _TentativelyInclude,
    /// This field may appear in the write set again after complication
    _TentativelyExclude,
    /// This field may not appear in the write set again
    Exclude,
}

impl WriteFieldMask {
    pub fn is_included(&self) -> bool {
        matches!(
            self,
            WriteFieldMask::Include | WriteFieldMask::_TentativelyInclude
        )
    }
}

/// Tracks the last step taken for the write shrinking.
enum ShrinkSearchStep {
    /// Remove a range of records
    Explore(usize),
    Recur,
    Done,
}

const WRITE_QUERY_DATA_VALUE_TREE_EXPLORE_PIECES: usize = 8;

/// Value tree to shrink a write query input.
/// For a failing test which writes N records, there are 2^N possible
/// candidate subsets and we want to find the smallest one which fails the test
/// in the shortest number of iterations.
/// That would be ideal but really finding any input that's small enough
/// to be human readable sounds good enough. We divide the record space
/// into WRITE_QUERY_DATA_VALUE_TREE_EXPLORE_PIECES chunks and identify which
/// of those chunks are necessary for the failure.
/// Recur until all of the chunks are necessary for failure, or there
/// is only one record.
///
/// TODO: for var sized attributes, follow up by shrinking the values.
struct WriteQueryDataValueTree {
    schema: Rc<SchemaData>,
    field_mask: Vec<WriteFieldMask>,
    field_data: HashMap<String, Option<FieldData>>,
    nrecords: usize,
    records_included: Vec<usize>,
    explore_results: [Option<bool>; WRITE_QUERY_DATA_VALUE_TREE_EXPLORE_PIECES],
    search: Option<ShrinkSearchStep>,
}

impl WriteQueryDataValueTree {
    pub fn new(
        schema: Rc<SchemaData>,
        field_mask: Vec<WriteFieldMask>,
        field_data: HashMap<String, Option<FieldData>>,
    ) -> Self {
        let nrecords = field_data
            .values()
            .filter_map(|f| f.as_ref())
            .take(1)
            .next()
            .unwrap()
            .len();
        let records_included = (0..nrecords).collect::<Vec<usize>>();

        WriteQueryDataValueTree {
            schema,
            field_mask,
            field_data,
            nrecords,
            records_included,
            explore_results: [None; WRITE_QUERY_DATA_VALUE_TREE_EXPLORE_PIECES],
            search: None,
        }
    }

    fn explore_step(&mut self, failed: bool) -> bool {
        match self.search {
            None => {
                if failed && self.nrecords > 0 {
                    /* failed on the whole input, begin the search */
                    self.search = Some(ShrinkSearchStep::Explore(0));
                    true
                } else {
                    /* passed on the whole input, nothing to do */
                    false
                }
            }
            Some(ShrinkSearchStep::Explore(c)) => {
                let nchunks = std::cmp::min(
                    self.records_included.len(),
                    WRITE_QUERY_DATA_VALUE_TREE_EXPLORE_PIECES,
                );

                self.explore_results[c] = Some(failed);

                match (c + 1).cmp(&nchunks) {
                    Ordering::Less => {
                        self.search = Some(ShrinkSearchStep::Explore(c + 1));
                        true
                    }
                    Ordering::Equal => {
                        /* finished exploring at this level, either recur or finish */
                        let approx_chunk_len =
                            self.records_included.len() / nchunks;
                        let mut new_records_included = vec![];
                        for i in 0..nchunks {
                            let chunk_min = i * approx_chunk_len;
                            let chunk_max = if i + 1 == nchunks {
                                self.records_included.len()
                            } else {
                                (i + 1) * approx_chunk_len
                            };

                            if !self.explore_results[i].take().unwrap() {
                                /* the test passed when chunk `i` was not included; keep it */
                                new_records_included.extend_from_slice(
                                    &self.records_included
                                        [chunk_min..chunk_max],
                                );
                            }
                        }

                        if new_records_included == self.records_included {
                            /* everything was needed to pass */
                            self.search = Some(ShrinkSearchStep::Done);
                        } else {
                            self.records_included = new_records_included;
                            self.search = Some(ShrinkSearchStep::Recur);
                        }
                        /* run another round on the updated input */
                        true
                    }
                    Ordering::Greater => unreachable!(),
                }
            }
            Some(ShrinkSearchStep::Recur) => {
                /* we must have failed unless the test itself is non-deterministic */
                assert!(failed);

                self.search = Some(ShrinkSearchStep::Explore(0));
                true
            }
            Some(ShrinkSearchStep::Done) => false,
        }
    }
}

impl ValueTree for WriteQueryDataValueTree {
    type Value = WriteQueryData;

    fn current(&self) -> Self::Value {
        let record_mask = match self.search {
            None => VarBitSet::saturated(self.nrecords),
            Some(ShrinkSearchStep::Explore(c)) => {
                let nchunks = self
                    .records_included
                    .len()
                    .clamp(1, WRITE_QUERY_DATA_VALUE_TREE_EXPLORE_PIECES);

                let approx_chunk_len = self.records_included.len() / nchunks;

                if approx_chunk_len == 0 {
                    /* no records are included, we have shrunk down to empty */
                    VarBitSet::new_bitset(self.nrecords)
                } else {
                    let mut record_mask = VarBitSet::new_bitset(self.nrecords);

                    let exclude_min = c * approx_chunk_len;
                    let exclude_max = if c + 1 == nchunks {
                        self.records_included.len()
                    } else {
                        (c + 1) * approx_chunk_len
                    };

                    for r in self.records_included[0..exclude_min]
                        .iter()
                        .chain(self.records_included[exclude_max..].iter())
                    {
                        record_mask.set(*r)
                    }

                    record_mask
                }
            }
            Some(ShrinkSearchStep::Recur) | Some(ShrinkSearchStep::Done) => {
                let mut record_mask = VarBitSet::new_bitset(self.nrecords);
                for r in self.records_included.iter() {
                    record_mask.set(*r);
                }
                record_mask
            }
        };

        let fields = self
            .field_mask
            .iter()
            .enumerate()
            .filter(|(_, f)| f.is_included())
            .map(|(i, _)| {
                let f = self.schema.field(i);
                (
                    f.name.clone(),
                    self.field_data[&f.name]
                        .as_ref()
                        .unwrap()
                        .filter(&record_mask),
                )
            })
            .collect::<Vec<(String, FieldData)>>();

        WriteQueryData {
            fields: fields.into_iter().collect(),
        }
    }

    fn simplify(&mut self) -> bool {
        self.explore_step(true)
    }

    fn complicate(&mut self) -> bool {
        self.explore_step(false)
    }
}

#[derive(Clone, Debug)]
pub struct WriteQueryDataParameters {
    pub schema: Option<Rc<SchemaData>>,
    pub min_records: usize,
    pub max_records: usize,
    pub value_min_var_size: usize,
    pub value_max_var_size: usize,
}

impl Default for WriteQueryDataParameters {
    fn default() -> Self {
        const WRITE_QUERY_MIN_RECORDS: usize = 0;
        const WRITE_QUERY_MAX_RECORDS: usize = 1024 * 1024;

        const WRITE_QUERY_MIN_VAR_SIZE: usize = 0;
        const WRITE_QUERY_MAX_VAR_SIZE: usize = 1024 * 128;

        WriteQueryDataParameters {
            schema: None,
            min_records: WRITE_QUERY_MIN_RECORDS,
            max_records: WRITE_QUERY_MAX_RECORDS,
            value_min_var_size: WRITE_QUERY_MIN_VAR_SIZE,
            value_max_var_size: WRITE_QUERY_MAX_VAR_SIZE,
        }
    }
}

#[derive(Debug)]
struct WriteQueryDataStrategy {
    schema: Rc<SchemaData>,
    params: WriteQueryDataParameters,
}

impl WriteQueryDataStrategy {
    pub fn new(
        schema: &Rc<SchemaData>,
        params: WriteQueryDataParameters,
    ) -> Self {
        WriteQueryDataStrategy {
            schema: Rc::clone(schema),
            params,
        }
    }
}

impl Strategy for WriteQueryDataStrategy {
    type Tree = WriteQueryDataValueTree;
    type Value = WriteQueryData;

    fn new_tree(&self, runner: &mut TestRunner) -> NewTree<Self> {
        /* Choose the maximum number of records */
        let nrecords = (self.params.min_records..=self.params.max_records)
            .new_tree(runner)?
            .current();

        /* generate an initial set of fields to write */
        let field_mask = {
            let ndimensions = self.schema.domain.dimension.len();
            let nattributes = self.schema.attributes.len();

            let dimensions_mask = match self.schema.array_type {
                ArrayType::Dense => {
                    /* dense array coordinates are handled by a subarray */
                    vec![WriteFieldMask::Exclude; ndimensions]
                }
                ArrayType::Sparse => {
                    /* sparse array must write coordinates */
                    vec![WriteFieldMask::Include; ndimensions]
                }
            };

            /* as of this writing, write queries must write to all attributes */
            let attributes_mask =
                std::iter::repeat(WriteFieldMask::Include).take(nattributes);

            dimensions_mask
                .into_iter()
                .chain(attributes_mask)
                .collect::<Vec<_>>()
        };

        let field_data = field_mask
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let field = self.schema.field(i);
                let field_data = if f.is_included() {
                    let datatype = field.datatype;
                    let cell_val_num = field
                        .cell_val_num
                        .unwrap_or(CellValNum::try_from(1).unwrap());

                    if cell_val_num == 1u32 {
                        Some(fn_typed!(datatype, DT, {
                            let data = proptest::collection::vec(
                                any::<DT>(),
                                nrecords..=nrecords,
                            )
                            .new_tree(runner)
                            .expect("Error generating query data")
                            .current();

                            FieldData::from(data)
                        }))
                    } else {
                        let (min, max) = if cell_val_num.is_var_sized() {
                            (
                                self.params.value_min_var_size,
                                self.params.value_max_var_size,
                            )
                        } else {
                            let fixed_bound =
                                Into::<u32>::into(cell_val_num) as usize;
                            (fixed_bound, fixed_bound)
                        };
                        Some(fn_typed!(datatype, DT, {
                            let data = proptest::collection::vec(
                                proptest::collection::vec(
                                    any::<DT>(),
                                    min..=max,
                                ),
                                nrecords..=nrecords,
                            )
                            .new_tree(runner)
                            .expect("Error generating query data")
                            .current();

                            FieldData::from(data)
                        }))
                    }
                } else {
                    None
                };
                (field.name.clone(), field_data)
            })
            .collect::<HashMap<String, Option<FieldData>>>();

        Ok(WriteQueryDataValueTree::new(
            Rc::clone(&self.schema),
            field_mask,
            field_data,
        ))
    }
}

impl Arbitrary for WriteQueryData {
    type Parameters = WriteQueryDataParameters;
    type Strategy = BoxedStrategy<WriteQueryData>;

    fn arbitrary_with(mut args: Self::Parameters) -> Self::Strategy {
        if let Some(schema) = args.schema.take() {
            WriteQueryDataStrategy::new(&schema, args).boxed()
        } else {
            any::<SchemaData>()
                .prop_flat_map(move |schema| {
                    WriteQueryDataStrategy::new(&Rc::new(schema), args.clone())
                })
                .boxed()
        }
    }
}

#[derive(Debug)]
pub struct WriteSequence {
    writes: Vec<WriteQueryData>,
}

impl Arbitrary for WriteSequence {
    type Parameters = Option<Rc<SchemaData>>;
    type Strategy = BoxedStrategy<WriteSequence>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        if let Some(schema) = args {
            prop_write_sequence(&schema).boxed()
        } else {
            any::<SchemaData>()
                .prop_flat_map(|schema| prop_write_sequence(&Rc::new(schema)))
                .boxed()
        }
    }
}

impl IntoIterator for WriteSequence {
    type Item = WriteQueryData;
    type IntoIter = <Vec<Self::Item> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.writes.into_iter()
    }
}

pub fn prop_write_sequence(
    schema: &Rc<SchemaData>,
) -> impl Strategy<Value = WriteSequence> {
    const MAX_WRITES: usize = 8;
    proptest::collection::vec(
        any_with::<WriteQueryData>(WriteQueryDataParameters {
            schema: Some(Rc::clone(schema)),
            ..Default::default()
        }),
        0..MAX_WRITES,
    )
    .prop_map(|writes| WriteSequence { writes })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    use crate::array::Mode;
    use crate::Factory;

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

        let mut accumulated_write: Option<WriteQueryData> = None;

        for write in write_sequence {
            /* write data */
            {
                let write = write
                    .attach_write(
                        WriteBuilder::new(array)
                            .expect("Error building write query"),
                    )
                    .expect("Error building write query")
                    .build();
                write.submit().expect("Error running write query");
                array = write.finalize().expect("Error finalizing write query");
            }

            /* update accumulated expected array data */
            if let Some(acc) = accumulated_write.as_mut() {
                acc.accumulate(write)
            } else {
                accumulated_write = Some(write);
            }

            let accumulated_write = accumulated_write.as_ref().unwrap();

            /* then read it back */
            {
                let mut cursors = accumulated_write
                    .fields
                    .keys()
                    .map(|key| (key.clone(), 0))
                    .collect::<HashMap<String, usize>>();

                let mut read = accumulated_write
                    .attach_read(
                        ReadBuilder::new(array)
                            .expect("Error building read query"),
                    )
                    .expect("Error building read query")
                    .build();

                loop {
                    let res = read.step().expect("Error in read query step");
                    match res.as_ref().into_inner() {
                        None => unimplemented!(), /* TODO: allocate more */
                        Some((raw, _)) => {
                            let raw = &raw.0;
                            let mut nvalues = None;
                            for (key, rdata) in raw.iter() {
                                let wdata = &accumulated_write.fields[key];

                                let nv = if let Some(nv) = nvalues {
                                    assert_eq!(nv, rdata.len());
                                    nv
                                } else {
                                    nvalues = Some(rdata.len());
                                    rdata.len()
                                };

                                let wdata =
                                    typed_field_data_go!(wdata, wdata, {
                                        FieldData::from(
                                            wdata[cursors[key]
                                                ..cursors[key] + nv]
                                                .to_vec(),
                                        )
                                    });

                                assert_eq!(wdata, *rdata);

                                *cursors.get_mut(key).unwrap() += nv;
                            }
                        }
                    }

                    if res.is_final() {
                        break;
                    }
                }

                array = read.finalize().expect("Error finalizing read query");
            }
        }
    }

    /// Test that each write in the sequence can be read back correctly at the right timestamp
    #[test]
    #[ignore]
    fn write_readback() {
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
