use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::ops::Range;
use std::rc::Rc;

use paste::paste;
use proptest::bits::{BitSetLike, VarBitSet};
use proptest::collection::SizeRange;
use proptest::prelude::*;
use proptest::strategy::{NewTree, ValueTree};
use proptest::test_runner::TestRunner;

use crate::array::schema::FieldData as SchemaField;
use crate::array::{ArrayType, CellValNum, SchemaData};
use crate::datatype::physical::{BitsEq, BitsOrd, IntegralType};
use crate::datatype::LogicalType;
use crate::query::read::output::{
    CellStructureSingleIterator, FixedDataIterator, RawReadOutput,
    TypedRawReadOutput, VarDataIterator,
};
use crate::query::read::{
    CallbackVarArgReadBuilder, FieldMetadata, ManagedBuffer, Map,
    RawReadHandle, ReadCallbackVarArg, ReadQueryBuilder, TypedReadHandle,
};
use crate::query::WriteBuilder;
use crate::{
    dimension_constraints_go, fn_typed, typed_query_buffers_go, Datatype,
    Result as TileDBResult,
};

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
                ncells: value.ncells,
                input: handle.borrow(),
            };
            match rr.input.cell_structure.as_cell_val_num() {
                CellValNum::Fixed(nz) if nz.get() == 1 => Self::from(
                    CellStructureSingleIterator::try_from(rr)
                        .unwrap()
                        .collect::<Vec<DT>>(),
                ),
                CellValNum::Fixed(_) => Self::from(
                    FixedDataIterator::try_from(rr)
                        .unwrap()
                        .map(|slice| slice.to_vec())
                        .collect::<Vec<Vec<DT>>>(),
                ),
                CellValNum::Var => Self::from(
                    VarDataIterator::try_from(rr)
                        .unwrap()
                        .map(|s| s.to_vec())
                        .collect::<Vec<Vec<DT>>>(),
                ),
            }
        })
    }
}

/// Applies a generic expression to the interior of a `FieldData` value.
///
/// The first form of this macro applies the same expression to all variants.
/// The second form enables applying a different expression to the forms
/// with an interior `Vec<DT>` versus `Vec<Vec<DT>>`.
/// The third form enables applying a different expression to the forms
/// with an interior `Vec<DT>` versus `Vec<FT>` versus `Vec<Vec<DT>>` versus `Vec<Vec<FT>>`,
/// where `DT` is an integral type and `FT` is a floating-point type.
///
/// # Examples
/// ```
/// use tiledb::query::strategy::FieldData;
/// use tiledb::typed_field_data_go;
///
/// fn dedup_cells(cells: &mut FieldData) {
///     typed_field_data_go!(cells, ref mut cells_interior, cells_interior.dedup())
/// }
/// let mut cells = FieldData::UInt64(vec![1, 2, 2, 3, 2]);
/// dedup_cells(&mut cells);
/// assert_eq!(cells, FieldData::UInt64(vec![1, 2, 3, 2]));
/// ```
#[macro_export]
macro_rules! typed_field_data_go {
    ($field:expr, $data:pat, $then:expr) => {
        typed_field_data_go!($field, _DT, $data, $then, $then)
    };
    ($field:expr, $DT:ident, $data:pat, $fixed:expr, $var:expr) => {
        typed_field_data_go!($field, $DT, $data, $fixed, $var, $fixed, $var)
    };
    ($field:expr, $DT:ident, $data:pat, $integral_fixed:expr, $integral_var:expr, $float_fixed:expr, $float_var:expr) => {{
        use $crate::query::strategy::FieldData;
        match $field {
            FieldData::UInt8($data) => {
                type $DT = u8;
                $integral_fixed
            }
            FieldData::UInt16($data) => {
                type $DT = u16;
                $integral_fixed
            }
            FieldData::UInt32($data) => {
                type $DT = u32;
                $integral_fixed
            }
            FieldData::UInt64($data) => {
                type $DT = u64;
                $integral_fixed
            }
            FieldData::Int8($data) => {
                type $DT = i8;
                $integral_fixed
            }
            FieldData::Int16($data) => {
                type $DT = i16;
                $integral_fixed
            }
            FieldData::Int32($data) => {
                type $DT = i32;
                $integral_fixed
            }
            FieldData::Int64($data) => {
                type $DT = i64;
                $integral_fixed
            }
            FieldData::Float32($data) => {
                type $DT = f32;
                $float_fixed
            }
            FieldData::Float64($data) => {
                type $DT = f64;
                $float_fixed
            }
            FieldData::VecUInt8($data) => {
                type $DT = u8;
                $integral_var
            }
            FieldData::VecUInt16($data) => {
                type $DT = u16;
                $integral_var
            }
            FieldData::VecUInt32($data) => {
                type $DT = u32;
                $integral_var
            }
            FieldData::VecUInt64($data) => {
                type $DT = u64;
                $integral_var
            }
            FieldData::VecInt8($data) => {
                type $DT = i8;
                $integral_var
            }
            FieldData::VecInt16($data) => {
                type $DT = i16;
                $integral_var
            }
            FieldData::VecInt32($data) => {
                type $DT = i32;
                $integral_var
            }
            FieldData::VecInt64($data) => {
                type $DT = i64;
                $integral_var
            }
            FieldData::VecFloat32($data) => {
                type $DT = f32;
                $float_var
            }
            FieldData::VecFloat64($data) => {
                type $DT = f64;
                $float_var
            }
        }
    }};
}

/// Applies a generic expression to the interiors of two `FieldData` values with matching variants,
/// i.e. with the same physical data type. Typical usage is for comparing the insides of the two
/// `FieldData` values.
macro_rules! typed_field_data_cmp {
    ($lexpr:expr, $rexpr:expr, $DT:ident, $lpat:pat, $rpat:pat, $same_type:expr, $else:expr) => {{
        use $crate::query::strategy::FieldData;
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

    pub fn is_cell_single(&self) -> bool {
        typed_field_data_go!(self, _DT, _, true, false)
    }

    pub fn slice(&self, start: usize, len: usize) -> FieldData {
        typed_field_data_go!(self, ref values, {
            FieldData::from(values[start..start + len].to_vec().clone())
        })
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

    pub fn truncate(&mut self, len: usize) {
        typed_field_data_go!(self, ref mut data, data.truncate(len))
    }

    pub fn sort(&mut self) {
        typed_field_data_go!(
            self,
            DT,
            ref mut data,
            {
                let cmp = |k1: &DT, k2: &DT| k1.bits_cmp(k2);
                data.sort_by(cmp)
            },
            {
                let cmp = |k1: &Vec<DT>, k2: &Vec<DT>| k1.bits_cmp(k2);
                data.sort_by(cmp)
            }
        );
    }

    pub fn extend(&mut self, other: Self) {
        typed_field_data_cmp!(
            self,
            other,
            _DT,
            ref mut data,
            other_data,
            {
                // the field types match
                data.extend(other_data);
            },
            {
                // if they do not match
                panic!("Field types do not match in `FieldData::extend`")
            }
        )
    }
}

impl BitsEq for FieldData {
    fn bits_eq(&self, other: &Self) -> bool {
        typed_field_data_cmp!(
            self,
            other,
            _DT,
            ref data,
            ref other_data,
            data.bits_eq(other_data), // match
            false                     // fields do not match
        )
    }
}

#[derive(Clone, Debug)]
pub enum FieldStrategyDatatype {
    Datatype(Datatype, CellValNum),
    SchemaField(SchemaField),
}

pub enum FieldValueStrategy {
    UInt8(BoxedStrategy<u8>),
    UInt16(BoxedStrategy<u16>),
    UInt32(BoxedStrategy<u32>),
    UInt64(BoxedStrategy<u64>),
    Int8(BoxedStrategy<i8>),
    Int16(BoxedStrategy<i16>),
    Int32(BoxedStrategy<i32>),
    Int64(BoxedStrategy<i64>),
    Float32(BoxedStrategy<f32>),
    Float64(BoxedStrategy<f64>),
}

macro_rules! field_value_strategy {
    ($($variant:ident : $T:ty),+) => {
        $(
            impl From<BoxedStrategy<$T>> for FieldValueStrategy {
                fn from(value: BoxedStrategy<$T>) -> Self {
                    Self::$variant(value)
                }
            }

            impl TryFrom<FieldValueStrategy> for BoxedStrategy<$T> {
                type Error = ();
                fn try_from(value: FieldValueStrategy) -> Result<Self, Self::Error> {
                    if let FieldValueStrategy::$variant(b) = value {
                        Ok(b)
                    } else {
                        Err(())
                    }
                }
            }
        )+
    }
}

field_value_strategy!(UInt8 : u8, UInt16 : u16, UInt32 : u32, UInt64 : u64);
field_value_strategy!(Int8 : i8, Int16 : i16, Int32 : i32, Int64 : i64);
field_value_strategy!(Float32 : f32, Float64 : f64);

#[derive(Clone, Debug)]
pub struct FieldDataParameters {
    pub nrecords: SizeRange,
    pub datatype: Option<FieldStrategyDatatype>,
    pub value_min_var_size: usize,
    pub value_max_var_size: usize,
    pub unique: bool,
}

impl FieldDataParameters {
    pub fn require_unique_cells(&self) -> bool {
        self.unique
    }
}

impl Default for FieldDataParameters {
    fn default() -> Self {
        FieldDataParameters {
            nrecords: (0..=1024).into(),
            datatype: None,
            value_min_var_size: 0,
            value_max_var_size: 8, /* TODO */
            unique: false,
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
            if params.require_unique_cells() {
                proptest::collection::btree_set(value_strat, params.nrecords)
                    .prop_flat_map(|cell_set| {
                        Just(cell_set.into_iter().collect::<Vec<_>>())
                            .prop_shuffle()
                            .prop_map(FieldData::from)
                    })
                    .boxed()
            } else {
                proptest::collection::vec(value_strat, params.nrecords)
                    .prop_map(FieldData::from)
                    .boxed()
            }
        } else {
            let (min, max) = if cell_val_num.is_var_sized() {
                (params.value_min_var_size, params.value_max_var_size)
            } else {
                let fixed_bound = Into::<u32>::into(cell_val_num) as usize;
                (fixed_bound, fixed_bound)
            };

            let cell_strat = proptest::collection::vec(value_strat, min..=max);

            if params.require_unique_cells() {
                proptest::collection::btree_set(cell_strat, params.nrecords)
                    .prop_flat_map(|cell_set| {
                        Just(cell_set.into_iter().collect::<Vec<_>>())
                            .prop_shuffle()
                            .prop_map(FieldData::from)
                    })
                    .boxed()
            } else {
                proptest::collection::vec(cell_strat, params.nrecords)
                    .prop_map(FieldData::from)
                    .boxed()
            }
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
                let cell_val_num =
                    d.cell_val_num.unwrap_or(CellValNum::single());

                /* if unique values are required then the request may not be satisfiable */
                if params.require_unique_cells()
                    && !matches!(d.cell_val_num, Some(CellValNum::Var))
                {
                    let CellValNum::Fixed(nz) =
                        d.cell_val_num.unwrap_or(CellValNum::single())
                    else {
                        unreachable!()
                    };

                    if let Some(num_cells) = d.constraints.num_cells() {
                        let num_cell_values = num_cells * nz.get() as u128;
                        if num_cell_values < params.nrecords.start() as u128 {
                            panic!("Uniqueness is not satisfiable for strategy parameters: nrecords = {:?}, num_cells = {:?}, dimension = {:?}", params.nrecords, num_cells, d);
                        }
                    }
                }

                dimension_constraints_go!(
                    d.constraints,
                    DT,
                    ref domain,
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

                fn_typed!(a.datatype, LT, {
                    type DT = <LT as LogicalType>::PhysicalType;
                    <DT as ArbitraryFieldData>::arbitrary(
                        params,
                        cell_val_num,
                        value_strat.try_into().unwrap(),
                    )
                })
            }
            Some(FieldStrategyDatatype::Datatype(datatype, cell_val_num)) => {
                fn_typed!(datatype, LT, {
                    type DT = <LT as LogicalType>::PhysicalType;
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
                    fn_typed!(datatype, LT, {
                        type DT = <LT as LogicalType>::PhysicalType;
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

/// Query callback which accumulates results from each step into `Cells`
/// and returns the `Cells` as the final result.
#[derive(Default)]
pub struct CellsConstructor {
    cells: Option<Cells>,
}

impl CellsConstructor {
    pub fn new() -> Self {
        CellsConstructor { cells: None }
    }
}

impl Map<RawReadQueryResult, RawReadQueryResult> for CellsConstructor {
    type Intermediate = ();
    type Final = Cells;

    fn map_intermediate(
        &mut self,
        batch: RawReadQueryResult,
    ) -> Self::Intermediate {
        let batch = Cells::new(batch.0);
        if let Some(cells) = self.cells.as_mut() {
            cells.extend(batch);
        } else {
            self.cells = Some(batch)
        }
    }

    fn map_final(mut self, batch: RawReadQueryResult) -> Self::Final {
        self.map_intermediate(batch);
        self.cells.unwrap()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Cells {
    fields: HashMap<String, FieldData>,
}

impl Cells {
    /// # Panics
    ///
    /// Panics if the fields do not all have the same number of cells.
    pub fn new(fields: HashMap<String, FieldData>) -> Self {
        let mut expect_len: Option<usize> = None;
        for (_, d) in fields.iter() {
            if let Some(expect_len) = expect_len {
                assert_eq!(d.len(), expect_len);
            } else {
                expect_len = Some(d.len())
            }
        }

        Cells { fields }
    }

    pub fn is_empty(&self) -> bool {
        self.fields.values().next().unwrap().is_empty()
    }

    pub fn len(&self) -> usize {
        self.fields.values().next().unwrap().len()
    }

    pub fn fields(&self) -> &HashMap<String, FieldData> {
        &self.fields
    }

    pub fn attach_write<'data>(
        &'data self,
        b: WriteBuilder<'data>,
    ) -> TileDBResult<WriteBuilder<'data>> {
        let mut b = b;
        for f in self.fields.iter() {
            b = typed_field_data_go!(f.1, data, b.data_typed(f.0, data))?;
        }
        Ok(b)
    }

    pub fn attach_read<'data, B>(
        &self,
        b: B,
    ) -> TileDBResult<CallbackVarArgReadBuilder<'data, RawResultCallback, B>>
    where
        B: ReadQueryBuilder<'data>,
    {
        let field_order = self.fields.keys().cloned().collect::<Vec<_>>();
        let handles = {
            let schema = b.base().array().schema().unwrap();

            field_order
                .iter()
                .map(|name| {
                    let field = schema.field(name.clone()).unwrap();
                    fn_typed!(field.datatype().unwrap(), LT, {
                        type DT = <LT as LogicalType>::PhysicalType;
                        let managed: ManagedBuffer<DT> = ManagedBuffer::new(
                            field.query_scratch_allocator().unwrap(),
                        );
                        let metadata = FieldMetadata::try_from(&field).unwrap();
                        let rr = RawReadHandle::managed(metadata, managed);
                        TypedReadHandle::from(rr)
                    })
                })
                .collect::<Vec<TypedReadHandle>>()
        };

        b.register_callback_var(handles, RawResultCallback { field_order })
    }

    /// Copies data from the argument.
    /// Overwrites data at common indices and extends `self` where necessary.
    pub fn copy_from(&mut self, cells: Self) {
        for (field, data) in cells.fields.into_iter() {
            match self.fields.entry(field) {
                Entry::Vacant(v) => {
                    v.insert(data);
                }
                Entry::Occupied(mut o) => {
                    let prev_write_data = o.get_mut();
                    typed_field_data_cmp!(
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

    /// Shortens the cells, keeping the first `len` records and dropping the rest.
    pub fn truncate(&mut self, len: usize) {
        for data in self.fields.values_mut() {
            data.truncate(len)
        }
    }

    /// Extends this cell data with the contents of another.
    ///
    /// # Panics
    ///
    /// Panics if the set of fields in `self` and `other` do not match.
    ///
    /// Panics if any field in `self` and `other` has a different type.
    pub fn extend(&mut self, other: Self) {
        let mut other = other;
        for (field, data) in self.fields.iter_mut() {
            let other_data = other.fields.remove(field).unwrap();
            data.extend(other_data);
        }
        assert_eq!(other.fields.len(), 0);
    }

    fn index_comparator(&self) -> impl Fn(&usize, &usize) -> Ordering + '_ {
        let key_order = {
            let mut keys = self.fields.keys().collect::<Vec<&String>>();
            keys.sort_unstable();
            keys
        };

        move |l: &usize, r: &usize| -> Ordering {
            for key in key_order.iter() {
                typed_field_data_go!(self.fields[*key], ref data, {
                    match BitsOrd::bits_cmp(&data[*l], &data[*r]) {
                        Ordering::Less => return Ordering::Less,
                        Ordering::Greater => return Ordering::Greater,
                        Ordering::Equal => continue,
                    }
                })
            }
            Ordering::Equal
        }
    }

    /// Returns whether the cells are sorted.
    /// The first sort key is the first field in alphabetical order,
    /// the second key is the second, and so on.
    pub fn is_sorted(&self) -> bool {
        let index_comparator = self.index_comparator();
        for i in 1..self.len() {
            if index_comparator(&(i - 1), &i) == Ordering::Greater {
                return false;
            }
        }
        true
    }

    /// Sorts the cells of the argument for comparing a write and read set.
    /// The first sort key is the first field in alphabetical order,
    /// the second key is the second, and so on.
    pub fn sort(&mut self) {
        let mut idx = std::iter::repeat(())
            .take(self.len())
            .enumerate()
            .map(|(i, _)| i)
            .collect::<Vec<usize>>();

        let idx_comparator = self.index_comparator();
        idx.sort_by(idx_comparator);

        for data in self.fields.values_mut() {
            typed_field_data_go!(data, ref mut data, {
                let mut unsorted = std::mem::replace(
                    data,
                    vec![Default::default(); data.len()],
                );
                for i in 0..unsorted.len() {
                    data[i] = std::mem::take(&mut unsorted[idx[i]]);
                }
            });
        }
    }

    /// Returns a copy of the cells, sorted as if by `self.sort()`.
    pub fn sorted(&self) -> Self {
        let mut sorted = self.clone();
        sorted.sort();
        sorted
    }

    /// Returns the number of distinct values grouped on `keys`
    pub fn count_distinct(&self, keys: &[String]) -> usize {
        if self.len() <= 1 {
            return self.len();
        }

        let key_cells = {
            let key_fields = self
                .fields
                .iter()
                .filter(|(k, _)| keys.contains(k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<HashMap<_, _>>();
            Cells::new(key_fields).sorted()
        };

        let mut icmp = 0;
        let mut count = 1;

        for i in 1..key_cells.len() {
            let distinct = keys.iter().any(|k| {
                let v = key_cells.fields().get(k).unwrap();
                typed_field_data_go!(
                    v,
                    ref cells,
                    cells[i].bits_ne(&cells[icmp])
                )
            });
            if distinct {
                icmp = i;
                count += 1;
            }
        }

        count
    }

    /// Returns a subset of the records using the bitmap to determine which are included
    pub fn filter(&self, set: &VarBitSet) -> Cells {
        Self::new(
            self.fields()
                .iter()
                .map(|(k, v)| (k.clone(), v.filter(set)))
                .collect::<HashMap<String, FieldData>>(),
        )
    }
}

impl BitsEq for Cells {
    fn bits_eq(&self, other: &Self) -> bool {
        for (key, mine) in self.fields().iter() {
            if let Some(theirs) = other.fields().get(key) {
                if !mine.bits_eq(theirs) {
                    return false;
                }
            } else {
                return false;
            }
        }
        self.fields().keys().len() == other.fields().keys().len()
    }
}

pub struct StructuredCells {
    dimensions: Vec<usize>,
    cells: Cells,
}

impl StructuredCells {
    pub fn new(dimensions: Vec<usize>, cells: Cells) -> Self {
        let expected_cells: usize = dimensions.iter().cloned().product();
        assert_eq!(expected_cells, cells.len(), "Dimensions: {:?}", dimensions);

        StructuredCells { dimensions, cells }
    }

    pub fn num_dimensions(&self) -> usize {
        self.dimensions.len()
    }

    /// Returns the span of dimension `d`
    pub fn dimension_len(&self, d: usize) -> usize {
        self.dimensions[d]
    }

    pub fn into_inner(self) -> Cells {
        self.cells
    }

    pub fn slice(&self, slices: Vec<Range<usize>>) -> Self {
        assert_eq!(slices.len(), self.dimensions.len()); // this is doable but unimportant

        struct NextIndex<'a> {
            dimensions: &'a [usize],
            ranges: &'a [Range<usize>],
            cursors: Option<Vec<usize>>,
        }

        impl<'a> NextIndex<'a> {
            fn new(
                dimensions: &'a [usize],
                ranges: &'a [Range<usize>],
            ) -> Self {
                for r in ranges {
                    if r.is_empty() {
                        return NextIndex {
                            dimensions,
                            ranges,
                            cursors: None,
                        };
                    }
                }

                NextIndex {
                    dimensions,
                    ranges,
                    cursors: Some(
                        ranges.iter().map(|r| r.start).collect::<Vec<usize>>(),
                    ),
                }
            }

            fn compute(&self) -> usize {
                let Some(cursors) = self.cursors.as_ref() else {
                    unreachable!()
                };
                let mut index = 0;
                let mut scale = 1;
                for i in 0..self.dimensions.len() {
                    let i = self.dimensions.len() - i - 1;
                    index += cursors[i] * scale;
                    scale *= self.dimensions[i];
                }
                index
            }

            fn advance(&mut self) {
                let Some(cursors) = self.cursors.as_mut() else {
                    return;
                };
                for d in 0..self.dimensions.len() {
                    let d = self.dimensions.len() - d - 1;
                    if cursors[d] + 1 < self.ranges[d].end {
                        cursors[d] += 1;
                        return;
                    } else {
                        cursors[d] = self.ranges[d].start;
                    }
                }

                // this means that we reset the final dimension
                self.cursors = None;
            }
        }

        impl<'a> Iterator for NextIndex<'a> {
            type Item = usize;
            fn next(&mut self) -> Option<Self::Item> {
                if self.cursors.is_some() {
                    let index = self.compute();
                    self.advance();
                    Some(index)
                } else {
                    None
                }
            }
        }

        let mut v = VarBitSet::new_bitset(self.cells.len());

        NextIndex::new(self.dimensions.as_slice(), slices.as_slice())
            .for_each(|idx| v.set(idx));

        StructuredCells {
            dimensions: self.dimensions.clone(),
            cells: self.cells.filter(&v),
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

/// Tracks the last step taken for the write shrinking.
enum ShrinkSearchStep {
    /// Remove a range of records
    Explore(usize),
    Recur,
    Done,
}

const WRITE_QUERY_DATA_VALUE_TREE_EXPLORE_PIECES: usize = 8;

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
    params: CellsParameters,
    field_data: HashMap<String, (FieldMask, Option<FieldData>)>,
    nrecords: usize,
    last_records_included: Option<Vec<usize>>,
    records_included: Vec<usize>,
    explore_results: Box<[Option<bool>]>,
    search: Option<ShrinkSearchStep>,
}

impl CellsValueTree {
    pub fn new(
        params: CellsParameters,
        field_data: HashMap<String, (FieldMask, Option<FieldData>)>,
    ) -> Self {
        let nrecords = field_data
            .values()
            .filter_map(|(_, f)| f.as_ref())
            .take(1)
            .next()
            .unwrap()
            .len();

        let nchunks = WRITE_QUERY_DATA_VALUE_TREE_EXPLORE_PIECES;
        let records_included = (0..nrecords).collect::<Vec<usize>>();

        CellsValueTree {
            params,
            field_data,
            nrecords,
            last_records_included: None,
            records_included,
            explore_results: vec![None; nchunks].into_boxed_slice(),
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
                    self.explore_results.len(),
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

                        if new_records_included.len() < self.params.min_records
                        {
                            /* buffer with some extras because the strategy requires it */
                            let mut rec = new_records_included
                                .into_iter()
                                .collect::<BTreeSet<usize>>();
                            let mut i = 0;
                            while rec.len() < self.params.min_records {
                                rec.insert(i);
                                i += 1;
                            }
                            new_records_included =
                                rec.into_iter().collect::<Vec<usize>>();
                        }

                        if new_records_included == self.records_included {
                            /* everything was needed to pass */
                            self.search = Some(ShrinkSearchStep::Done);
                        } else {
                            self.last_records_included =
                                Some(std::mem::replace(
                                    &mut self.records_included,
                                    new_records_included,
                                ));
                            self.search = Some(ShrinkSearchStep::Recur);
                        }
                        /* run another round on the updated input */
                        true
                    }
                    Ordering::Greater => {
                        assert_eq!(0, nchunks);
                        false
                    }
                }
            }
            Some(ShrinkSearchStep::Recur) => {
                if failed {
                    self.search = Some(ShrinkSearchStep::Explore(0));
                } else {
                    /*
                     * This means that removing more than one chunk causes the
                     * test to no longer fail.
                     * Try again with a larger chunk size if possible
                     */
                    if self.explore_results.len() == 1 {
                        unreachable!()
                    }
                    let Some(last_records_included) =
                        self.last_records_included.take()
                    else {
                        unreachable!()
                    };
                    self.last_records_included = None;
                    self.records_included = last_records_included;
                    self.explore_results =
                        vec![None; self.explore_results.len() / 2]
                            .into_boxed_slice();
                }
                self.search = Some(ShrinkSearchStep::Explore(0));
                true
            }
            Some(ShrinkSearchStep::Done) => false,
        }
    }
}

impl ValueTree for CellsValueTree {
    type Value = Cells;

    fn current(&self) -> Self::Value {
        let record_mask = match self.search {
            None => VarBitSet::saturated(self.nrecords),
            Some(ShrinkSearchStep::Explore(c)) => {
                let nchunks = self
                    .records_included
                    .len()
                    .clamp(1, self.explore_results.len());

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
            .field_data
            .iter()
            .filter(|(_, &(mask, _))| mask.is_included())
            .map(|(name, (_, data))| {
                (name.clone(), data.as_ref().unwrap().filter(&record_mask))
            })
            .collect::<HashMap<String, FieldData>>();

        Cells::new(fields)
    }

    fn simplify(&mut self) -> bool {
        self.explore_step(true)
    }

    fn complicate(&mut self) -> bool {
        self.explore_step(false)
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
        nrecords: usize,
    ) -> HashMap<String, (FieldMask, Option<FieldData>)> {
        let field_data_parameters_base = FieldDataParameters::default();

        match self {
            Self::Fields(fields) => {
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

                field_mask
                    .into_iter()
                    .map(|(field, mask)| {
                        let require_unique_values = field.is_dimension()
                            && !schema.allow_duplicates.unwrap_or(false);
                        let field_name = field.name().to_string();
                        let field_data = if mask.is_included() {
                            let params = FieldDataParameters {
                                nrecords: nrecords.into(),
                                datatype: Some(
                                    FieldStrategyDatatype::SchemaField(field),
                                ),
                                unique: require_unique_values,
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
    pub value_min_var_size: usize,
    pub value_max_var_size: usize,
}

impl Default for CellsParameters {
    fn default() -> Self {
        const WRITE_QUERY_MIN_RECORDS: usize = 0;
        const WRITE_QUERY_MAX_RECORDS: usize = 16;

        const WRITE_QUERY_MIN_VAR_SIZE: usize = 0;
        const WRITE_QUERY_MAX_VAR_SIZE: usize = 8;

        CellsParameters {
            schema: None,
            min_records: WRITE_QUERY_MIN_RECORDS,
            max_records: WRITE_QUERY_MAX_RECORDS,
            value_min_var_size: WRITE_QUERY_MIN_VAR_SIZE,
            value_max_var_size: WRITE_QUERY_MAX_VAR_SIZE,
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
                /*
                 * TODO: this is a much larger constraint than it needs to
                 * be, we want all the rows to be unique but right now
                 * too much of the strategy reasons about columns, so it's
                 * going to be a big effort to restrict this down
                 * to `DomainData::num_cells` instead
                 */
                return usize::try_from(
                    schema
                        .domain
                        .dimension
                        .iter()
                        .filter_map(|d| d.constraints.num_cells())
                        .min()?,
                )
                .ok();
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
                todo!()
            } else {
                let max_records = std::cmp::min(self.params.max_records, limit);
                self.params.min_records..=max_records
            }
        } else {
            self.params.min_records..=self.params.max_records
        };

        let nrecords = strat_nrecords.new_tree(runner)?.current();

        /* generate an initial set of fields to write */
        let field_tree = self.schema.new_field_tree(runner, nrecords);

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
            let keys = crate::array::attribute::strategy::prop_attribute_name();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatype::physical::BitsKeyAdapter;
    use std::collections::HashSet;

    fn do_field_data_unique(data: FieldData) {
        typed_field_data_go!(
            data,
            _DT,
            values,
            {
                let num_cells = values.len();
                let num_unique_cells =
                    values.clone().into_iter().collect::<HashSet<_>>().len();
                assert_eq!(
                    num_cells, num_unique_cells,
                    "values = {:?}",
                    values
                );
            },
            {
                let num_cells = values.len();
                let num_unique_cells =
                    values.clone().into_iter().collect::<HashSet<_>>().len();
                assert_eq!(
                    num_cells, num_unique_cells,
                    "values = {:?}",
                    values
                );
            },
            {
                let values =
                    values.into_iter().map(|f| f.to_bits()).collect::<Vec<_>>();
                let num_cells = values.len();
                let num_unique_cells =
                    values.clone().into_iter().collect::<HashSet<_>>().len();
                assert_eq!(
                    num_cells, num_unique_cells,
                    "values.to_bits = {:?}",
                    values
                );
            },
            {
                let values = values
                    .into_iter()
                    .map(|v| {
                        v.into_iter().map(|f| f.to_bits()).collect::<Vec<_>>()
                    })
                    .collect::<Vec<Vec<_>>>();
                let num_cells = values.len();
                let num_unique_cells =
                    values.clone().into_iter().collect::<HashSet<_>>().len();
                assert_eq!(
                    num_cells, num_unique_cells,
                    "values.to_bits = {:?}",
                    values
                );
            }
        )
    }

    fn do_field_data_extend(dst: FieldData, src: FieldData) {
        let orig_dst = dst.clone();
        let orig_src = src.clone();

        let mut dst = dst;
        dst.extend(src);

        typed_field_data_go!(dst, dst, {
            assert_eq!(
                orig_dst,
                FieldData::from(dst[0..orig_dst.len()].to_vec())
            );
            assert_eq!(
                orig_src,
                FieldData::from(dst[orig_dst.len()..dst.len()].to_vec())
            );
            assert_eq!(dst.len(), orig_dst.len() + orig_src.len());
        })
    }

    fn do_cells_extend(dst: Cells, src: Cells) {
        let orig_dst = dst.clone();
        let orig_src = src.clone();

        let mut dst = dst;
        dst.extend(src);

        for (fname, data) in dst.fields().iter() {
            let orig_dst = orig_dst.fields().get(fname).unwrap();
            let orig_src = orig_src.fields().get(fname).unwrap();

            typed_field_data_go!(data, ref dst, {
                assert_eq!(
                    *orig_dst,
                    FieldData::from(dst[0..orig_dst.len()].to_vec())
                );
                assert_eq!(
                    *orig_src,
                    FieldData::from(dst[orig_dst.len()..dst.len()].to_vec())
                );
                assert_eq!(dst.len(), orig_dst.len() + orig_src.len());
            });
        }

        // all Cells involved should have same set of fields
        assert_eq!(orig_dst.fields.len(), dst.fields.len());
        assert_eq!(orig_src.fields.len(), dst.fields.len());
    }

    fn do_cells_sort(cells: Cells) {
        let cells_sorted = cells.sorted();
        assert!(cells_sorted.is_sorted());

        assert_eq!(cells.fields().len(), cells_sorted.fields().len());

        if cells.is_sorted() {
            // running the sort should not have changed anything
            assert_eq!(cells, cells_sorted);
        }

        /*
         * We want to verify that the contents of the records are the
         * same before and after the sort. We can precisely do that
         * with a hash join, though it's definitely tricky to turn
         * the columnar data into rows, or we can approximate it
         * by sorting and comparing each column, which is not fully
         * precise but way easier.
         */
        for (fname, data) in cells.fields().iter() {
            let Some(data_sorted) = cells_sorted.fields().get(fname) else {
                unreachable!()
            };

            let orig_sorted = {
                let mut orig = data.clone();
                orig.sort();
                orig
            };
            let sorted_sorted = {
                let mut sorted = data_sorted.clone();
                sorted.sort();
                sorted
            };
            assert_eq!(orig_sorted, sorted_sorted);
        }
    }

    fn do_cells_slice_1d(cells: Cells, slice: Range<usize>) {
        let cells = StructuredCells::new(vec![cells.len()], cells);
        let sliced = cells.slice(vec![slice.clone()]).into_inner();
        let cells = cells.into_inner();

        assert_eq!(cells.fields().len(), sliced.fields().len());

        for (key, value) in cells.fields().iter() {
            let Some(sliced) = sliced.fields().get(key) else {
                unreachable!()
            };
            assert_eq!(
                value.slice(slice.start, slice.end - slice.start),
                *sliced
            );
        }
    }

    fn do_cells_slice_2d(
        cells: Cells,
        d1: usize,
        d2: usize,
        s1: Range<usize>,
        s2: Range<usize>,
    ) {
        let mut cells = cells;
        cells.truncate(d1 * d2);

        let cells = StructuredCells::new(vec![d1, d2], cells);
        let sliced = cells.slice(vec![s1.clone(), s2.clone()]).into_inner();
        let cells = cells.into_inner();

        assert_eq!(cells.fields().len(), sliced.fields().len());

        for (key, value) in cells.fields.iter() {
            let Some(sliced) = sliced.fields().get(key) else {
                unreachable!()
            };

            assert_eq!(s1.len() * s2.len(), sliced.len());

            typed_field_data_cmp!(
                value,
                sliced,
                _DT,
                ref value_data,
                ref sliced_data,
                {
                    for r in s1.clone() {
                        let value_start = (r * d2) + s2.start;
                        let value_end = (r * d2) + s2.end;
                        let value_expect = &value_data[value_start..value_end];

                        let sliced_start = (r - s1.start) * s2.len();
                        let sliced_end = (r + 1 - s1.start) * s2.len();
                        let sliced_cmp = &sliced_data[sliced_start..sliced_end];

                        assert_eq!(value_expect, sliced_cmp);
                    }
                },
                unreachable!()
            );
        }
    }

    fn do_cells_slice_3d(
        cells: Cells,
        d1: usize,
        d2: usize,
        d3: usize,
        s1: Range<usize>,
        s2: Range<usize>,
        s3: Range<usize>,
    ) {
        let mut cells = cells;
        cells.truncate(d1 * d2 * d3);

        let cells = StructuredCells::new(vec![d1, d2, d3], cells);
        let sliced = cells
            .slice(vec![s1.clone(), s2.clone(), s3.clone()])
            .into_inner();
        let cells = cells.into_inner();

        assert_eq!(cells.fields().len(), sliced.fields().len());

        for (key, value) in cells.fields.iter() {
            let Some(sliced) = sliced.fields.get(key) else {
                unreachable!()
            };

            assert_eq!(s1.len() * s2.len() * s3.len(), sliced.len());

            typed_field_data_cmp!(
                value,
                sliced,
                _DT,
                ref value_data,
                ref sliced_data,
                {
                    for z in s1.clone() {
                        for y in s2.clone() {
                            let value_start =
                                (z * d2 * d3) + (y * d3) + s3.start;
                            let value_end = (z * d2 * d3) + (y * d3) + s3.end;
                            let value_expect =
                                &value_data[value_start..value_end];

                            let sliced_start =
                                ((z - s1.start) * s2.len() * s3.len())
                                    + ((y - s2.start) * s3.len());
                            let sliced_end =
                                ((z - s1.start) * s2.len() * s3.len())
                                    + ((y + 1 - s2.start) * s3.len());
                            let sliced_cmp =
                                &sliced_data[sliced_start..sliced_end];

                            assert_eq!(value_expect, sliced_cmp);
                        }
                    }
                },
                unreachable!()
            );
        }
    }

    fn do_cells_count_distinct_1d(cells: Cells) {
        for (key, field_cells) in cells.fields().iter() {
            let expect_count =
                typed_field_data_go!(field_cells, ref field_cells, {
                    let mut c = field_cells.clone();
                    c.sort_by(|l, r| l.bits_cmp(r));
                    c.dedup_by(|l, r| l.bits_eq(r));
                    c.len()
                });

            let keys_for_distinct = vec![key.clone()];
            let actual_count =
                cells.count_distinct(keys_for_distinct.as_slice());

            assert_eq!(expect_count, actual_count);
        }
    }

    fn do_cells_count_distinct_2d(cells: Cells) {
        let keys = cells.fields().keys().collect::<Vec<_>>();

        for i in 0..keys.len() {
            for j in 0..keys.len() {
                let expect_count = {
                    typed_field_data_go!(
                        cells.fields().get(keys[i]).unwrap(),
                        ref ki_cells,
                        {
                            typed_field_data_go!(
                                cells.fields().get(keys[j]).unwrap(),
                                ref kj_cells,
                                {
                                    let mut unique = HashMap::new();

                                    for r in 0..ki_cells.len() {
                                        let values = match unique.entry(
                                            BitsKeyAdapter(ki_cells[r].clone()),
                                        ) {
                                            Entry::Vacant(v) => {
                                                v.insert(HashSet::new())
                                            }
                                            Entry::Occupied(o) => o.into_mut(),
                                        };
                                        values.insert(BitsKeyAdapter(
                                            kj_cells[r].clone(),
                                        ));
                                    }

                                    unique.values().flatten().count()
                                }
                            )
                        }
                    )
                };

                let keys_for_distinct = vec![keys[i].clone(), keys[j].clone()];
                let actual_count =
                    cells.count_distinct(keys_for_distinct.as_slice());

                assert_eq!(expect_count, actual_count);
            }
        }
    }

    proptest! {
        #[test]
        fn field_data_unique(data in any_with::<FieldData>(FieldDataParameters { unique: true, ..Default::default() }))
        {
            do_field_data_unique(data)
        }

        #[test]
        fn field_data_extend((dst, src) in (any::<Datatype>(), any::<CellValNum>()).prop_flat_map(|(dt, cvn)| {
            let params = FieldDataParameters {
                datatype: Some(FieldStrategyDatatype::Datatype(dt, cvn)),
                ..Default::default()
            };
            (any_with::<FieldData>(params.clone()), any_with::<FieldData>(params.clone()))
        })) {
            do_field_data_extend(dst, src)
        }

        #[test]
        fn cells_extend((dst, src) in any::<SchemaData>().prop_flat_map(|s| {
            let params = CellsParameters {
                schema: Some(CellsStrategySchema::WriteSchema(Rc::new(s))),
                ..Default::default()
            };
            (any_with::<Cells>(params.clone()), any_with::<Cells>(params.clone()))
        })) {
            do_cells_extend(dst, src)
        }

        #[test]
        fn cells_sort(cells in any::<Cells>()) {
            do_cells_sort(cells)
        }

        #[test]
        fn cells_slice_1d((cells, bound1, bound2) in any::<Cells>().prop_flat_map(|cells| {
            let slice_min = 0;
            let slice_max = cells.len();
            (Just(cells),
            slice_min..=slice_max,
            slice_min..=slice_max)
        })) {
            let start = std::cmp::min(bound1, bound2);
            let end = std::cmp::max(bound1, bound2);
            do_cells_slice_1d(cells, start.. end)
        }

        #[test]
        fn cells_slice_2d((cells, d1, d2, b11, b12, b21, b22) in any_with::<Cells>(CellsParameters {
            min_records: 1,
            ..Default::default()
        }).prop_flat_map(|cells| {
            let ncells = cells.len();
            (Just(cells),
            1..=((ncells as f64).sqrt() as usize),
            1..=((ncells as f64).sqrt() as usize))
                .prop_flat_map(|(cells, d1, d2)| {
                    (Just(cells),
                    Just(d1),
                    Just(d2),
                    0..=d1,
                    0..=d1,
                    0..=d2,
                    0..=d2)
                })
        })) {
            let s1 = std::cmp::min(b11, b12).. std::cmp::max(b11, b12);
            let s2 = std::cmp::min(b21, b22).. std::cmp::max(b21, b22);
            do_cells_slice_2d(cells, d1, d2, s1, s2)
        }

        #[test]
        fn cells_slice_3d((cells, d1, d2, d3, b11, b12, b21, b22, b31, b32) in any_with::<Cells>(CellsParameters {
            min_records: 1,
            ..Default::default()
        }).prop_flat_map(|cells| {
            let ncells = cells.len();
            (Just(cells),
            1..=((ncells as f64).cbrt() as usize),
            1..=((ncells as f64).cbrt() as usize),
            1..=((ncells as f64).cbrt() as usize))
                .prop_flat_map(|(cells, d1, d2, d3)| {
                    (Just(cells),
                    Just(d1),
                    Just(d2),
                    Just(d3),
                    0..=d1,
                    0..=d1,
                    0..=d2,
                    0..=d2,
                    0..=d3,
                    0..=d3)
                })
        })) {
            let s1 = std::cmp::min(b11, b12).. std::cmp::max(b11, b12);
            let s2 = std::cmp::min(b21, b22).. std::cmp::max(b21, b22);
            let s3 = std::cmp::min(b31, b32).. std::cmp::max(b31, b32);
            do_cells_slice_3d(cells, d1, d2, d3, s1, s2, s3)
        }

        #[test]
        fn cells_count_distinct_1d(cells in any::<Cells>()) {
            do_cells_count_distinct_1d(cells)
        }

        #[test]
        fn cells_count_distinct_2d(cells in any::<Cells>()) {
            prop_assume!(cells.fields().len() >= 2);
            do_cells_count_distinct_2d(cells)
        }
    }
}
