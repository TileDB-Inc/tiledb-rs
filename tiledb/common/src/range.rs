use std::hash::{Hash, Hasher};
use std::num::NonZeroU32;
use std::ops::{Deref, RangeInclusive};

use num_traits::FromPrimitive;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::array::CellValNum;
use crate::datatype::physical::{BitsEq, BitsHash, BitsOrd};
use crate::datatype::{Datatype, Error as DatatypeError};
use crate::physical_type_go;

pub type MinimumBoundingRectangle = Vec<TypedRange>;

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum DimensionCompatibilityError {
    #[error(
        "Dimensions cannot have a multiple-value fixed ranges: found range of size {0}"
    )]
    MultiValueRange(usize),
    #[error("{:?} is invalid for dimensions", CellValNum::Fixed(*.0))]
    CellValNumFixed(NonZeroU32),
    #[error("Dimension of type {} cannot have {:?}", Datatype::StringAscii.to_string(), CellValNum::Fixed(*.0))]
    FixedStringAsciiDimension(NonZeroU32),
    #[error("Dimension of type {0} cannot have variable-length range")]
    VarRangeForNonStringDimension(Datatype),
    #[error("Dimension of type {} cannot have a fixed-length range", Datatype::StringAscii.to_string())]
    FixedRangeForStringDimension,
    #[error("Dimension of type {0} cannot have {:?}", CellValNum::Var)]
    CellValNumVar(Datatype),
    #[error("Datatype error: {0}")]
    Datatype(#[from] DatatypeError),
}

#[derive(Clone, Debug, Error)]
pub enum RangeFromSlicesError {
    #[error("Start range truncation of datatype {0}: expected multiple of {} bytes but found {1}", .0.size())]
    StartTruncation(Datatype, usize),
    #[error("End range truncation of datatype {0}: expected multiple of {} bytes but found {1}", .0.size())]
    EndTruncation(Datatype, usize),

    #[error("Start range invalid number of values: expected {0}, found {1}")]
    StartMultiValueRangeMismatch(NonZeroU32, usize),
    #[error("End range invalid number of values: expected {0}, found {1}")]
    EndMultiValueRangeMismatch(NonZeroU32, usize),
    #[error("Invalid multi-value range: {0}")]
    InvalidMultiValueRange(#[from] MultiValueRangeError),
}

#[derive(Clone, Debug, Error)]
pub enum MultiValueRangeError {
    #[error(
        "Expected multiple value cells but found {:?}; use SingleValueRange instead",
        CellValNum::single()
    )]
    CellValNumSingle,
    #[error("Expected fixed-length {} but found {:?}", std::any::type_name::<CellValNum>(), CellValNum::Var)]
    CellValNumVar,
    #[error("Invalid start range: expected range of length {0} but found {1}")]
    InvalidStartRange(NonZeroU32, usize),
    #[error("Invalid end range: expected range of length {0} but found {1}")]
    InvalidEndRange(NonZeroU32, usize),
}

macro_rules! check_datatype_inner {
    ($ty:ty, $dtype:expr) => {{
        let datatype = $dtype;
        if !datatype.is_compatible_type::<$ty>() {
            return Err(DatatypeError::physical_type_incompatible::<$ty>(
                datatype,
            ));
        }
    }};
}

macro_rules! check_datatype {
    ($self:expr, $datatype:expr) => {
        match $self {
            Self::UInt8(_, _) => check_datatype_inner!(u8, $datatype),
            Self::UInt16(_, _) => check_datatype_inner!(u16, $datatype),
            Self::UInt32(_, _) => check_datatype_inner!(u32, $datatype),
            Self::UInt64(_, _) => check_datatype_inner!(u64, $datatype),
            Self::Int8(_, _) => check_datatype_inner!(i8, $datatype),
            Self::Int16(_, _) => check_datatype_inner!(i16, $datatype),
            Self::Int32(_, _) => check_datatype_inner!(i32, $datatype),
            Self::Int64(_, _) => check_datatype_inner!(i64, $datatype),
            Self::Float32(_, _) => check_datatype_inner!(f32, $datatype),
            Self::Float64(_, _) => check_datatype_inner!(f64, $datatype),
        }
    };
}

fn intersection<'a, B>(
    left_lower: &'a B,
    left_upper: &'a B,
    right_lower: &'a B,
    right_upper: &'a B,
) -> Option<(&'a B, &'a B)>
where
    B: BitsOrd + ?Sized,
{
    // input integrity check
    assert!(left_lower.bits_le(left_upper),);
    assert!(right_lower.bits_le(right_upper),);

    if left_upper.bits_lt(right_lower) || right_upper.bits_lt(left_lower) {
        return None;
    }

    let lower = if left_lower.bits_lt(right_lower) {
        right_lower
    } else {
        left_lower
    };

    let upper = if left_upper.bits_gt(right_upper) {
        right_upper
    } else {
        left_upper
    };

    // output integrity check
    assert!(lower.bits_le(upper),);

    Some((lower, upper))
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum SingleValueRange {
    UInt8(u8, u8),
    UInt16(u16, u16),
    UInt32(u32, u32),
    UInt64(u64, u64),
    Int8(i8, i8),
    Int16(i16, i16),
    Int32(i32, i32),
    Int64(i64, i64),
    Float32(f32, f32),
    Float64(f64, f64),
}

impl SingleValueRange {
    /// Returns the number of cells spanned by this range if it is a
    /// range over a discrete domain.
    /// ```
    /// use tiledb_common::range::SingleValueRange;
    /// assert_eq!(Some(100), SingleValueRange::Int64(1, 100).num_cells());
    /// assert_eq!(None, SingleValueRange::Float64(1.0, 100.0).num_cells());
    /// ```
    pub fn num_cells(&self) -> Option<u128> {
        let (low, high) = crate::single_value_range_go!(self, _DT : Integral, start, end,
            (i128::from(*start), i128::from(*end)),
            return None
        );
        Some(1 + (high - low) as u128)
    }

    /// Returns a `CellValNum` description of values in this range, i.e. `CellValNum::single()`.
    pub fn cell_val_num(&self) -> CellValNum {
        CellValNum::single()
    }

    pub fn is_integral(&self) -> bool {
        matches!(
            self,
            Self::UInt8(_, _)
                | Self::UInt16(_, _)
                | Self::UInt32(_, _)
                | Self::UInt64(_, _)
                | Self::Int8(_, _)
                | Self::Int16(_, _)
                | Self::Int32(_, _)
                | Self::Int64(_, _)
        )
    }

    pub fn check_datatype(
        &self,
        datatype: Datatype,
    ) -> Result<(), DatatypeError> {
        check_datatype!(self, datatype);
        Ok(())
    }

    /// Returns the range covered by the union of `self` and `other`.
    ///
    /// # Panics
    ///
    /// Panics if `self` and `other` do not have the same physical datatype.
    pub fn union(&self, other: &Self) -> Self {
        crate::single_value_range_cmp!(
            self,
            other,
            DT,
            lstart,
            lend,
            rstart,
            rend,
            {
                let cmp = |l: &DT, r: &DT| l.bits_cmp(r);
                let min = std::cmp::min_by(*lstart, *rstart, cmp);
                let max = std::cmp::max_by(*lend, *rend, cmp);
                SingleValueRange::from(&[min, max])
            },
            {
                panic!(
                    "`SingleValueRange::union` on non-matching datatypes: `self` = {:?}, `other` = {:?}",
                    self, other
                )
            }
        )
    }

    /// Returns the range covered by the intersection of `self` and `other`,
    /// or `None` if `self` and `other` do not overlap.
    ///
    /// # Panics
    ///
    /// Panics if `self` and `other` do not have the same physical datatype or the same fixed
    /// length.
    pub fn intersection(&self, other: &Self) -> Option<Self> {
        crate::single_value_range_cmp!(
            self,
            other,
            DT,
            lstart,
            lend,
            rstart,
            rend,
            {
                let (lower, upper) =
                    intersection::<DT>(lstart, lend, rstart, rend)?;
                Some(SingleValueRange::from(&[*lower, *upper]))
            },
            {
                panic!(
                    "`SingleValueRange::intersection` on non-matching datatypes: `self` = {:?}, `other` = {:?}",
                    self, other
                )
            }
        )
    }
}

impl PartialEq for SingleValueRange {
    fn eq(&self, other: &Self) -> bool {
        crate::single_value_range_cmp!(
            self,
            other,
            _DT,
            lstart,
            lend,
            rstart,
            rend,
            lstart.bits_eq(rstart) && lend.bits_eq(rend),
            false
        )
    }
}

/// The [PartialEq] implementation of [SingleValueRange] compares the
/// floating-point variants using [BitsEq],
/// and as such is an equivalence relation.
impl Eq for SingleValueRange {}

/// Uses the [BitsHash] implementation of the wrapped values.
impl Hash for SingleValueRange {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        crate::single_value_range_go!(self, _DT, start, end, {
            start.bits_hash(state);
            end.bits_hash(state);
        })
    }
}

macro_rules! single_value_range_from {
    ($($V:ident : $U:ty),+) => {
        $(
            impl From<&[$U; 2]> for SingleValueRange {
                fn from(value: &[$U; 2]) -> SingleValueRange {
                    SingleValueRange::$V(value[0], value[1])
                }
            }

            impl From<RangeInclusive<$U>> for SingleValueRange {
                fn from(value: RangeInclusive<$U>) -> SingleValueRange {
                    SingleValueRange::$V(*value.start(), *value.end())
                }
            }
        )+
    }
}

single_value_range_from!(UInt8: u8, UInt16: u16, UInt32: u32, UInt64: u64);
single_value_range_from!(Int8: i8, Int16: i16, Int32: i32, Int64: i64);
single_value_range_from!(Float32: f32, Float64: f64);

impl<T> TryFrom<SingleValueRange> for RangeInclusive<T>
where
    T: FromPrimitive,
{
    type Error = SingleValueRange;

    fn try_from(value: SingleValueRange) -> Result<Self, Self::Error> {
        match value.clone() {
            SingleValueRange::UInt8(lower, upper) => {
                let lower = <T>::from_u8(lower).ok_or(value.clone())?;
                let upper = <T>::from_u8(upper).ok_or(value)?;
                Ok(lower..=upper)
            }
            SingleValueRange::UInt16(lower, upper) => {
                let lower = <T>::from_u16(lower).ok_or(value.clone())?;
                let upper = <T>::from_u16(upper).ok_or(value)?;
                Ok(lower..=upper)
            }
            SingleValueRange::UInt32(lower, upper) => {
                let lower = <T>::from_u32(lower).ok_or(value.clone())?;
                let upper = <T>::from_u32(upper).ok_or(value)?;
                Ok(lower..=upper)
            }
            SingleValueRange::UInt64(lower, upper) => {
                let lower = <T>::from_u64(lower).ok_or(value.clone())?;
                let upper = <T>::from_u64(upper).ok_or(value)?;
                Ok(lower..=upper)
            }
            SingleValueRange::Int8(lower, upper) => {
                let lower = <T>::from_i8(lower).ok_or(value.clone())?;
                let upper = <T>::from_i8(upper).ok_or(value)?;
                Ok(lower..=upper)
            }
            SingleValueRange::Int16(lower, upper) => {
                let lower = <T>::from_i16(lower).ok_or(value.clone())?;
                let upper = <T>::from_i16(upper).ok_or(value)?;
                Ok(lower..=upper)
            }
            SingleValueRange::Int32(lower, upper) => {
                let lower = <T>::from_i32(lower).ok_or(value.clone())?;
                let upper = <T>::from_i32(upper).ok_or(value)?;
                Ok(lower..=upper)
            }
            SingleValueRange::Int64(lower, upper) => {
                let lower = <T>::from_i64(lower).ok_or(value.clone())?;
                let upper = <T>::from_i64(upper).ok_or(value)?;
                Ok(lower..=upper)
            }
            SingleValueRange::Float32(lower, upper) => {
                let lower = <T>::from_f32(lower).ok_or(value.clone())?;
                let upper = <T>::from_f32(upper).ok_or(value)?;
                Ok(lower..=upper)
            }
            SingleValueRange::Float64(lower, upper) => {
                let lower = <T>::from_f64(lower).ok_or(value.clone())?;
                let upper = <T>::from_f64(upper).ok_or(value)?;
                Ok(lower..=upper)
            }
        }
    }
}

#[macro_export]
macro_rules! single_value_range_go {
    ($expr:expr, $DT:ident, $start:pat, $end:pat, $then:expr) => {
        match $expr {
            SingleValueRange::UInt8($start, $end) => {
                type $DT = u8;
                $then
            }
            SingleValueRange::UInt16($start, $end) => {
                type $DT = u16;
                $then
            }
            SingleValueRange::UInt32($start, $end) => {
                type $DT = u32;
                $then
            }
            SingleValueRange::UInt64($start, $end) => {
                type $DT = u64;
                $then
            }
            SingleValueRange::Int8($start, $end) => {
                type $DT = i8;
                $then
            }
            SingleValueRange::Int16($start, $end) => {
                type $DT = i16;
                $then
            }
            SingleValueRange::Int32($start, $end) => {
                type $DT = i32;
                $then
            }
            SingleValueRange::Int64($start, $end) => {
                type $DT = i64;
                $then
            }
            SingleValueRange::Float32($start, $end) => {
                type $DT = f32;
                $then
            }
            SingleValueRange::Float64($start, $end) => {
                type $DT = f64;
                $then
            }
        }
    };
    ($expr:expr, $DT:ident : Integral, $start:pat, $end:pat, $then:expr, $else:expr) => {{
        use $crate::range::SingleValueRange;
        match $expr {
            SingleValueRange::UInt8($start, $end) => {
                type $DT = u8;
                $then
            }
            SingleValueRange::UInt16($start, $end) => {
                type $DT = u16;
                $then
            }
            SingleValueRange::UInt32($start, $end) => {
                type $DT = u32;
                $then
            }
            SingleValueRange::UInt64($start, $end) => {
                type $DT = u64;
                $then
            }
            SingleValueRange::Int8($start, $end) => {
                type $DT = i8;
                $then
            }
            SingleValueRange::Int16($start, $end) => {
                type $DT = i16;
                $then
            }
            SingleValueRange::Int32($start, $end) => {
                type $DT = i32;
                $then
            }
            SingleValueRange::Int64($start, $end) => {
                type $DT = i64;
                $then
            }
            SingleValueRange::Float32(_, _) => {
                type $DT = f32;
                $else
            }
            SingleValueRange::Float64(_, _) => {
                type $DT = f64;
                $else
            }
        }
    }};
}

#[macro_export]
macro_rules! single_value_range_cmp {
    ($lexpr:expr, $rexpr:expr, $DT:ident, $lstart:pat, $lend:pat, $rstart:pat, $rend:pat, $then:expr, $else:expr) => {{
        use $crate::range::SingleValueRange;
        match ($lexpr, $rexpr) {
            (
                SingleValueRange::UInt8($lstart, $lend),
                SingleValueRange::UInt8($rstart, $rend),
            ) => {
                type $DT = u8;
                $then
            }
            (
                SingleValueRange::UInt16($lstart, $lend),
                SingleValueRange::UInt16($rstart, $rend),
            ) => {
                type $DT = u16;
                $then
            }
            (
                SingleValueRange::UInt32($lstart, $lend),
                SingleValueRange::UInt32($rstart, $rend),
            ) => {
                type $DT = u32;
                $then
            }
            (
                SingleValueRange::UInt64($lstart, $lend),
                SingleValueRange::UInt64($rstart, $rend),
            ) => {
                type $DT = u64;
                $then
            }
            (
                SingleValueRange::Int8($lstart, $lend),
                SingleValueRange::Int8($rstart, $rend),
            ) => {
                type $DT = i8;
                $then
            }
            (
                SingleValueRange::Int16($lstart, $lend),
                SingleValueRange::Int16($rstart, $rend),
            ) => {
                type $DT = i16;
                $then
            }
            (
                SingleValueRange::Int32($lstart, $lend),
                SingleValueRange::Int32($rstart, $rend),
            ) => {
                type $DT = i32;
                $then
            }
            (
                SingleValueRange::Int64($lstart, $lend),
                SingleValueRange::Int64($rstart, $rend),
            ) => {
                type $DT = i64;
                $then
            }
            (
                SingleValueRange::Float32($lstart, $lend),
                SingleValueRange::Float32($rstart, $rend),
            ) => {
                type $DT = f32;
                $then
            }
            (
                SingleValueRange::Float64($lstart, $lend),
                SingleValueRange::Float64($rstart, $rend),
            ) => {
                type $DT = f64;
                $then
            }
            _ => $else,
        }
    }};
    ($expr1:expr, $expr2:expr, $expr3:expr, $DT:ident, $start1:pat, $end1:pat, $start2:pat, $end2:pat, $start3:pat, $end3:pat, $cmp:expr, $else:expr) => {{
        use $crate::range::SingleValueRange::*;
        match ($expr1, $expr2, $expr3) {
            (
                UInt8($start1, $end1),
                UInt8($start2, $end2),
                UInt8($start3, $end3),
            ) => {
                type $DT = u8;
                $cmp
            }
            (
                UInt16($start1, $end1),
                UInt16($start2, $end2),
                UInt16($start3, $end3),
            ) => {
                type $DT = u16;
                $cmp
            }
            (
                UInt32($start1, $end1),
                UInt32($start2, $end2),
                UInt32($start3, $end3),
            ) => {
                type $DT = u32;
                $cmp
            }
            (
                UInt64($start1, $end1),
                UInt64($start2, $end2),
                UInt64($start3, $end3),
            ) => {
                type $DT = u64;
                $cmp
            }
            (
                Int8($start1, $end1),
                Int8($start2, $end2),
                Int8($start3, $end3),
            ) => {
                type $DT = i8;
                $cmp
            }
            (
                Int16($start1, $end1),
                Int16($start2, $end2),
                Int16($start3, $end3),
            ) => {
                type $DT = i16;
                $cmp
            }
            (
                Int32($start1, $end1),
                Int32($start2, $end2),
                Int32($start3, $end3),
            ) => {
                type $DT = i32;
                $cmp
            }
            (
                Int64($start1, $end1),
                Int64($start2, $end2),
                Int64($start3, $end3),
            ) => {
                type $DT = i64;
                $cmp
            }
            (
                Float32($start1, $end1),
                Float32($start2, $end2),
                Float32($start3, $end3),
            ) => {
                type $DT = f32;
                $cmp
            }
            (
                Float64($start1, $end1),
                Float64($start2, $end2),
                Float64($start3, $end3),
            ) => {
                type $DT = f64;
                $cmp
            }
            _ => $else,
        }
    }};
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum MultiValueRange {
    UInt8(Box<[u8]>, Box<[u8]>),
    UInt16(Box<[u16]>, Box<[u16]>),
    UInt32(Box<[u32]>, Box<[u32]>),
    UInt64(Box<[u64]>, Box<[u64]>),
    Int8(Box<[i8]>, Box<[i8]>),
    Int16(Box<[i16]>, Box<[i16]>),
    Int32(Box<[i32]>, Box<[i32]>),
    Int64(Box<[i64]>, Box<[i64]>),
    Float32(Box<[f32]>, Box<[f32]>),
    Float64(Box<[f64]>, Box<[f64]>),
}

impl MultiValueRange {
    pub fn check_datatype(
        &self,
        datatype: Datatype,
    ) -> Result<(), DatatypeError> {
        check_datatype!(self, datatype);
        Ok(())
    }

    /// Returns the number of values held by each end of this range.
    pub fn num_values(&self) -> usize {
        crate::multi_value_range_go!(self, _DT, start, _, start.len())
    }

    /// Returns the number of cells spanned by this range if it is a
    /// range over a discrete domain.
    ///
    /// If the lower and upper bounds differ only in the last value,
    /// then the result is the discrete difference between the last values.
    /// ```
    /// use tiledb_common::array::CellValNum;
    /// use tiledb_common::range::MultiValueRange;
    ///
    /// let cvn = CellValNum::try_from(2).unwrap();
    /// assert_eq!(Some(100),
    ///            MultiValueRange::Int64(vec![1, 1].into_boxed_slice(),
    ///                                   vec![1, 100].into_boxed_slice()).num_cells());
    /// assert_eq!(None,
    ///            MultiValueRange::Float64(vec![1.0, 1.0].into_boxed_slice(),
    ///                                     vec![1.0, 100.0].into_boxed_slice()).num_cells());
    /// ```
    ///
    /// If there is a difference in a prior value in the range,
    /// then all possible values of the trailing values represent unique
    /// cells in the range.
    /// ```
    /// use tiledb_common::range::MultiValueRange;
    /// let num_i32s = ((i32::MAX as i128 - i32::MIN as i128) + 1) as u128;
    /// let num_i64s = ((i64::MAX as i128 - i64::MIN as i128) + 1) as u128;
    /// assert_eq!(Some(num_i32s + 1),
    ///            MultiValueRange::Int32(vec![0, 0].into_boxed_slice(),
    ///                                   vec![1, 0].into_boxed_slice()).num_cells());
    /// assert_eq!(Some(num_i32s + 9 + 1),
    ///            MultiValueRange::Int32(vec![0, 0].into_boxed_slice(),
    ///                                   vec![1, 9].into_boxed_slice()).num_cells());
    /// assert_eq!(Some(num_i64s + 1),
    ///            MultiValueRange::Int64(vec![0, 0].into_boxed_slice(),
    ///                                   vec![1, 0].into_boxed_slice()).num_cells());
    /// assert_eq!(Some(num_i64s + 9 + 1),
    ///            MultiValueRange::Int64(vec![0, 0].into_boxed_slice(),
    ///                                   vec![1, 9].into_boxed_slice()).num_cells());
    /// ```
    /// This will also return `None` if the result would overflow an `i128` value.
    pub fn num_cells(&self) -> Option<u128> {
        crate::multi_value_range_go!(
            self,
            DT,
            start,
            end,
            {
                let iter_factor = i128::from(DT::MAX) - i128::from(DT::MIN) + 1;
                start
                    .iter()
                    .zip(end.iter())
                    .skip_while(|(lb, ub)| lb == ub)
                    .try_fold(0i128, |num_cells, (lower, upper)| {
                        if upper < lower && num_cells == 0 {
                            // this is the first unequal value, upper must be greater
                            unreachable!(
                                "Invalid `MultiValueRange`: {:?}",
                                self
                            )
                        }

                        let num_cells = num_cells.checked_mul(iter_factor)?;
                        let delta = i128::from(*upper) - i128::from(*lower);
                        Some(num_cells + delta)
                    })
                    .map(|n| n as u128 + 1)
            },
            None
        )
    }

    /// Returns a `CellValNum` which matches the values in this range.
    pub fn cell_val_num(&self) -> CellValNum {
        CellValNum::Fixed(NonZeroU32::new(self.num_values() as u32).unwrap())
    }

    /// Returns the range covered by the union of `self` and `other`.
    ///
    /// # Panics
    ///
    /// Panics if `self` and `other` do not have the same physical datatype or the same fixed
    /// length.
    pub fn union(&self, other: &Self) -> Self {
        assert_eq!(
            self.num_values(),
            other.num_values(),
            "`MultiValueRange::union` on ranges of non-matching length: `self` = {:?}, `other` = {:?}",
            self,
            other
        );

        crate::multi_value_range_cmp!(
            self,
            other,
            _DT,
            lstart,
            lend,
            rstart,
            rend,
            {
                let min = if lstart.bits_lt(rstart) {
                    lstart.clone()
                } else {
                    rstart.clone()
                };

                let max = if lend.bits_gt(rend) {
                    lend.clone()
                } else {
                    rend.clone()
                };

                MultiValueRange::try_from((self.cell_val_num(), min, max))
                    .unwrap()
            },
            panic!(
                "`MultiValueRange::union` on non-matching datatypes: `self` = {:?}, `other` = {:?}",
                self, other
            )
        )
    }

    /// Returns the range covered by the intersection of `self` and `other`,
    /// or `None` if `self` and `other` do not overlap.
    ///
    /// # Panics
    ///
    /// Panics if `self` and `other` do not have the same physical datatype or the same fixed
    /// length.
    pub fn intersection(&self, other: &Self) -> Option<Self> {
        assert_eq!(
            self.num_values(),
            other.num_values(),
            "`MultiValueRange::union` on ranges of non-matching length: `self` = {:?}, `other` = {:?}",
            self,
            other
        );

        crate::multi_value_range_cmp!(
            self,
            other,
            DT,
            lstart,
            lend,
            rstart,
            rend,
            {
                let (lower, upper) = intersection::<[DT]>(
                    &**lstart, &**lend, &**rstart, &**rend,
                )?;
                Some(
                    MultiValueRange::try_from((
                        self.cell_val_num(),
                        lower.to_vec().into_boxed_slice(),
                        upper.to_vec().into_boxed_slice(),
                    ))
                    .unwrap(),
                )
            },
            panic!(
                "`MultiValueRange::union` on non-matching datatypes: `self` = {:?}, `other` = {:?}",
                self, other
            )
        )
    }
}

impl PartialEq for MultiValueRange {
    fn eq(&self, other: &Self) -> bool {
        crate::multi_value_range_cmp!(
            self,
            other,
            _DT,
            lstart,
            lend,
            rstart,
            rend,
            lstart.bits_eq(rstart) && lend.bits_eq(rend),
            false
        )
    }
}

/// The [PartialEq] implementation of [MultiValueRange] compares the
/// floating-point variants using [BitsEq],
/// and as such is an equivalence relation.
impl Eq for MultiValueRange {}

/// Uses the [BitsHash] implementation of the wrapped values.
impl Hash for MultiValueRange {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        crate::multi_value_range_go!(self, _DT, start, end, {
            start.bits_hash(state);
            end.bits_hash(state);
        })
    }
}

macro_rules! multi_value_range_try_from {
    ($($V:ident : $U:ty),+) => {
        $(
            impl TryFrom<(CellValNum, Box<[$U]>, Box<[$U]>)> for MultiValueRange {
                type Error = MultiValueRangeError;
                fn try_from(value: (CellValNum, Box<[$U]>, Box<[$U]>)) ->
                        Result<Self, Self::Error> {
                    let cell_val_num = match value.0 {
                        CellValNum::Fixed(cvn) if u32::from(cvn) == 1u32 => {
                            return Err(MultiValueRangeError::CellValNumSingle)
                        }
                        CellValNum::Fixed(cvn) => cvn,
                        CellValNum::Var => {
                            return Err(MultiValueRangeError::CellValNumVar)
                        }
                    };
                    if value.1.len() as u32 != cell_val_num.get() {
                        return Err(MultiValueRangeError::InvalidStartRange(cell_val_num, value.1.len()))
                    }
                    if value.2.len() as u32 != cell_val_num.get() {
                        return Err(MultiValueRangeError::InvalidEndRange(cell_val_num, value.2.len()))
                    }
                    Ok(MultiValueRange::$V(value.1, value.2))
                }
            }

            impl TryFrom<(CellValNum, Vec<$U>, Vec<$U>)> for MultiValueRange {
                type Error = <Self as TryFrom<(CellValNum, Box<[$U]>, Box<[$U]>)>>::Error;
                fn try_from(value: (CellValNum, Vec<$U>, Vec<$U>)) -> Result<Self, Self::Error> {
                    let (cell_val_num, lb, ub) = value;
                    Self::try_from((cell_val_num, lb.into_boxed_slice(), ub.into_boxed_slice()))
                }
            }
        )+
    }
}

multi_value_range_try_from!(UInt8: u8, UInt16: u16, UInt32: u32, UInt64: u64);
multi_value_range_try_from!(Int8: i8, Int16: i16, Int32: i32, Int64: i64);
multi_value_range_try_from!(Float32: f32, Float64: f64);

#[macro_export]
macro_rules! multi_value_range_go {
    ($expr:expr, $DT:ident, $start:pat, $end:pat, $then:expr) => {{ $crate::multi_value_range_go!($expr, $DT, $start, $end, $then, $then) }};
    ($expr:expr, $DT:ident, $start:pat, $end:pat, $if_integral:expr, $if_float:expr) => {
        match $expr {
            #[allow(unused_variables)]
            MultiValueRange::UInt8($start, $end) => {
                #[allow(dead_code)]
                type $DT = u8;
                $if_integral
            }
            #[allow(unused_variables)]
            MultiValueRange::UInt16($start, $end) => {
                #[allow(dead_code)]
                type $DT = u16;
                $if_integral
            }
            #[allow(unused_variables)]
            MultiValueRange::UInt32($start, $end) => {
                #[allow(dead_code)]
                type $DT = u32;
                $if_integral
            }
            #[allow(unused_variables)]
            MultiValueRange::UInt64($start, $end) => {
                #[allow(dead_code)]
                type $DT = u64;
                $if_integral
            }
            #[allow(unused_variables)]
            MultiValueRange::Int8($start, $end) => {
                #[allow(dead_code)]
                type $DT = i8;
                $if_integral
            }
            #[allow(unused_variables)]
            MultiValueRange::Int16($start, $end) => {
                #[allow(dead_code)]
                type $DT = i16;
                $if_integral
            }
            #[allow(unused_variables)]
            MultiValueRange::Int32($start, $end) => {
                #[allow(dead_code)]
                type $DT = i32;
                $if_integral
            }
            #[allow(unused_variables)]
            MultiValueRange::Int64($start, $end) => {
                #[allow(dead_code)]
                type $DT = i64;
                $if_integral
            }
            #[allow(unused_variables)]
            MultiValueRange::Float32($start, $end) => {
                #[allow(dead_code)]
                type $DT = f32;
                $if_float
            }
            #[allow(unused_variables)]
            MultiValueRange::Float64($start, $end) => {
                #[allow(dead_code)]
                type $DT = f64;
                $if_float
            }
        }
    };
}

#[macro_export]
macro_rules! multi_value_range_cmp {
    ($lexpr:expr, $rexpr:expr, $DT:ident, $lstart:pat, $lend:pat, $rstart:pat, $rend:pat, $cmp:expr, $else:expr) => {{
        use $crate::range::MultiValueRange::*;
        match ($lexpr, $rexpr) {
            (UInt8($lstart, $lend), UInt8($rstart, $rend)) => {
                type $DT = u8;
                $cmp
            }
            (UInt16($lstart, $lend), UInt16($rstart, $rend)) => {
                type $DT = u16;
                $cmp
            }
            (UInt32($lstart, $lend), UInt32($rstart, $rend)) => {
                type $DT = u32;
                $cmp
            }
            (UInt64($lstart, $lend), UInt64($rstart, $rend)) => {
                type $DT = u64;
                $cmp
            }
            (Int8($lstart, $lend), Int8($rstart, $rend)) => {
                type $DT = i8;
                $cmp
            }
            (Int16($lstart, $lend), Int16($rstart, $rend)) => {
                type $DT = i16;
                $cmp
            }
            (Int32($lstart, $lend), Int32($rstart, $rend)) => {
                type $DT = i32;
                $cmp
            }
            (Int64($lstart, $lend), Int64($rstart, $rend)) => {
                type $DT = i64;
                $cmp
            }
            (Float32($lstart, $lend), Float32($rstart, $rend)) => {
                type $DT = f32;
                $cmp
            }
            (Float64($lstart, $lend), Float64($rstart, $rend)) => {
                type $DT = f64;
                $cmp
            }
            _ => $else,
        }
    }};
    ($expr1:expr, $expr2:expr, $expr3:expr, $DT:ident, $start1:pat, $end1:pat, $start2:pat, $end2:pat, $start3:pat, $end3:pat, $cmp:expr, $else:expr) => {{
        use $crate::range::MultiValueRange::*;
        match ($expr1, $expr2, $expr3) {
            (
                UInt8($start1, $end1),
                UInt8($start2, $end2),
                UInt8($start3, $end3),
            ) => {
                type $DT = u8;
                $cmp
            }
            (
                UInt16($start1, $end1),
                UInt16($start2, $end2),
                UInt16($start3, $end3),
            ) => {
                type $DT = u16;
                $cmp
            }
            (
                UInt32($start1, $end1),
                UInt32($start2, $end2),
                UInt32($start3, $end3),
            ) => {
                type $DT = u32;
                $cmp
            }
            (
                UInt64($start1, $end1),
                UInt64($start2, $end2),
                UInt64($start3, $end3),
            ) => {
                type $DT = u64;
                $cmp
            }
            (
                Int8($start1, $end1),
                Int8($start2, $end2),
                Int8($start3, $end3),
            ) => {
                type $DT = i8;
                $cmp
            }
            (
                Int16($start1, $end1),
                Int16($start2, $end2),
                Int16($start3, $end3),
            ) => {
                type $DT = i16;
                $cmp
            }
            (
                Int32($start1, $end1),
                Int32($start2, $end2),
                Int32($start3, $end3),
            ) => {
                type $DT = i32;
                $cmp
            }
            (
                Int64($start1, $end1),
                Int64($start2, $end2),
                Int64($start3, $end3),
            ) => {
                type $DT = i64;
                $cmp
            }
            (
                Float32($start1, $end1),
                Float32($start2, $end2),
                Float32($start3, $end3),
            ) => {
                type $DT = f32;
                $cmp
            }
            (
                Float64($start1, $end1),
                Float64($start2, $end2),
                Float64($start3, $end3),
            ) => {
                type $DT = f64;
                $cmp
            }
            _ => $else,
        }
    }};
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum VarValueRange {
    UInt8(Box<[u8]>, Box<[u8]>),
    UInt16(Box<[u16]>, Box<[u16]>),
    UInt32(Box<[u32]>, Box<[u32]>),
    UInt64(Box<[u64]>, Box<[u64]>),
    Int8(Box<[i8]>, Box<[i8]>),
    Int16(Box<[i16]>, Box<[i16]>),
    Int32(Box<[i32]>, Box<[i32]>),
    Int64(Box<[i64]>, Box<[i64]>),
    Float32(Box<[f32]>, Box<[f32]>),
    Float64(Box<[f64]>, Box<[f64]>),
}

impl VarValueRange {
    /// Returns a `CellValNum` which matches the values in this range, i.e. `CellValNum::Var`.
    pub fn cell_val_num(&self) -> CellValNum {
        CellValNum::Var
    }

    pub fn check_datatype(
        &self,
        datatype: Datatype,
    ) -> Result<(), DatatypeError> {
        check_datatype!(self, datatype);
        Ok(())
    }

    /// Returns the range covered by the union of `self` and `other`.
    ///
    /// # Panics
    ///
    /// Panics if `self` and `other` do not have the same physical datatype.
    pub fn union(&self, other: &Self) -> Self {
        crate::var_value_range_cmp!(
            self,
            other,
            _DT,
            lstart,
            lend,
            rstart,
            rend,
            {
                let min = if lstart.bits_lt(rstart) {
                    lstart.clone()
                } else {
                    rstart.clone()
                };

                let max = if lend.bits_gt(rend) {
                    lend.clone()
                } else {
                    rend.clone()
                };

                VarValueRange::from((min, max))
            },
            panic!(
                "`VarValueRange::union` on non-matching datatypes: `self` = {:?}, `other` = {:?}",
                self, other
            )
        )
    }

    /// Returns the range covered by the intersection of `self` and `other`,
    /// or `None` if `self` and `other` do not overlap.
    ///
    /// # Panics
    ///
    /// Panics if `self` and `other` do not have the same physical datatype.
    pub fn intersection(&self, other: &Self) -> Option<Self> {
        crate::var_value_range_cmp!(
            self,
            other,
            DT,
            lstart,
            lend,
            rstart,
            rend,
            {
                let (lower, upper) = intersection::<[DT]>(
                    &**lstart, &**lend, &**rstart, &**rend,
                )?;
                Some(VarValueRange::from((
                    lower.to_vec().into_boxed_slice(),
                    upper.to_vec().into_boxed_slice(),
                )))
            },
            panic!(
                "`VarValueRange::union` on non-matching datatypes: `self` = {:?}, `other` = {:?}",
                self, other
            )
        )
    }
}

impl PartialEq for VarValueRange {
    fn eq(&self, other: &Self) -> bool {
        crate::var_value_range_cmp!(
            self,
            other,
            _DT,
            lstart,
            lend,
            rstart,
            rend,
            lstart.bits_eq(rstart) && lend.bits_eq(rend),
            false
        )
    }
}

/// The [PartialEq] implementation of [VarValueRange] compares the
/// floating-point variants using [BitsEq],
/// and as such is an equivalence relation.
impl Eq for VarValueRange {}

/// Uses the [BitsHash] implementation of the wrapped values.
impl Hash for VarValueRange {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        crate::var_value_range_go!(self, _DT, start, end, {
            start.bits_hash(state);
            end.bits_hash(state);
        })
    }
}

macro_rules! var_value_range_from {
    ($($V:ident : $U:ty),+) => {
        $(
            impl From<(Box<[$U]>, Box<[$U]>)> for VarValueRange {
                fn from(value: (Box<[$U]>, Box<[$U]>)) -> VarValueRange {
                    VarValueRange::$V(value.0, value.1)
                }
            }
        )+
    }
}

var_value_range_from!(UInt8: u8, UInt16: u16, UInt32: u32, UInt64: u64);
var_value_range_from!(Int8: i8, Int16: i16, Int32: i32, Int64: i64);
var_value_range_from!(Float32: f32, Float64: f64);

impl From<(&str, &str)> for VarValueRange {
    fn from(value: (&str, &str)) -> VarValueRange {
        let start = value.0.as_bytes().to_vec().into_boxed_slice();
        let end = value.1.as_bytes().to_vec().into_boxed_slice();
        VarValueRange::UInt8(start, end)
    }
}

impl From<&[&str; 2]> for VarValueRange {
    fn from(value: &[&str; 2]) -> VarValueRange {
        let start = value[0].as_bytes().to_vec().into_boxed_slice();
        let end = value[1].as_bytes().to_vec().into_boxed_slice();
        VarValueRange::UInt8(start, end)
    }
}

impl From<(String, String)> for VarValueRange {
    fn from(value: (String, String)) -> VarValueRange {
        let start = value.0.into_bytes().into_boxed_slice();
        let end = value.1.into_bytes().into_boxed_slice();
        VarValueRange::UInt8(start, end)
    }
}

impl From<[String; 2]> for VarValueRange {
    fn from(value: [String; 2]) -> VarValueRange {
        let [start, end] = value;
        let start = start.into_bytes().into_boxed_slice();
        let end = end.into_bytes().into_boxed_slice();
        VarValueRange::UInt8(start, end)
    }
}

#[macro_export]
macro_rules! var_value_range_go {
    ($expr:expr, $DT:ident, $start:pat, $end:pat, $then:expr) => {
        match $expr {
            VarValueRange::UInt8($start, $end) => {
                type $DT = u8;
                $then
            }
            VarValueRange::UInt16($start, $end) => {
                type $DT = u16;
                $then
            }
            VarValueRange::UInt32($start, $end) => {
                type $DT = u32;
                $then
            }
            VarValueRange::UInt64($start, $end) => {
                type $DT = u64;
                $then
            }
            VarValueRange::Int8($start, $end) => {
                type $DT = i8;
                $then
            }
            VarValueRange::Int16($start, $end) => {
                type $DT = i16;
                $then
            }
            VarValueRange::Int32($start, $end) => {
                type $DT = i32;
                $then
            }
            VarValueRange::Int64($start, $end) => {
                type $DT = i64;
                $then
            }
            VarValueRange::Float32($start, $end) => {
                type $DT = f32;
                $then
            }
            VarValueRange::Float64($start, $end) => {
                type $DT = f64;
                $then
            }
        }
    };
}

#[macro_export]
macro_rules! var_value_range_cmp {
    ($lexpr:expr, $rexpr:expr, $DT:ident, $lstart:pat, $lend:pat, $rstart:pat, $rend:pat, $cmp:expr, $else:expr) => {{
        use $crate::range::VarValueRange::*;
        match ($lexpr, $rexpr) {
            (UInt8($lstart, $lend), UInt8($rstart, $rend)) => {
                type $DT = u8;
                $cmp
            }
            (UInt16($lstart, $lend), UInt16($rstart, $rend)) => {
                type $DT = u16;
                $cmp
            }
            (UInt32($lstart, $lend), UInt32($rstart, $rend)) => {
                type $DT = u32;
                $cmp
            }
            (UInt64($lstart, $lend), UInt64($rstart, $rend)) => {
                type $DT = u64;
                $cmp
            }
            (Int8($lstart, $lend), Int8($rstart, $rend)) => {
                type $DT = i8;
                $cmp
            }
            (Int16($lstart, $lend), Int16($rstart, $rend)) => {
                type $DT = i16;
                $cmp
            }
            (Int32($lstart, $lend), Int32($rstart, $rend)) => {
                type $DT = i32;
                $cmp
            }
            (Int64($lstart, $lend), Int64($rstart, $rend)) => {
                type $DT = i64;
                $cmp
            }
            (Float32($lstart, $lend), Float32($rstart, $rend)) => {
                type $DT = f32;
                $cmp
            }
            (Float64($lstart, $lend), Float64($rstart, $rend)) => {
                type $DT = f64;
                $cmp
            }
            _ => $else,
        }
    }};
    ($expr1:expr, $expr2:expr, $expr3:expr, $DT:ident, $start1:pat, $end1:pat, $start2:pat, $end2:pat, $start3:pat, $end3:pat, $cmp:expr, $else:expr) => {{
        use $crate::range::VarValueRange::*;
        match ($expr1, $expr2, $expr3) {
            (
                UInt8($start1, $end1),
                UInt8($start2, $end2),
                UInt8($start3, $end3),
            ) => {
                type $DT = u8;
                $cmp
            }
            (
                UInt16($start1, $end1),
                UInt16($start2, $end2),
                UInt16($start3, $end3),
            ) => {
                type $DT = u16;
                $cmp
            }
            (
                UInt32($start1, $end1),
                UInt32($start2, $end2),
                UInt32($start3, $end3),
            ) => {
                type $DT = u32;
                $cmp
            }
            (
                UInt64($start1, $end1),
                UInt64($start2, $end2),
                UInt64($start3, $end3),
            ) => {
                type $DT = u64;
                $cmp
            }
            (
                Int8($start1, $end1),
                Int8($start2, $end2),
                Int8($start3, $end3),
            ) => {
                type $DT = i8;
                $cmp
            }
            (
                Int16($start1, $end1),
                Int16($start2, $end2),
                Int16($start3, $end3),
            ) => {
                type $DT = i16;
                $cmp
            }
            (
                Int32($start1, $end1),
                Int32($start2, $end2),
                Int32($start3, $end3),
            ) => {
                type $DT = i32;
                $cmp
            }
            (
                Int64($start1, $end1),
                Int64($start2, $end2),
                Int64($start3, $end3),
            ) => {
                type $DT = i64;
                $cmp
            }
            (
                Float32($start1, $end1),
                Float32($start2, $end2),
                Float32($start3, $end3),
            ) => {
                type $DT = f32;
                $cmp
            }
            (
                Float64($start1, $end1),
                Float64($start2, $end2),
                Float64($start3, $end3),
            ) => {
                type $DT = f64;
                $cmp
            }
            _ => $else,
        }
    }};
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum Range {
    Single(SingleValueRange),
    Multi(MultiValueRange),
    Var(VarValueRange),
}

impl Range {
    pub fn cell_val_num(&self) -> CellValNum {
        match self {
            Self::Single(r) => r.cell_val_num(),
            Self::Multi(r) => r.cell_val_num(),
            Self::Var(r) => r.cell_val_num(),
        }
    }

    /// Returns the number of cells spanned by this range if it is a discrete range.
    /// See `SingleValueRange::num_cells()` and `MultiValueRange::num_cells()`.
    /// `Range::Var` variants are not discrete ranges and will return `None`.
    pub fn num_cells(&self) -> Option<u128> {
        match self {
            Self::Single(r) => r.num_cells(),
            Self::Multi(r) => r.num_cells(),
            Self::Var(_) => None,
        }
    }

    // N.B. This is not `check_field_compatibility` because dimensions have
    // restrictions on their cell_val_num that don't apply to attributes.
    //
    // See: tiledb::sm::Dimension::set_cell_val_num
    pub fn check_dimension_compatibility(
        &self,
        datatype: Datatype,
        cell_val_num: CellValNum,
    ) -> Result<(), DimensionCompatibilityError> {
        match self {
            Self::Single(svr) => svr.check_datatype(datatype)?,
            Self::Multi(mvr) => {
                return Err(DimensionCompatibilityError::MultiValueRange(
                    mvr.num_values(),
                ));
            }
            Self::Var(vvr) => vvr.check_datatype(datatype)?,
        }

        match cell_val_num {
            CellValNum::Fixed(cvn) => {
                if cvn.get() > 1 {
                    return Err(DimensionCompatibilityError::CellValNumFixed(
                        cvn,
                    ));
                }
                if datatype == Datatype::StringAscii {
                    return Err(
                        DimensionCompatibilityError::FixedStringAsciiDimension(
                            cvn,
                        ),
                    );
                }
                if !matches!(self, Self::Single(_)) {
                    return Err(DimensionCompatibilityError::VarRangeForNonStringDimension(datatype));
                }
            }
            CellValNum::Var => {
                if datatype != Datatype::StringAscii {
                    return Err(DimensionCompatibilityError::CellValNumVar(
                        datatype,
                    ));
                }
                match self {
                    Range::Single(SingleValueRange::UInt8(_, _)) =>
                        Err(DimensionCompatibilityError::FixedRangeForStringDimension),
                    Range::Multi(MultiValueRange::UInt8(_, _)) =>
                        Err(DimensionCompatibilityError::FixedRangeForStringDimension),
                    Range::Var(VarValueRange::UInt8(_, _)) => Ok(()),
                    Range::Single(s) => single_value_range_go!(s, DT, _, _,
                        Err(DimensionCompatibilityError::Datatype(
                                DatatypeError::physical_type_incompatible::<DT>(datatype)))),
                    Range::Multi(m) => {
                        // NB: this is actually unreachable but this is what it would be if it were
                        multi_value_range_go!(m, DT, _, _,
                            Err(DimensionCompatibilityError::Datatype(
                                    DatatypeError::physical_type_incompatible::<DT>(datatype))))
                    },
                    Range::Var(v) => var_value_range_go!(v, DT, _, _,
                        Err(
                            DimensionCompatibilityError::Datatype(
                                DatatypeError::physical_type_incompatible::<DT>(
                                    datatype,
                                ),
                            ),
                        )
                    ),
                }?
            }
        }

        Ok(())
    }

    /// Returns the range covered by the union of `self` and `other`.
    ///
    /// # Panics
    ///
    /// Panics if `self` and `other` are not the same variant, or if
    /// `self` and `other` do not have the same physical datatype.
    pub fn union(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::Single(l), Self::Single(r)) => Self::Single(l.union(r)),
            (Self::Multi(l), Self::Multi(r)) => Self::Multi(l.union(r)),
            (Self::Var(l), Self::Var(r)) => Self::Var(l.union(r)),
            _ => panic!(
                "`Range::union` on non-matching range variants: `self` = {:?}, `other` = {:?}",
                self, other
            ),
        }
    }

    /// Returns the range covered by the intersection of `self` and `other`,
    /// or `None` if `self` and `other` do not overlap.
    ///
    /// # Panics
    ///
    /// Panics if `self.cell_val_num() != other.cell_val_num()` or if
    /// `self` and `other` do not have the same physical datatype.
    pub fn intersection(&self, other: &Self) -> Option<Self> {
        match (self, other) {
            (Self::Single(l), Self::Single(r)) => {
                Some(Self::Single(l.intersection(r)?))
            }
            (Self::Multi(l), Self::Multi(r)) => {
                Some(Self::Multi(l.intersection(r)?))
            }
            (Self::Var(l), Self::Var(r)) => Some(Self::Var(l.intersection(r)?)),
            _ => panic!(
                "`Range::intersection` on non-matching range variants: `self` = {:?}, `other` = {:?}",
                self, other
            ),
        }
    }
}

macro_rules! range_from_impl {
    ($($V:ident : $U:ty),+) => {
        $(
            impl From<&[$U; 2]> for Range {
                fn from(value: &[$U; 2]) -> Range {
                    Range::Single(SingleValueRange::from(value))
                }
            }

            impl TryFrom<(CellValNum, Box<[$U]>, Box<[$U]>)> for Range {
                type Error = <MultiValueRange as TryFrom<(CellValNum, Box<[$U]>, Box<[$U]>)>>::Error;
                fn try_from(value: (CellValNum, Box<[$U]>, Box<[$U]>)) -> Result<Self, Self::Error> {
                    Ok(Range::Multi(MultiValueRange::try_from(value)?))
                }
            }

            impl From<(Box<[$U]>, Box<[$U]>)> for Range {
                fn from(value: (Box<[$U]>, Box<[$U]>)) -> Range {
                    Range::Var(VarValueRange::from(value))
                }
            }
        )+
    }
}

range_from_impl!(UInt8: u8, UInt16: u16, UInt32: u32, UInt64: u64);
range_from_impl!(Int8: i8, Int16: i16, Int32: i32, Int64: i64);
range_from_impl!(Float32: f32, Float64: f64);

impl From<(&str, &str)> for Range {
    fn from(value: (&str, &str)) -> Range {
        Range::Var(VarValueRange::from(value))
    }
}

impl From<&[&str; 2]> for Range {
    fn from(value: &[&str; 2]) -> Range {
        Range::Var(VarValueRange::from(value))
    }
}

impl From<(String, String)> for Range {
    fn from(value: (String, String)) -> Range {
        Range::Var(VarValueRange::from(value))
    }
}

impl From<[String; 2]> for Range {
    fn from(value: [String; 2]) -> Range {
        Range::Var(VarValueRange::from(value))
    }
}

impl From<SingleValueRange> for Range {
    fn from(value: SingleValueRange) -> Self {
        Range::Single(value)
    }
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct TypedRange {
    pub datatype: Datatype,
    pub range: Range,
}

impl TypedRange {
    pub fn new(datatype: Datatype, range: Range) -> Self {
        Self { datatype, range }
    }

    pub fn cell_val_num(&self) -> CellValNum {
        self.range.cell_val_num()
    }

    pub fn from_slices(
        datatype: Datatype,
        cell_val_num: CellValNum,
        start: &[u8],
        end: &[u8],
    ) -> Result<Self, RangeFromSlicesError> {
        match cell_val_num {
            CellValNum::Var => {
                if start.len() % datatype.size() != 0 {
                    return Err(RangeFromSlicesError::StartTruncation(
                        datatype,
                        start.len(),
                    ));
                }
                if end.len() % datatype.size() != 0 {
                    return Err(RangeFromSlicesError::EndTruncation(
                        datatype,
                        start.len(),
                    ));
                }
            }
            CellValNum::Fixed(cvn) => {
                if start.len() % datatype.size() != 0 {
                    return Err(RangeFromSlicesError::StartTruncation(
                        datatype,
                        start.len(),
                    ));
                } else if end.len() % datatype.size() != 0 {
                    return Err(RangeFromSlicesError::EndTruncation(
                        datatype,
                        start.len(),
                    ));
                }

                let num_elements_start = start.len() / datatype.size();
                if num_elements_start != cvn.get() as usize {
                    return Err(
                        RangeFromSlicesError::StartMultiValueRangeMismatch(
                            cvn,
                            num_elements_start,
                        ),
                    );
                }

                let num_elements_end = end.len() / datatype.size();
                if num_elements_end != cvn.get() as usize {
                    return Err(
                        RangeFromSlicesError::EndMultiValueRangeMismatch(
                            cvn,
                            num_elements_end,
                        ),
                    );
                }
            }
        }

        physical_type_go!(datatype, DT, {
            let start_slice = unsafe {
                std::slice::from_raw_parts(
                    start.as_ptr() as *const DT,
                    start.len() / datatype.size(),
                )
            };
            let start = start_slice.to_vec().into_boxed_slice();
            let end_slice = unsafe {
                std::slice::from_raw_parts(
                    end.as_ptr() as *const DT,
                    end.len() / datatype.size(),
                )
            };
            let end = end_slice.to_vec().into_boxed_slice();

            match cell_val_num {
                CellValNum::Fixed(cvn) if u32::from(cvn) == 1u32 => {
                    Ok(TypedRange {
                        datatype,
                        range: Range::from(&[start[0], end[0]]),
                    })
                }
                CellValNum::Fixed(_) => Ok(TypedRange {
                    datatype,
                    range: Range::try_from((cell_val_num, start, end))?,
                }),
                CellValNum::Var => Ok(TypedRange {
                    datatype,
                    range: Range::from((start, end)),
                }),
            }
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct NonEmptyDomain(Vec<Range>);

impl NonEmptyDomain {
    /// Returns the non-empty domain covered by the union of `self` and `other`.
    ///
    /// # Panics
    ///
    /// Panics if any of the physical datatypes of the dimensions do not match,
    /// or if `self` and `other` do not have the same number of dimensions.
    pub fn union(&self, other: &Self) -> Self {
        assert_eq!(self.len(), other.len());

        self.iter()
            .zip(other.iter())
            .map(|(l, r)| l.union(r))
            .collect::<Self>()
    }
}

impl Deref for NonEmptyDomain {
    type Target = Vec<Range>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<F> From<F> for NonEmptyDomain
where
    Vec<Range>: From<F>,
{
    fn from(value: F) -> Self {
        NonEmptyDomain(value.into())
    }
}

impl FromIterator<Range> for NonEmptyDomain {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Range>,
    {
        NonEmptyDomain(Vec::<Range>::from_iter(iter))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct TypedNonEmptyDomain(Vec<TypedRange>);

impl TypedNonEmptyDomain {
    pub fn untyped(&self) -> NonEmptyDomain {
        self.iter()
            .map(|typed_range| typed_range.range.clone())
            .collect::<NonEmptyDomain>()
    }
}

impl Deref for TypedNonEmptyDomain {
    type Target = Vec<TypedRange>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<F> From<F> for TypedNonEmptyDomain
where
    Vec<TypedRange>: From<F>,
{
    fn from(value: F) -> Self {
        TypedNonEmptyDomain(value.into())
    }
}

impl FromIterator<TypedRange> for TypedNonEmptyDomain {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = TypedRange>,
    {
        TypedNonEmptyDomain(Vec::<TypedRange>::from_iter(iter))
    }
}

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy {
    use proptest::prelude::*;

    use super::*;
    use crate::physical_type_go;

    impl Arbitrary for SingleValueRange {
        type Parameters = Option<Datatype>;
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
            let strat_type = params
                .map(|dt| Just(dt).boxed())
                .unwrap_or(any::<Datatype>().boxed());
            strat_type
                .prop_flat_map(|dt| {
                    physical_type_go!(dt, DT, {
                        any::<DT>()
                            .prop_flat_map(move |low| {
                                (Just(low), low..=DT::MAX)
                            })
                            .prop_map(move |(low, high)| {
                                SingleValueRange::from(&[low, high])
                            })
                            .boxed()
                    })
                })
                .boxed()
        }
    }

    impl Arbitrary for MultiValueRange {
        type Parameters = (Option<Datatype>, Option<NonZeroU32>);
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
            let strat_type = params
                .0
                .map(|dt| Just(dt).boxed())
                .unwrap_or(any::<Datatype>().boxed());
            let strat_nz = params.1.map(|nz| Just(nz).boxed()).unwrap_or(
                (2..1024u32)
                    .prop_map(|nz| NonZeroU32::new(nz).unwrap())
                    .boxed(),
            );

            (strat_type, strat_nz)
                .prop_flat_map(|(dt, nz)| {
                    physical_type_go!(dt, DT, {
                        (
                            proptest::collection::vec(
                                any::<DT>(),
                                nz.get() as usize,
                            ),
                            proptest::collection::vec(
                                any::<DT>(),
                                nz.get() as usize,
                            ),
                        )
                            .prop_map(move |(left, right)| {
                                let (min, max) = if left.bits_lt(&right) {
                                    (left, right)
                                } else {
                                    (right, left)
                                };
                                MultiValueRange::try_from((
                                    CellValNum::Fixed(nz),
                                    min.into_boxed_slice(),
                                    max.into_boxed_slice(),
                                ))
                                .unwrap()
                            })
                            .boxed()
                    })
                })
                .boxed()
        }
    }

    impl Arbitrary for VarValueRange {
        type Parameters = Option<Datatype>;
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
            let strat_type = params
                .map(|dt| Just(dt).boxed())
                .unwrap_or(any::<Datatype>().boxed());

            const VAR_RANGE_MIN_VALUES: usize = 0;
            const VAR_RANGE_MAX_VALUES: usize = 1024;

            strat_type
                .prop_ind_flat_map(|dt| {
                    physical_type_go!(dt, DT, {
                        (
                            proptest::collection::vec(
                                any::<DT>(),
                                VAR_RANGE_MIN_VALUES..=VAR_RANGE_MAX_VALUES,
                            ),
                            proptest::collection::vec(
                                any::<DT>(),
                                VAR_RANGE_MIN_VALUES..=VAR_RANGE_MAX_VALUES,
                            ),
                        )
                            .prop_map(move |(left, right)| {
                                let (min, max) = if left.bits_lt(&right) {
                                    (left, right)
                                } else {
                                    (right, left)
                                };
                                VarValueRange::from((
                                    min.into_boxed_slice(),
                                    max.into_boxed_slice(),
                                ))
                            })
                            .boxed()
                    })
                })
                .boxed()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::fmt::Debug;

    use paste::paste;
    use proptest::prelude::*;

    use super::*;

    fn test_clone<T>(value: &T)
    where
        T: Clone + Debug + PartialEq,
    {
        let other = value.clone();
        assert_eq!(*value, other);
    }

    #[test]
    fn test_range_inclusive_try_from() {
        macro_rules! do_test_range_inclusive_try_from {
            ($Variant:ident, $DT:ty) => {{
                let zero = 0 as $DT;
                let one = 1 as $DT;
                paste! {
                    let min = [< $DT >]::MIN;
                    let max = [< $DT >]::MAX;
                }
                assert_eq!(
                    zero..=zero,
                    SingleValueRange::$Variant(zero, zero).try_into().unwrap()
                );
                assert_eq!(
                    zero..=one,
                    SingleValueRange::$Variant(zero, one).try_into().unwrap()
                );
                assert_eq!(
                    zero..=max,
                    SingleValueRange::$Variant(zero, max).try_into().unwrap()
                );
                assert_eq!(
                    min..=zero,
                    SingleValueRange::$Variant(min, zero).try_into().unwrap()
                );
                assert_eq!(
                    min..=max,
                    SingleValueRange::$Variant(min, max).try_into().unwrap()
                );
            }};
        }

        do_test_range_inclusive_try_from!(UInt8, u8);
        do_test_range_inclusive_try_from!(UInt16, u16);
        do_test_range_inclusive_try_from!(UInt32, u32);
        do_test_range_inclusive_try_from!(UInt64, u64);
        do_test_range_inclusive_try_from!(Int8, i8);
        do_test_range_inclusive_try_from!(Int16, i16);
        do_test_range_inclusive_try_from!(Int32, i32);
        do_test_range_inclusive_try_from!(Int64, i64);
        do_test_range_inclusive_try_from!(Float32, f32);
        do_test_range_inclusive_try_from!(Float64, f64);
    }

    fn do_inclusive_range_conversion<T>(range_in: RangeInclusive<T>)
    where
        T: Clone + Debug + FromPrimitive + PartialEq,
        SingleValueRange: From<RangeInclusive<T>>,
    {
        let range_out = RangeInclusive::<T>::try_from(SingleValueRange::from(
            range_in.clone(),
        ));
        assert_eq!(Ok(range_in), range_out);
    }

    macro_rules! inclusive_range_conversion {
        ($($U:ty),+) => {
            paste! {
                proptest! {
                    $(
                        #[test]
                        fn [< inclusive_range_conversion_ $U >](r in any::<RangeInclusive<$U>>()) {
                            do_inclusive_range_conversion(r)
                        }
                    )+
                }
            }
        }
    }
    inclusive_range_conversion!(u8, u16, u32, u64, i8, i16, i32, i64, f32, f64);

    fn test_dimension_compatibility(
        range: &Range,
        datatype: Datatype,
    ) -> anyhow::Result<()> {
        match range {
            Range::Single(srange) => {
                if !matches!(datatype, Datatype::StringAscii) {
                    range.check_dimension_compatibility(
                        datatype,
                        1.try_into()?,
                    )?;
                } else {
                    assert!(
                        range
                            .check_dimension_compatibility(
                                datatype,
                                1.try_into()?
                            )
                            .is_err()
                    );
                    srange.check_datatype(datatype)?;
                }
            }
            Range::Multi(mrange) => {
                // MultiValueRange is not valid for dimensions
                assert!(
                    range
                        .check_dimension_compatibility(
                            datatype,
                            CellValNum::Var
                        )
                        .is_err()
                );
                // But we can check that the datatype is correct.
                mrange.check_datatype(datatype)?;
            }
            Range::Var(vrange) => {
                if matches!(datatype, Datatype::StringAscii) {
                    range.check_dimension_compatibility(
                        datatype,
                        CellValNum::Var,
                    )?;
                } else {
                    // Only StringAscii can be var sized
                    assert!(
                        range
                            .check_dimension_compatibility(
                                datatype,
                                CellValNum::Var
                            )
                            .is_err()
                    );

                    // But we can still check the datatype correctness
                    vrange.check_datatype(datatype)?;
                }
            }
        }

        Ok(())
    }

    #[cfg(feature = "serde")]
    fn test_serialization_roundtrip(range: &Range) {
        let data = serde_json::to_string(range).unwrap();
        let other: Range = serde_json::from_str(&data).unwrap();
        assert_eq!(*range, other);
    }

    fn test_from_slices(
        range: &Range,
        datatype: Datatype,
        cvn: CellValNum,
        start: &[u8],
        end: &[u8],
    ) {
        let range2 =
            TypedRange::from_slices(datatype, cvn, start, end).unwrap();

        assert_eq!(datatype, range2.datatype);
        assert_eq!(*range, range2.range);
    }

    proptest! {
        #[test]
        fn single_value_range((datatype, range) in any::<Datatype>().prop_flat_map(|dt|
                (
                    Just(dt),
                    any_with::<SingleValueRange>(Some(dt))
                )
        ))
        {
            do_single_value_range(datatype, range).unwrap()
        }

        #[test]
        fn multi_value_range((datatype, range) in any::<Datatype>().prop_flat_map(|dt|
                (
                    Just(dt),
                    any_with::<MultiValueRange>((Some(dt), None))
                )
        ))
        {
            do_multi_value_range(datatype, range).unwrap()
        }

        #[test]
        fn var_value_range((datatype, range) in any::<Datatype>().prop_flat_map(|dt|
                (
                    Just(dt),
                    any_with::<VarValueRange>(Some(dt))
                )
        ))
        {
            do_var_value_range(datatype, range).unwrap()
        }
    }

    fn do_single_value_range(
        datatype: Datatype,
        range: SingleValueRange,
    ) -> anyhow::Result<()> {
        test_clone(&range);

        let rr = Range::Single(range.clone());
        test_dimension_compatibility(&rr, datatype)?;

        #[cfg(feature = "serde")]
        test_serialization_roundtrip(&rr);

        let (start_slice, end_slice) =
            single_value_range_go!(range, _DT, ref start, ref end, {
                (start.to_le_bytes().to_vec(), end.to_le_bytes().to_vec())
            });
        test_from_slices(
            &rr,
            datatype,
            CellValNum::try_from(1)?,
            &start_slice[..],
            &end_slice[..],
        );
        Ok(())
    }

    fn do_multi_value_range(
        datatype: Datatype,
        range: MultiValueRange,
    ) -> anyhow::Result<()> {
        test_clone(&range);

        let rr = Range::Multi(range.clone());
        test_dimension_compatibility(&rr, datatype)?;

        #[cfg(feature = "serde")]
        test_serialization_roundtrip(&rr);

        let CellValNum::Fixed(cell_val_num) = range.cell_val_num() else {
            unreachable!()
        };

        let (start_slice, end_slice) =
            multi_value_range_go!(range, _DT, ref start, ref end, {
                assert_eq!(start.len(), end.len());
                assert_eq!(cell_val_num.get() as usize, start.len());

                let nbytes = std::mem::size_of_val(start.as_ref());

                let start_slice = unsafe {
                    std::slice::from_raw_parts(
                        start.as_ptr() as *mut u8 as *const u8,
                        nbytes,
                    )
                };
                let end_slice = unsafe {
                    std::slice::from_raw_parts(
                        end.as_ptr() as *mut u8 as *const u8,
                        nbytes,
                    )
                };
                (start_slice, end_slice)
            });

        test_from_slices(
            &rr,
            datatype,
            CellValNum::Fixed(cell_val_num),
            start_slice,
            end_slice,
        );

        // Check TryFrom failures
        multi_value_range_go!(range, _DT, ref start, ref end, {
            assert!(
                Range::try_from((
                    CellValNum::try_from(1)?,
                    start.clone(),
                    end.clone()
                ))
                .is_err()
            );
            assert!(
                Range::try_from((CellValNum::Var, start.clone(), end.clone()))
                    .is_err()
            );

            {
                let start = start.clone();
                let mut end = end.clone().into_vec();
                end.push(end[0]);
                let end = end.into_boxed_slice();
                assert!(
                    Range::try_from((
                        CellValNum::Fixed(cell_val_num),
                        start,
                        end
                    ))
                    .is_err()
                );
            }

            {
                let mut start = start.clone().into_vec();
                start.push(start[0]);
                let start = start.into_boxed_slice();
                let end = end.clone();
                assert!(
                    Range::try_from((
                        CellValNum::Fixed(cell_val_num),
                        start,
                        end
                    ))
                    .is_err()
                );
            }
        });
        Ok(())
    }

    fn do_var_value_range(
        datatype: Datatype,
        range: VarValueRange,
    ) -> anyhow::Result<()> {
        test_clone(&range);

        let rr = Range::Var(range.clone());
        test_dimension_compatibility(&rr, datatype)?;

        #[cfg(feature = "serde")]
        test_serialization_roundtrip(&rr);

        let (start_slice, end_slice) = var_value_range_go!(
            range,
            DT,
            ref start,
            ref end,
            #[allow(clippy::unnecessary_cast)]
            {
                let to_byte_slice = |s: &[DT]| unsafe {
                    std::slice::from_raw_parts(
                        if s.is_empty() {
                            std::ptr::NonNull::<DT>::dangling().as_ptr()
                                as *mut u8
                        } else {
                            s.as_ptr() as *mut u8
                        } as *const u8,
                        std::mem::size_of_val(s),
                    )
                };
                let start_slice = to_byte_slice(start);
                let end_slice = to_byte_slice(end);
                (start_slice, end_slice)
            }
        );

        test_from_slices(
            &rr,
            datatype,
            CellValNum::Var,
            start_slice,
            end_slice,
        );
        Ok(())
    }

    #[test]
    fn coverage_checks() {
        // Some stuff that didn't covered by the above tests.
        let _ = Range::from(("foo", "bar"));
        let _ = Range::from(&["foo", "bar"]);

        let range = Range::from(&[1u32, 2]);
        assert!(
            range
                .check_dimension_compatibility(
                    Datatype::UInt32,
                    2.try_into().unwrap(),
                )
                .is_err()
        );

        let range = Range::from((
            vec![].into_boxed_slice(),
            vec![1i32].into_boxed_slice(),
        ));
        assert!(
            range
                .check_dimension_compatibility(
                    Datatype::Int32,
                    1.try_into().unwrap()
                )
                .is_err()
        );

        let _ = format!("{:?}", range);
    }

    #[test]
    fn dimension_compatibility_string_ascii_var() {
        // single
        assert_eq!(
            Err(DimensionCompatibilityError::Datatype(
                DatatypeError::PhysicalTypeIncompatible {
                    physical_type: "u16",
                    logical_type: Datatype::StringAscii
                }
            )),
            Range::from(&[0u16, 1u16]).check_dimension_compatibility(
                Datatype::StringAscii,
                CellValNum::Var
            )
        );
        assert_eq!(
            Err(DimensionCompatibilityError::FixedRangeForStringDimension),
            Range::from(&[0u8, 1u8]).check_dimension_compatibility(
                Datatype::StringAscii,
                CellValNum::Var
            )
        );

        // multi
        assert_eq!(
            Err(DimensionCompatibilityError::MultiValueRange(2)),
            Range::Multi(
                MultiValueRange::try_from((
                    CellValNum::try_from(2).unwrap(),
                    vec![1u16, 10u16].into_boxed_slice(),
                    vec![10u16, 1u16].into_boxed_slice()
                ))
                .unwrap()
            )
            .check_dimension_compatibility(
                Datatype::StringAscii,
                CellValNum::Var
            )
        );
        assert_eq!(
            Err(DimensionCompatibilityError::MultiValueRange(2)),
            Range::Multi(
                MultiValueRange::try_from((
                    CellValNum::try_from(2).unwrap(),
                    vec![1u8, 10u8].into_boxed_slice(),
                    vec![10u8, 1u8].into_boxed_slice()
                ))
                .unwrap()
            )
            .check_dimension_compatibility(
                Datatype::StringAscii,
                CellValNum::Var
            )
        );

        // var but not u8
        assert_eq!(
            Err(DimensionCompatibilityError::Datatype(
                DatatypeError::PhysicalTypeIncompatible {
                    physical_type: "u16",
                    logical_type: Datatype::StringAscii
                }
            )),
            Range::Var(VarValueRange::from((
                vec![1u16, 10u16].into_boxed_slice(),
                vec![10u16, 1u16].into_boxed_slice()
            )))
            .check_dimension_compatibility(
                Datatype::StringAscii,
                CellValNum::Var
            )
        );

        // var u8
        assert_eq!(
            Ok(()),
            Range::Var(VarValueRange::from((
                vec![1u8, 10u8].into_boxed_slice(),
                vec![10u8, 1u8].into_boxed_slice()
            )))
            .check_dimension_compatibility(
                Datatype::StringAscii,
                CellValNum::Var
            )
        );
    }

    #[test]
    fn multi_range_num_cells() {
        let num_u32s = u32::MAX as u128 - u32::MIN as u128 + 1;

        // not sure how to write a proptest for this without
        // just re-implementing the function
        let cvn_2 = CellValNum::try_from(2).unwrap();
        let do_cvn_2 =
            |expect, (lb0, lb1): (u32, u32), (ub0, ub1): (u32, u32)| {
                assert_eq!(
                    Some(expect),
                    MultiValueRange::try_from((
                        cvn_2,
                        vec![lb0, lb1],
                        vec![ub0, ub1]
                    ))
                    .unwrap()
                    .num_cells()
                );
            };

        do_cvn_2(1, (0, 0), (0, 0));
        do_cvn_2(2, (0, 0), (0, 1));
        do_cvn_2(num_u32s + 1, (0, 0), (1, 0));
        do_cvn_2(num_u32s + 2, (0, 0), (1, 1));
        do_cvn_2(3, (0, 8), (0, 10));
        do_cvn_2(num_u32s * 2 + 1, (8, 0), (10, 0));
        do_cvn_2(num_u32s * 2 + 3, (8, 8), (10, 10));
        do_cvn_2(num_u32s, (0, 0), (0, u32::MAX));
        do_cvn_2(2, (0, u32::MAX), (1, 0));

        let cvn_3 = CellValNum::try_from(3).unwrap();
        let do_cvn_3 =
            |expect: u128,
             (lb0, lb1, lb2): (u32, u32, u32),
             (ub0, ub1, ub2): (u32, u32, u32)| {
                assert_eq!(
                    Some(expect),
                    MultiValueRange::try_from((
                        cvn_3,
                        vec![lb0, lb1, lb2],
                        vec![ub0, ub1, ub2]
                    ))
                    .unwrap()
                    .num_cells()
                );
            };
        do_cvn_3(1, (0, 0, 0), (0, 0, 0));
        do_cvn_3(4, (0, 0, 0), (0, 0, 3));
        do_cvn_3(num_u32s * 3 + 4, (0, 11, 0), (0, 14, 3));
        do_cvn_3(
            num_u32s * num_u32s * 3 + num_u32s * 3 + 4,
            (11, 11, 0),
            (14, 14, 3),
        );
        do_cvn_3(num_u32s, (0, 0, 0), (0, 0, u32::MAX));
        do_cvn_3((num_u32s - 1) * num_u32s + 1, (0, 0, 0), (0, u32::MAX, 0));
        do_cvn_3(
            (num_u32s - 1) * num_u32s + num_u32s,
            (0, 0, 0),
            (0, u32::MAX, u32::MAX),
        );
        do_cvn_3(2, (0, 0, u32::MAX), (0, 1, 0));
        do_cvn_3(2, (0, u32::MAX, u32::MAX), (1, 0, 0));
        do_cvn_3(2, (0, 0, u32::MAX), (0, 1, 0));
    }

    fn assert_intersection_body<B>(
        lstart: &B,
        lend: &B,
        rstart: &B,
        rend: &B,
        ostart: &B,
        oend: &B,
    ) where
        B: BitsOrd + Debug + ?Sized,
    {
        match lstart.bits_cmp(ostart) {
            Ordering::Less => {
                assert_eq!(Ordering::Equal, rstart.bits_cmp(ostart))
            }
            Ordering::Equal => assert!(rstart.bits_le(ostart)),
            Ordering::Greater => {
                unreachable!(
                    "Intersection of intervals is not narrower than an input"
                )
            }
        }
        match lend.bits_cmp(oend) {
            Ordering::Less => unreachable!(
                "Intersection of intervals is not narrower than an input"
            ),
            Ordering::Equal => assert!(rend.bits_ge(oend)),
            Ordering::Greater => {
                assert_eq!(Ordering::Equal, rend.bits_cmp(oend))
            }
        }

        // also check against false positives
        assert!(lstart.bits_le(rend));
        assert!(rstart.bits_le(lend));
        assert!(lend.bits_ge(rstart));
        assert!(rend.bits_ge(lstart));
    }

    fn do_intersection_single(left: SingleValueRange, right: SingleValueRange) {
        fn assert_intersection(
            left: SingleValueRange,
            right: SingleValueRange,
            output: SingleValueRange,
        ) {
            single_value_range_cmp!(
                left,
                right,
                output,
                DT,
                lstart,
                lend,
                rstart,
                rend,
                ostart,
                oend,
                assert_intersection_body::<DT>(
                    &lstart, &lend, &rstart, &rend, &ostart, &oend
                ),
                unreachable!()
            );
        }

        let output = left.intersection(&right);
        if let Some(output) = output {
            assert_intersection(left, right, output);
        } else {
            single_value_range_cmp!(
                left,
                right,
                _DT,
                lstart,
                lend,
                rstart,
                rend,
                {
                    assert!(lstart.bits_le(&lend));
                    assert!(rstart.bits_le(&rend));
                    assert!(lend.bits_lt(&rstart) || rend.bits_lt(&lstart));
                },
                unreachable!()
            )
        }
    }

    fn do_intersection_multi(left: MultiValueRange, right: MultiValueRange) {
        fn assert_intersection(
            left: MultiValueRange,
            right: MultiValueRange,
            output: MultiValueRange,
        ) {
            multi_value_range_cmp!(
                left,
                right,
                output,
                DT,
                lstart,
                lend,
                rstart,
                rend,
                ostart,
                oend,
                assert_intersection_body::<[DT]>(
                    &lstart, &lend, &rstart, &rend, &ostart, &oend
                ),
                unreachable!()
            );
        }

        let output = left.intersection(&right);
        if let Some(output) = output {
            assert_intersection(left, right, output);
        } else {
            multi_value_range_cmp!(
                left,
                right,
                _DT,
                lstart,
                lend,
                rstart,
                rend,
                {
                    assert!(lstart.bits_le(&lend));
                    assert!(rstart.bits_le(&rend));
                    assert!(lend.bits_lt(&rstart) || rend.bits_lt(&lstart));
                },
                unreachable!()
            )
        }
    }

    fn do_intersection_var(left: VarValueRange, right: VarValueRange) {
        fn assert_intersection(
            left: VarValueRange,
            right: VarValueRange,
            output: VarValueRange,
        ) {
            var_value_range_cmp!(
                left,
                right,
                output,
                DT,
                lstart,
                lend,
                rstart,
                rend,
                ostart,
                oend,
                assert_intersection_body::<[DT]>(
                    &lstart, &lend, &rstart, &rend, &ostart, &oend
                ),
                unreachable!()
            );
        }

        let output = left.intersection(&right);
        if let Some(output) = output {
            assert_intersection(left, right, output);
        } else {
            var_value_range_cmp!(
                left,
                right,
                _DT,
                lstart,
                lend,
                rstart,
                rend,
                {
                    assert!(lstart.bits_le(&lend));
                    assert!(rstart.bits_le(&rend));
                    assert!(lend.bits_lt(&rstart) || rend.bits_lt(&lstart));
                },
                unreachable!()
            )
        }
    }

    fn strat_intersection_single()
    -> impl Strategy<Value = (SingleValueRange, SingleValueRange)> {
        any::<Datatype>().prop_flat_map(|dt| {
            (
                any_with::<SingleValueRange>(Some(dt)),
                any_with::<SingleValueRange>(Some(dt)),
            )
        })
    }

    fn strat_intersection_multi()
    -> impl Strategy<Value = (MultiValueRange, MultiValueRange)> {
        (any::<Datatype>(), 2..1024u32).prop_flat_map(|(dt, nz)| {
            let nz = NonZeroU32::try_from(nz).unwrap();
            (
                any_with::<MultiValueRange>((Some(dt), Some(nz))),
                any_with::<MultiValueRange>((Some(dt), Some(nz))),
            )
        })
    }

    fn strat_intersection_var()
    -> impl Strategy<Value = (VarValueRange, VarValueRange)> {
        any::<Datatype>().prop_flat_map(|dt| {
            (
                any_with::<VarValueRange>(Some(dt)),
                any_with::<VarValueRange>(Some(dt)),
            )
        })
    }

    proptest! {
        #[test]
        fn intersection_single((left, right) in strat_intersection_single()) {
            do_intersection_single(left, right)
        }

        #[test]
        fn intersection_multi((left, right) in strat_intersection_multi()) {
            do_intersection_multi(left, right)
        }

        #[test]
        fn intersection_var((left, right) in strat_intersection_var()) {
            do_intersection_var(left, right)
        }
    }
}
