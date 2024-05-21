use std::fmt::{Debug, Formatter, Result as FmtResult};

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::array::CellValNum;
use crate::datatype::logical::*;
use crate::datatype::Datatype;
use crate::error::{DatatypeErrorKind, Error};
use crate::fn_typed;
use crate::Result as TileDBResult;

pub type NonEmptyDomain = Vec<TypedRange>;
pub type MinimumBoundingRectangle = Vec<TypedRange>;

macro_rules! check_datatype_inner {
    ($ty:ty, $dtype:expr) => {
        if !$dtype.is_compatible_type::<$ty>() {
            return Err(Error::Datatype(DatatypeErrorKind::TypeMismatch {
                user_type: std::any::type_name::<$ty>().to_owned(),
                tiledb_type: $dtype,
            }));
        }
    };
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

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
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
    /// Returns the number of cells spanned by this range if it is an integral range
    pub fn num_cells(&self) -> Option<u128> {
        let (low, high) = crate::single_value_range_go!(self, _DT : Integral, start, end,
            (i128::from(*start), i128::from(*end)),
            return None
        );
        Some(1 + (high - low) as u128)
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

    pub fn check_datatype(&self, datatype: Datatype) -> TileDBResult<()> {
        check_datatype!(self, datatype);
        Ok(())
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
        )+
    }
}

single_value_range_from!(UInt8: u8, UInt16: u16, UInt32: u32, UInt64: u64);
single_value_range_from!(Int8: i8, Int16: i16, Int32: i32, Int64: i64);
single_value_range_from!(Float32: f32, Float64: f64);

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
}

impl TryFrom<SingleValueRange> for std::ops::RangeInclusive<i128> {
    type Error = ();
    fn try_from(value: SingleValueRange) -> Result<Self, Self::Error> {
        type Target = i128;
        single_value_range_go!(value, _DT : Integral, start, end,
            {
                let start = Target::from(start);
                let end = Target::from(end);
                Ok(start..=end)
            },
            Err(())
        )
    }
}

#[derive(Clone, Deserialize, Serialize, PartialEq)]
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
    pub fn check_datatype(&self, datatype: Datatype) -> TileDBResult<()> {
        check_datatype!(self, datatype);
        Ok(())
    }
}

macro_rules! multi_value_range_try_from {
    ($($V:ident : $U:ty),+) => {
        $(
            impl TryFrom<(CellValNum, Box<[$U]>, Box<[$U]>)> for MultiValueRange {
                type Error = crate::error::Error;
                fn try_from(value: (CellValNum, Box<[$U]>, Box<[$U]>)) ->
                        TileDBResult<MultiValueRange> {
                    let cell_val_num = match value.0 {
                        CellValNum::Fixed(cvn) if u32::from(cvn) == 1u32 => {
                            return Err(Error::InvalidArgument(anyhow!(
                                "MultiValueRange does not support CellValNum::Fixed(1)"
                            )));
                        }
                        CellValNum::Fixed(cvn) => cvn.get(),
                        CellValNum::Var => {
                            return Err(Error::InvalidArgument(anyhow!(
                                "MultiValueRange does not support CellValNum::Var"
                            )));
                        }
                    };
                    if value.1.len() as u32 != cell_val_num {
                        return Err(Error::InvalidArgument(anyhow!(
                            "Invalid range start length. Found {}, not {}",
                            value.1.len(),
                            cell_val_num
                        )))
                    }
                    if value.2.len() as u32 != cell_val_num {
                        return Err(Error::InvalidArgument(anyhow!(
                            "Invalid range end length. Found {}, not {}",
                            value.2.len(),
                            cell_val_num
                        )))
                    }
                    Ok(MultiValueRange::$V(value.1, value.2))
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
    ($expr:expr, $DT:ident, $start:pat, $end:pat, $then:expr) => {
        match $expr {
            MultiValueRange::UInt8($start, $end) => {
                type $DT = u8;
                $then
            }
            MultiValueRange::UInt16($start, $end) => {
                type $DT = u16;
                $then
            }
            MultiValueRange::UInt32($start, $end) => {
                type $DT = u32;
                $then
            }
            MultiValueRange::UInt64($start, $end) => {
                type $DT = u64;
                $then
            }
            MultiValueRange::Int8($start, $end) => {
                type $DT = i8;
                $then
            }
            MultiValueRange::Int16($start, $end) => {
                type $DT = i16;
                $then
            }
            MultiValueRange::Int32($start, $end) => {
                type $DT = i32;
                $then
            }
            MultiValueRange::Int64($start, $end) => {
                type $DT = i64;
                $then
            }
            MultiValueRange::Float32($start, $end) => {
                type $DT = f32;
                $then
            }
            MultiValueRange::Float64($start, $end) => {
                type $DT = f64;
                $then
            }
        }
    };
}

#[derive(Clone, Deserialize, Serialize, PartialEq)]
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
    pub fn check_datatype(&self, datatype: Datatype) -> TileDBResult<()> {
        check_datatype!(self, datatype);
        Ok(())
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

#[derive(Clone, Deserialize, Serialize, PartialEq)]
pub enum Range {
    Single(SingleValueRange),
    Multi(MultiValueRange),
    Var(VarValueRange),
}

impl Range {
    // N.B. This is not `check_field_compatibility` because dimensions have
    // restrictions on their cell_val_num that don't apply to attributes.
    //
    // See: tiledb::sm::Dimension::set_cell_val_num
    pub fn check_dimension_compatibility(
        &self,
        datatype: Datatype,
        cell_val_num: CellValNum,
    ) -> TileDBResult<()> {
        match self {
            Self::Single(svr) => svr.check_datatype(datatype)?,
            Self::Multi(_) => {
                return Err(Error::InvalidArgument(anyhow!(
                    "Dimensions can not have a fixed cell val num > 1"
                )));
            }
            Self::Var(vvr) => vvr.check_datatype(datatype)?,
        }

        match cell_val_num {
            CellValNum::Fixed(cvn) => {
                if cvn.get() > 1 {
                    return Err(Error::InvalidArgument(anyhow!(
                        "Invalid cell val number: {}",
                        cvn.get()
                    )));
                }
                if datatype == Datatype::StringAscii {
                    return Err(Error::InvalidArgument(anyhow!(
                        "StringAscii dimensions must be var sized."
                    )));
                }
                if !matches!(self, Self::Single(_)) {
                    return Err(Error::InvalidArgument(anyhow!(
                        "Non-string dimensions must have a cell val num of 1."
                    )));
                }
            }
            CellValNum::Var => {
                if datatype != Datatype::StringAscii {
                    return Err(Error::InvalidArgument(anyhow!(
                        "Dimensions of type {} must have a cell val num of 1",
                        datatype
                    )));
                }
                if !matches!(self, Self::Var(VarValueRange::UInt8(_, _))) {
                    return Err(Error::InvalidArgument(anyhow!(
                        "String dimensions must use VarValueRange::UInt8"
                    )));
                }
            }
        }

        Ok(())
    }
}

impl Debug for Range {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}", json!(self))
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
                type Error = crate::error::Error;
                fn try_from(value: (CellValNum, Box<[$U]>, Box<[$U]>)) -> TileDBResult<Range> {
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

#[derive(Serialize, Deserialize, PartialEq)]
pub struct TypedRange {
    pub datatype: Datatype,
    pub range: Range,
}

impl TypedRange {
    pub fn new(datatype: Datatype, range: Range) -> Self {
        Self { datatype, range }
    }

    pub fn from_slices(
        datatype: Datatype,
        cell_val_num: CellValNum,
        start: &[u8],
        end: &[u8],
    ) -> TileDBResult<Self> {
        match cell_val_num {
            CellValNum::Var => {
                if start.len() as u64 % datatype.size() != 0 {
                    return Err(Error::InvalidArgument(anyhow!(
                        "Invalid start length not a multiple of {:?}",
                        datatype.size()
                    )));
                }
                if end.len() as u64 % datatype.size() != 0 {
                    return Err(Error::InvalidArgument(anyhow!(
                        "Invalid end length not a multiple of {:?}",
                        datatype.size()
                    )));
                }
            }
            CellValNum::Fixed(cvn) => {
                let expected_len = datatype.size() * cvn.get() as u64;
                if start.len() as u64 != expected_len {
                    return Err(Error::InvalidArgument(anyhow!(
                        "Invalid start length is {}, not {}",
                        start.len(),
                        expected_len
                    )));
                }
                if end.len() as u64 != expected_len {
                    return Err(Error::InvalidArgument(anyhow!(
                        "Invalid end length is {}, not {}",
                        start.len(),
                        expected_len
                    )));
                }
            }
        }

        fn_typed!(datatype, LT, {
            type DT = <LT as LogicalType>::PhysicalType;
            let start_slice = unsafe {
                std::slice::from_raw_parts(
                    start.as_ptr() as *const DT,
                    start.len() / datatype.size() as usize,
                )
            };
            let start = start_slice.to_vec().into_boxed_slice();
            let end_slice = unsafe {
                std::slice::from_raw_parts(
                    end.as_ptr() as *const DT,
                    end.len() / datatype.size() as usize,
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

impl Debug for TypedRange {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}", json!(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Result as TileDBResult;
    use proptest::collection::vec;
    use proptest::prelude::*;

    fn test_clone(range: &Range) {
        let other = range.clone();
        assert_eq!(*range, other);
    }

    fn test_dimension_compatibility(
        range: &Range,
        datatype: Datatype,
    ) -> TileDBResult<()> {
        match range {
            Range::Single(srange) => {
                if !matches!(datatype, Datatype::StringAscii) {
                    range.check_dimension_compatibility(
                        datatype,
                        1.try_into()?,
                    )?;
                } else {
                    assert!(range
                        .check_dimension_compatibility(datatype, 1.try_into()?)
                        .is_err());
                    srange.check_datatype(datatype)?;
                }
            }
            Range::Multi(mrange) => {
                // MultiValueRange is not valid for dimensions
                assert!(range
                    .check_dimension_compatibility(datatype, CellValNum::Var)
                    .is_err());
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
                    assert!(range
                        .check_dimension_compatibility(
                            datatype,
                            CellValNum::Var
                        )
                        .is_err());

                    // But we can still check the datatype correctness
                    vrange.check_datatype(datatype)?;
                }
            }
        }

        Ok(())
    }

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

    // fn_typed! seems to be fairly heavy for using with llvm-cov so I've
    // minimized the number of usages in these tests by adding test helpers
    // that are called from as few fn_typed macros as possible.
    #[test]
    fn test_single_value_range() {
        for datatype in Datatype::iter() {
            fn_typed!(datatype, LT, {
                type DT = <LT as LogicalType>::PhysicalType;
                proptest!(ProptestConfig::with_cases(8),
                        |(start in any::<DT>(), end in any::<DT>())| {

                    let range = Range::from(&[start, end]);
                    test_clone(&range);
                    test_dimension_compatibility(&range, *datatype)?;
                    test_serialization_roundtrip(&range);

                    let start_slice = start.to_le_bytes();
                    let end_slice = end.to_le_bytes();
                    test_from_slices(
                        &range,
                        *datatype,
                        CellValNum::try_from(1)?,
                        &start_slice[..],
                        &end_slice[..]
                    );
                });
            });
        }
    }

    #[test]
    fn test_multi_value_range() {
        for datatype in Datatype::iter() {
            fn_typed!(datatype, LT, {
                type DT = <LT as LogicalType>::PhysicalType;
                proptest!(ProptestConfig::with_cases(8),
                        |(data in vec(any::<DT>(), 2..=32))| {
                    let len = data.len() as u32;
                    let cell_val_num = CellValNum::try_from(len)?;
                    let start = data.clone().into_boxed_slice();
                    let end = start.clone();

                    let range = Range::try_from(
                        (cell_val_num, start.clone(), end.clone()))?;
                    test_clone(&range);
                    test_dimension_compatibility(&range, *datatype)?;
                    test_serialization_roundtrip(&range);

                    let nbytes = (len as u64 * datatype.size()) as usize;
                    let start = data.clone().into_boxed_slice();
                    let end = data.clone().into_boxed_slice();

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

                    test_from_slices(
                        &range,
                        *datatype,
                        CellValNum::try_from(len)?,
                        start_slice,
                        end_slice
                    );

                    // Check TryFrom failures
                    assert!(Range::try_from(
                        (CellValNum::try_from(1)?, start.clone(), end.clone())).is_err());
                    assert!(Range::try_from(
                        (CellValNum::Var, start.clone(), end.clone())).is_err());

                    let start = data.clone().into_boxed_slice();
                    let mut end = data.clone();
                    end.push(data[0]);
                    let end = end.into_boxed_slice();
                    assert!(Range::try_from((cell_val_num, start, end)).is_err());

                    let mut start = data.clone();
                    start.push(data[0]);
                    let start = start.into_boxed_slice();
                    let end = data.clone().into_boxed_slice();
                    assert!(Range::try_from((cell_val_num, start, end)).is_err());
                });
            });
        }
    }

    #[test]
    fn test_var_value_range() {
        for datatype in Datatype::iter() {
            fn_typed!(datatype, LT, {
                type DT = <LT as LogicalType>::PhysicalType;
                proptest!(ProptestConfig::with_cases(8),
                        |(start in vec(any::<DT>(), 0..=32), end in vec(any::<DT>(), 0..=32))| {
                    let start = start.into_boxed_slice();
                    let end = end.into_boxed_slice();

                    let range = Range::from((start.clone(), end.clone()));
                    test_clone(&range);
                    test_dimension_compatibility(&range, *datatype)?;
                    test_serialization_roundtrip(&range);

                    // Test from slices
                    let start_slice = unsafe {
                        std::slice::from_raw_parts(
                            start.as_ptr() as *mut u8 as *const u8,
                            std::mem::size_of_val(&*start),
                        )
                    };

                    let end_slice = unsafe {
                        std::slice::from_raw_parts(
                            end.as_ptr() as *mut u8 as *const u8,
                            std::mem::size_of_val(&*end),
                        )
                    };

                    test_from_slices(
                        &range,
                        *datatype,
                        CellValNum::Var,
                        start_slice,
                        end_slice
                    );
                });
            });
        }
    }

    #[test]
    fn coverage_checks() {
        // Some stuff that didn't covered by the above tests.
        let _ = Range::from(("foo", "bar"));
        let _ = Range::from(&["foo", "bar"]);

        let range = Range::from(&[1u32, 2]);
        assert!(range
            .check_dimension_compatibility(
                Datatype::UInt32,
                2.try_into().unwrap(),
            )
            .is_err());

        let range = Range::from((
            vec![].into_boxed_slice(),
            vec![1i32].into_boxed_slice(),
        ));
        assert!(range
            .check_dimension_compatibility(
                Datatype::Int32,
                1.try_into().unwrap()
            )
            .is_err());

        let range = Range::from(&[0u8, 1u8]);
        assert!(range
            .check_dimension_compatibility(
                Datatype::StringAscii,
                CellValNum::Var
            )
            .is_err());

        let _ = format!("{:?}", range);
    }
}
