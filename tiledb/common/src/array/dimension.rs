use thiserror::Error;

#[cfg(feature = "option-subset")]
use tiledb_utils::option::OptionSubset;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::array::CellValNum;
use crate::datatype::{Datatype, Error as DatatypeError};
use crate::range::SingleValueRange;

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Invalid datatype: {0}")]
    Datatype(#[from] DatatypeError),
    #[error("Expected {} but found {0}", Datatype::StringAscii.to_string())]
    ExpectedStringAscii(Datatype),
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum DimensionConstraints {
    Int8([i8; 2], Option<i8>),
    Int16([i16; 2], Option<i16>),
    Int32([i32; 2], Option<i32>),
    Int64([i64; 2], Option<i64>),
    UInt8([u8; 2], Option<u8>),
    UInt16([u16; 2], Option<u16>),
    UInt32([u32; 2], Option<u32>),
    UInt64([u64; 2], Option<u64>),
    Float32([f32; 2], Option<f32>),
    Float64([f64; 2], Option<f64>),
    StringAscii,
}

#[macro_export]
macro_rules! dimension_constraints_go {
    ($expr:expr, $DT:ident, $range:pat, $extent:pat, $then:expr, $string:expr) => {{
        dimension_constraints_go!(
            $expr, $DT, $range, $extent, $then, $then, $string
        )
    }};
    ($expr:expr, $DT:ident, $range:pat, $extent:pat, $integral:expr, $float:expr, $string:expr) => {{
        use $crate::array::dimension::DimensionConstraints;
        match $expr {
            #[allow(unused_variables)]
            DimensionConstraints::Int8($range, $extent) => {
                #[allow(dead_code)]
                type $DT = i8;
                $integral
            }
            #[allow(unused_variables)]
            DimensionConstraints::Int16($range, $extent) => {
                #[allow(dead_code)]
                type $DT = i16;
                $integral
            }
            #[allow(unused_variables)]
            DimensionConstraints::Int32($range, $extent) => {
                #[allow(dead_code)]
                type $DT = i32;
                $integral
            }
            #[allow(unused_variables)]
            DimensionConstraints::Int64($range, $extent) => {
                #[allow(dead_code)]
                type $DT = i64;
                $integral
            }
            #[allow(unused_variables)]
            DimensionConstraints::UInt8($range, $extent) => {
                #[allow(dead_code)]
                type $DT = u8;
                $integral
            }
            #[allow(unused_variables)]
            DimensionConstraints::UInt16($range, $extent) => {
                #[allow(dead_code)]
                type $DT = u16;
                $integral
            }
            #[allow(unused_variables)]
            DimensionConstraints::UInt32($range, $extent) => {
                #[allow(dead_code)]
                type $DT = u32;
                $integral
            }
            #[allow(unused_variables)]
            DimensionConstraints::UInt64($range, $extent) => {
                #[allow(dead_code)]
                type $DT = u64;
                $integral
            }
            #[allow(unused_variables)]
            DimensionConstraints::Float32($range, $extent) => {
                #[allow(dead_code)]
                type $DT = f32;
                $float
            }
            #[allow(unused_variables)]
            DimensionConstraints::Float64($range, $extent) => {
                #[allow(dead_code)]
                type $DT = f64;
                $float
            }
            DimensionConstraints::StringAscii => $string,
        }
    }};
}

macro_rules! dimension_constraints_impl {
    ($($V:ident : $U:ty),+) => {
        $(
            impl From<[$U; 2]> for DimensionConstraints {
                fn from(value: [$U; 2]) -> DimensionConstraints {
                    DimensionConstraints::$V(value, None)
                }
            }

            impl From<&[$U; 2]> for DimensionConstraints {
                fn from(value: &[$U; 2]) -> DimensionConstraints {
                    DimensionConstraints::$V([value[0], value[1]], None)
                }
            }

            impl From<([$U; 2], $U)> for DimensionConstraints {
                fn from(value: ([$U; 2], $U)) -> DimensionConstraints {
                    DimensionConstraints::$V([value.0[0], value.0[1]], Some(value.1))
                }
            }

            impl From<(&[$U; 2], $U)> for DimensionConstraints {
                fn from(value: (&[$U; 2], $U)) -> DimensionConstraints {
                    DimensionConstraints::$V([value.0[0], value.0[1]], Some(value.1))
                }
            }

            impl From<([$U; 2], Option<$U>)> for DimensionConstraints {
                fn from(value: ([$U; 2], Option<$U>)) -> DimensionConstraints {
                    DimensionConstraints::$V([value.0[0], value.0[1]], value.1)
                }
            }

            impl From<(&[$U; 2], Option<$U>)> for DimensionConstraints {
                fn from(value: (&[$U; 2], Option<$U>)) -> DimensionConstraints {
                    DimensionConstraints::$V([value.0[0], value.0[1]], value.1)
                }
            }
        )+
    }
}

dimension_constraints_impl!(Int8: i8, Int16: i16, Int32: i32, Int64: i64);
dimension_constraints_impl!(UInt8: u8, UInt16: u16, UInt32: u32, UInt64: u64);
dimension_constraints_impl!(Float32: f32, Float64: f64);

impl DimensionConstraints {
    /// Returns a [Datatype] which represents the physical type of this constraint.
    pub fn physical_datatype(&self) -> Datatype {
        match self {
            Self::UInt8(_, _) => Datatype::UInt8,
            Self::UInt16(_, _) => Datatype::UInt16,
            Self::UInt32(_, _) => Datatype::UInt32,
            Self::UInt64(_, _) => Datatype::UInt64,
            Self::Int8(_, _) => Datatype::Int8,
            Self::Int16(_, _) => Datatype::Int16,
            Self::Int32(_, _) => Datatype::Int32,
            Self::Int64(_, _) => Datatype::Int64,
            Self::Float32(_, _) => Datatype::Float32,
            Self::Float64(_, _) => Datatype::Float64,
            Self::StringAscii => Datatype::StringAscii,
        }
    }

    pub fn cell_val_num(&self) -> CellValNum {
        match self {
            DimensionConstraints::StringAscii => CellValNum::Var,
            _ => CellValNum::single(),
        }
    }

    pub fn verify_type_compatible(
        &self,
        datatype: Datatype,
    ) -> Result<(), Error> {
        dimension_constraints_go!(
            self,
            DT,
            _range,
            _extent,
            {
                if !datatype.is_compatible_type::<DT>() {
                    return Err(Error::Datatype(
                        DatatypeError::physical_type_incompatible::<DT>(
                            datatype,
                        ),
                    ));
                }
            },
            {
                if !matches!(datatype, Datatype::StringAscii) {
                    return Err(Error::ExpectedStringAscii(datatype));
                }
            }
        );

        Ok(())
    }

    /// Returns the number of cells spanned by this constraint, if applicable
    pub fn num_cells(&self) -> Option<u128> {
        let (low, high) = crate::dimension_constraints_go!(
            self,
            _DT,
            [low, high],
            _,
            (i128::from(*low), i128::from(*high)),
            return None,
            return None
        );

        Some(1 + (high - low) as u128)
    }
    /// Returns the number of cells spanned by a
    /// single tile under this constraint, if applicable
    pub fn num_cells_per_tile(&self) -> Option<usize> {
        crate::dimension_constraints_go!(
            self,
            _DT,
            _,
            extent,
            extent.map(|extent| {
                #[allow(clippy::unnecessary_fallible_conversions)]
                // this `unwrap` should be safe, validation will confirm nonzero
                usize::try_from(extent).unwrap()
            }),
            None,
            None
        )
    }

    /// Returns the domain of the dimension constraint, if present, as a range.
    pub fn domain(&self) -> Option<SingleValueRange> {
        crate::dimension_constraints_go!(
            self,
            _DT,
            [low, high],
            _,
            Some(SingleValueRange::from(&[*low, *high])),
            None
        )
    }
}
