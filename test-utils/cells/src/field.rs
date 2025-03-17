use paste::paste;
use proptest::bits::{BitSetLike, VarBitSet};
use strategy_ext::records::Records;
use tiledb_common::array::CellValNum;
use tiledb_common::datatype::physical::{BitsEq, BitsOrd};
use tiledb_common::datatype::Error as DatatypeError;
use tiledb_common::physical_type_go;
use tiledb_common::range::{Range, SingleValueRange, VarValueRange};
use tiledb_pod::array::EnumerationData;

/// Represents the write query input for a single field.
///
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

#[macro_export]
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

            impl TryFrom<FieldData> for Vec<$U> {
                type Error = DatatypeError;

                fn try_from(value: FieldData) -> Result<Self, Self::Error> {
                    if let FieldData::$V(values) = value {
                        Ok(values)
                    } else {
                        $crate::typed_field_data_go!(value, DT, _,
                            {
                                Err(DatatypeError::physical_type_mismatch::<$U, DT>())
                            },
                            {
                                Err(DatatypeError::physical_type_mismatch::<$U, Vec<DT>>())
                            })
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

impl From<EnumerationData> for FieldData {
    fn from(value: EnumerationData) -> Self {
        physical_type_go!(value.datatype, DT, {
            const WIDTH: usize = std::mem::size_of::<DT>();
            type ByteArray = [u8; WIDTH];

            let dts = value.records().into_iter().map(|v| {
                assert_eq!(0, {
                    #[allow(clippy::modulo_one)]
                    {
                        v.len() % WIDTH
                    }
                });

                v.chunks(WIDTH)
                    .map(|c| DT::from_le_bytes(ByteArray::try_from(c).unwrap()))
                    .collect::<Vec<_>>()
            });
            if value.cell_val_num.is_none()
                || matches!(value.cell_val_num, Some(CellValNum::Fixed(nz)) if nz.get() == 1)
            {
                FieldData::from(
                    dts.map(|v| {
                        assert_eq!(1, v.len());
                        v[0]
                    })
                    .collect::<Vec<DT>>(),
                )
            } else if let Some(CellValNum::Fixed(nz)) = value.cell_val_num {
                FieldData::from(
                    dts.inspect(|v| {
                        assert_eq!(nz.get() as usize, v.len());
                    })
                    .collect::<Vec<Vec<DT>>>(),
                )
            } else {
                FieldData::from(dts.collect::<Vec<Vec<DT>>>())
            }
        })
    }
}

impl Records for FieldData {
    fn len(&self) -> usize {
        self.len()
    }

    fn filter(&self, subset: &VarBitSet) -> Self {
        self.filter(subset)
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
/// use cells::field::FieldData;
/// use cells::typed_field_data_go;
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
        $crate::typed_field_data_go!($field, _DT, $data, $then, $then)
    };
    ($field:expr, $DT:ident, $data:pat, $fixed:expr, $var:expr) => {
        $crate::typed_field_data_go!(
            $field, $DT, $data, $fixed, $var, $fixed, $var
        )
    };
    ($field:expr, $DT:ident, $data:pat, $integral_fixed:expr, $integral_var:expr, $float_fixed:expr, $float_var:expr) => {{
        use $crate::field::FieldData;
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
#[macro_export]
macro_rules! typed_field_data_cmp {
    ($lexpr:expr, $rexpr:expr, $DT:ident, $lpat:pat, $rpat:pat, $same_type:expr, $else:expr) => {{
        use $crate::field::FieldData;
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

    /// Returns the number of null values.
    ///
    /// At this time, values in `FieldData` are not nullable, so this is always zero.
    pub fn null_count(&self) -> usize {
        0
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

    /// Returns the bounding range of the data in this field.
    pub fn domain(&self) -> Option<Range> {
        typed_field_data_go!(
            self,
            _DT,
            cells,
            {
                cells
                    .iter()
                    .min_by(|l, r| l.bits_cmp(r))
                    .zip(cells.iter().max_by(|l, r| l.bits_cmp(r)))
                    .map(|(min, max)| {
                        Range::Single(SingleValueRange::from(&[*min, *max]))
                    })
            },
            {
                cells
                    .iter()
                    .min_by(|l, r| l.bits_cmp(r))
                    .zip(cells.iter().max_by(|l, r| l.bits_cmp(r)))
                    .map(|(min, max)| {
                        Range::Var(VarValueRange::from((
                            min.to_vec().into_boxed_slice(),
                            max.to_vec().into_boxed_slice(),
                        )))
                    })
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

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use proptest::prelude::*;
    use tiledb_common::array::CellValNum;
    use tiledb_common::datatype::Datatype;

    use super::*;
    use crate::strategy::{FieldDataParameters, FieldStrategyDatatype};

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

    proptest! {
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
    }

    /// Asserts that `field_data.domain()` is the tightest possible bound on the contents of
    /// `field_data`.
    fn do_field_domain(field_data: FieldData) {
        let Some(domain) = field_data.domain() else {
            assert!(field_data.is_empty());
            return;
        };

        macro_rules! check_correctness {
            ($min:expr, $max:expr, $values:expr) => {{
                // must be a proper bound
                for value in $values.iter() {
                    assert!($min.bits_le(value));
                    assert!(value.bits_le($max));
                }

                // the value must be in the field data
                assert!($values.iter().any(|value| value.bits_eq($min)));
                assert!($values.iter().any(|value| value.bits_eq($max)));
            }};
        }

        match field_data {
            FieldData::UInt8(values) => {
                let Range::Single(SingleValueRange::UInt8(min, max)) = domain
                else {
                    unreachable!()
                };
                check_correctness!(&min, &max, &values)
            }
            FieldData::UInt16(values) => {
                let Range::Single(SingleValueRange::UInt16(min, max)) = domain
                else {
                    unreachable!()
                };
                check_correctness!(&min, &max, &values)
            }
            FieldData::UInt32(values) => {
                let Range::Single(SingleValueRange::UInt32(min, max)) = domain
                else {
                    unreachable!()
                };
                check_correctness!(&min, &max, &values)
            }
            FieldData::UInt64(values) => {
                let Range::Single(SingleValueRange::UInt64(min, max)) = domain
                else {
                    unreachable!()
                };
                check_correctness!(&min, &max, &values)
            }
            FieldData::Int8(values) => {
                let Range::Single(SingleValueRange::Int8(min, max)) = domain
                else {
                    unreachable!()
                };
                check_correctness!(&min, &max, &values)
            }
            FieldData::Int16(values) => {
                let Range::Single(SingleValueRange::Int16(min, max)) = domain
                else {
                    unreachable!()
                };
                check_correctness!(&min, &max, &values)
            }
            FieldData::Int32(values) => {
                let Range::Single(SingleValueRange::Int32(min, max)) = domain
                else {
                    unreachable!()
                };
                check_correctness!(&min, &max, &values)
            }
            FieldData::Int64(values) => {
                let Range::Single(SingleValueRange::Int64(min, max)) = domain
                else {
                    unreachable!()
                };
                check_correctness!(&min, &max, &values)
            }
            FieldData::Float32(values) => {
                let Range::Single(SingleValueRange::Float32(min, max)) = domain
                else {
                    unreachable!()
                };
                check_correctness!(&min, &max, &values)
            }
            FieldData::Float64(values) => {
                let Range::Single(SingleValueRange::Float64(min, max)) = domain
                else {
                    unreachable!()
                };
                check_correctness!(&min, &max, &values)
            }
            FieldData::VecUInt8(values) => {
                let Range::Var(VarValueRange::UInt8(min, max)) = domain else {
                    unreachable!()
                };
                let slices =
                    values.iter().map(|v| v.as_slice()).collect::<Vec<_>>();
                check_correctness!(min.deref(), max.deref(), slices)
            }
            FieldData::VecUInt16(values) => {
                let Range::Var(VarValueRange::UInt16(min, max)) = domain else {
                    unreachable!()
                };
                let slices =
                    values.iter().map(|v| v.as_slice()).collect::<Vec<_>>();
                check_correctness!(min.deref(), max.deref(), slices)
            }
            FieldData::VecUInt32(values) => {
                let Range::Var(VarValueRange::UInt32(min, max)) = domain else {
                    unreachable!()
                };
                let slices =
                    values.iter().map(|v| v.as_slice()).collect::<Vec<_>>();
                check_correctness!(min.deref(), max.deref(), slices)
            }
            FieldData::VecUInt64(values) => {
                let Range::Var(VarValueRange::UInt64(min, max)) = domain else {
                    unreachable!()
                };
                let slices =
                    values.iter().map(|v| v.as_slice()).collect::<Vec<_>>();
                check_correctness!(min.deref(), max.deref(), slices)
            }
            FieldData::VecInt8(values) => {
                let Range::Var(VarValueRange::Int8(min, max)) = domain else {
                    unreachable!()
                };
                let slices =
                    values.iter().map(|v| v.as_slice()).collect::<Vec<_>>();
                check_correctness!(min.deref(), max.deref(), slices)
            }
            FieldData::VecInt16(values) => {
                let Range::Var(VarValueRange::Int16(min, max)) = domain else {
                    unreachable!()
                };
                let slices =
                    values.iter().map(|v| v.as_slice()).collect::<Vec<_>>();
                check_correctness!(min.deref(), max.deref(), slices)
            }
            FieldData::VecInt32(values) => {
                let Range::Var(VarValueRange::Int32(min, max)) = domain else {
                    unreachable!()
                };
                let slices =
                    values.iter().map(|v| v.as_slice()).collect::<Vec<_>>();
                check_correctness!(min.deref(), max.deref(), slices)
            }
            FieldData::VecInt64(values) => {
                let Range::Var(VarValueRange::Int64(min, max)) = domain else {
                    unreachable!()
                };
                let slices =
                    values.iter().map(|v| v.as_slice()).collect::<Vec<_>>();
                check_correctness!(min.deref(), max.deref(), slices)
            }
            FieldData::VecFloat32(values) => {
                let Range::Var(VarValueRange::Float32(min, max)) = domain
                else {
                    unreachable!()
                };
                let slices =
                    values.iter().map(|v| v.as_slice()).collect::<Vec<_>>();
                check_correctness!(min.deref(), max.deref(), slices)
            }
            FieldData::VecFloat64(values) => {
                let Range::Var(VarValueRange::Float64(min, max)) = domain
                else {
                    unreachable!()
                };
                let slices =
                    values.iter().map(|v| v.as_slice()).collect::<Vec<_>>();
                check_correctness!(min.deref(), max.deref(), slices)
            }
        }
    }

    proptest! {
        #[test]
        fn field_domain(field_data in any::<FieldData>()) {
            do_field_domain(field_data)
        }
    }
}
