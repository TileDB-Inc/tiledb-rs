use std::num::NonZeroU32;
use std::sync::Arc;

use arrow::datatypes::{ArrowNativeTypeOp, ArrowPrimitiveType};

use crate::array::CellValNum;
use crate::Datatype;

pub trait ArrowPrimitiveTypeNative: ArrowNativeTypeOp {
    type ArrowPrimitiveType: ArrowPrimitiveType<Native = Self>;
}

impl ArrowPrimitiveTypeNative for i8 {
    type ArrowPrimitiveType = arrow::datatypes::Int8Type;
}

impl ArrowPrimitiveTypeNative for i16 {
    type ArrowPrimitiveType = arrow::datatypes::Int16Type;
}

impl ArrowPrimitiveTypeNative for i32 {
    type ArrowPrimitiveType = arrow::datatypes::Int32Type;
}

impl ArrowPrimitiveTypeNative for i64 {
    type ArrowPrimitiveType = arrow::datatypes::Int64Type;
}

impl ArrowPrimitiveTypeNative for u8 {
    type ArrowPrimitiveType = arrow::datatypes::UInt8Type;
}

impl ArrowPrimitiveTypeNative for u16 {
    type ArrowPrimitiveType = arrow::datatypes::UInt16Type;
}

impl ArrowPrimitiveTypeNative for u32 {
    type ArrowPrimitiveType = arrow::datatypes::UInt32Type;
}

impl ArrowPrimitiveTypeNative for u64 {
    type ArrowPrimitiveType = arrow::datatypes::UInt64Type;
}

impl ArrowPrimitiveTypeNative for f32 {
    type ArrowPrimitiveType = arrow::datatypes::Float32Type;
}

impl ArrowPrimitiveTypeNative for f64 {
    type ArrowPrimitiveType = arrow::datatypes::Float64Type;
}

/// For a TileDB type and cell val num, returns a compatible Arrow type.
/// An arrow type is compatible if:
/// 1) the bits of the primitive values of the tiledb type can
///    be interpreted as sound values of the arrow type.
/// 2) it carries the same number of primitive values as is expressed by the cell val num.
///
/// In general, this means that:
/// 1) a cell val num of 1 maps to an arrow primitive or date/time type,
/// 2) a cell val num of greater than 1 maps to an arrow fixed size list,
/// 3) a variable cell val num maps to an arrow list .
///
/// There are exceptions, such as (Datatype::Utf8, CellValNum::Var) mapping
/// to the arrow Utf8 type which is always variable-length.
///
/// This function is not always invertible as there are some date/time types in tiledb
/// which have no corresponding bit width or precision in arrow.
pub fn arrow_type_physical(
    tdb_dt: &Datatype,
    cell_val_num: CellValNum,
) -> arrow::datatypes::DataType {
    use arrow::datatypes::DataType as ADT;
    use arrow::datatypes::TimeUnit;

    match cell_val_num {
        CellValNum::Fixed(nz) if nz.get() == 1 => {
            match tdb_dt {
                Datatype::Int8 | Datatype::Char => ADT::Int8,
                Datatype::Int16 => ADT::Int16,
                Datatype::Int32 => ADT::Int32,
                Datatype::Int64 => ADT::Int64,
                Datatype::UInt8
                | Datatype::StringAscii
                | Datatype::Any
                | Datatype::Blob
                | Datatype::Boolean
                | Datatype::GeometryWkb
                | Datatype::GeometryWkt => ADT::UInt8,
                Datatype::UInt16 => ADT::UInt16,
                Datatype::UInt32 => ADT::UInt32,
                Datatype::UInt64 => ADT::UInt64,
                Datatype::Float32 => ADT::Float32,
                Datatype::Float64 => ADT::Float64,
                Datatype::StringUtf8 => ADT::UInt8,
                Datatype::StringUtf16 => ADT::UInt16,
                Datatype::StringUtf32 => ADT::UInt32,
                Datatype::StringUcs2 => ADT::UInt16,
                Datatype::StringUcs4 => ADT::UInt32,
                Datatype::DateTimeSecond => {
                    ADT::Timestamp(TimeUnit::Second, None)
                }
                Datatype::DateTimeMillisecond => {
                    ADT::Timestamp(TimeUnit::Millisecond, None)
                }
                Datatype::DateTimeMicrosecond => {
                    ADT::Timestamp(TimeUnit::Microsecond, None)
                }
                Datatype::DateTimeNanosecond => {
                    ADT::Timestamp(TimeUnit::Nanosecond, None)
                }
                Datatype::DateTimeDay
                | Datatype::DateTimeYear
                | Datatype::DateTimeMonth
                | Datatype::DateTimeWeek
                | Datatype::DateTimeHour
                | Datatype::DateTimeMinute
                | Datatype::DateTimePicosecond
                | Datatype::DateTimeFemtosecond
                | Datatype::DateTimeAttosecond
                | Datatype::TimeHour
                | Datatype::TimeMinute
                | Datatype::TimeSecond
                | Datatype::TimeMillisecond
                | Datatype::TimePicosecond
                | Datatype::TimeFemtosecond
                | Datatype::TimeAttosecond => {
                    // these are signed 64-bit integers in tiledb,
                    // arrow datetypes with the same precision are 32 bits
                    // (or there is no equivalent time unit)
                    ADT::Int64
                }
                Datatype::TimeMicrosecond => ADT::Time64(TimeUnit::Microsecond),
                Datatype::TimeNanosecond => ADT::Time64(TimeUnit::Nanosecond),
            }
        }
        CellValNum::Fixed(nz) => match i32::try_from(nz.get()) {
            Ok(nz) => {
                if matches!(tdb_dt, Datatype::Blob) {
                    ADT::FixedSizeBinary(nz)
                } else {
                    let item =
                        arrow_type_physical(tdb_dt, CellValNum::single());
                    ADT::FixedSizeList(
                        Arc::new(arrow::datatypes::Field::new_list_field(
                            item, false,
                        )),
                        nz,
                    )
                }
            }
            Err(_) => unimplemented!(),
        },
        CellValNum::Var => match tdb_dt {
            Datatype::StringAscii | Datatype::StringUtf8 => ADT::Utf8,
            Datatype::Blob => ADT::Binary,
            dt => {
                let item = arrow_type_physical(dt, CellValNum::single());
                ADT::new_list(item, false)
            }
        },
    }
}

/// For an Arrow type, returns a compatible TileDB type and cell val num.
/// A type is compatible if:
/// 1) the bits of the primitive values of the arrow type can
///    be interpreted as sound values of the tiledb type.
/// 2) it carries the same number of primitive values.
///
/// This function is not always invertible.
/// TODO: name some exceptions
pub fn tiledb_type_physical(
    arrow_dt: &arrow::datatypes::DataType,
) -> Option<(Datatype, CellValNum)> {
    use arrow::datatypes::DataType as ADT;
    use arrow::datatypes::TimeUnit;

    Some(match arrow_dt {
        ADT::Null => return None,
        ADT::Int8 => (Datatype::Int8, CellValNum::single()),
        ADT::Int16 => (Datatype::Int16, CellValNum::single()),
        ADT::Int32 => (Datatype::Int32, CellValNum::single()),
        ADT::Int64 => (Datatype::Int64, CellValNum::single()),
        ADT::UInt8 => (Datatype::UInt8, CellValNum::single()),
        ADT::UInt16 => (Datatype::UInt16, CellValNum::single()),
        ADT::UInt32 => (Datatype::UInt32, CellValNum::single()),
        ADT::UInt64 => (Datatype::UInt64, CellValNum::single()),
        ADT::Float16 => {
            /* tiledb has no f16 type, so use u16 as a 2-byte container */
            (Datatype::UInt16, CellValNum::single())
        }
        ADT::Float32 => (Datatype::Float32, CellValNum::single()),
        ADT::Float64 => (Datatype::Float64, CellValNum::single()),
        ADT::Decimal128(_, _) | ADT::Decimal256(_, _) => {
            /*
             * We could map this to fixed-length blob but probably
             * better to do a proper 128 or 256 bit thing in core
             * so we avoid making mistakes here
             */
            return None;
        }
        ADT::Timestamp(TimeUnit::Second, _) => {
            (Datatype::DateTimeSecond, CellValNum::single())
        }
        ADT::Timestamp(TimeUnit::Millisecond, _) => {
            (Datatype::DateTimeMillisecond, CellValNum::single())
        }
        ADT::Timestamp(TimeUnit::Microsecond, _) => {
            (Datatype::DateTimeMicrosecond, CellValNum::single())
        }
        ADT::Timestamp(TimeUnit::Nanosecond, _) => {
            (Datatype::DateTimeNanosecond, CellValNum::single())
        }
        ADT::Date32 | ADT::Time32(_) => (Datatype::Int32, CellValNum::single()),
        ADT::Date64 => (Datatype::DateTimeMillisecond, CellValNum::single()),
        ADT::Time64(TimeUnit::Microsecond) => {
            (Datatype::TimeMicrosecond, CellValNum::single())
        }
        ADT::Time64(TimeUnit::Nanosecond) => {
            (Datatype::TimeNanosecond, CellValNum::single())
        }
        ADT::Time64(_) => (Datatype::UInt64, CellValNum::single()),
        ADT::Boolean => {
            /* this may be bit-packed by arrow but is not by tiledb */
            return None;
        }
        ADT::Duration(_) | ADT::Interval(_) => {
            /* these are scalars but the doc does not specify bit width */
            return None;
        }
        ADT::Binary => (Datatype::Blob, CellValNum::Var),
        ADT::FixedSizeBinary(len) => (
            Datatype::Blob,
            match NonZeroU32::new(*len as u32) {
                None => return None,
                Some(nz) => CellValNum::Fixed(nz),
            },
        ),
        ADT::FixedSizeList(ref item, ref len) => {
            if item.data_type().primitive_width().is_none() {
                /*
                 * probably there are some cases we can handle,
                 * but let's omit for now
                 */
                return None;
            }
            if let Some((dt, item_cell_val)) =
                tiledb_type_physical(item.data_type())
            {
                let len = match NonZeroU32::new(*len as u32) {
                    None => return None,
                    Some(nz) => nz,
                };
                match item_cell_val {
                    CellValNum::Fixed(nz) => match nz.checked_mul(len) {
                        None => return None,
                        Some(nz) => (dt, CellValNum::Fixed(nz)),
                    },
                    CellValNum::Var => (dt, CellValNum::Var),
                }
            } else {
                return None;
            }
        }
        ADT::Utf8 => (Datatype::StringUtf8, CellValNum::Var),
        ADT::List(ref item) => {
            if item.data_type().primitive_width().is_none() {
                /*
                 * probably there are some cases we can handle,
                 * but let's omit for now
                 */
                return None;
            }
            if let Some((dt, item_cell_val)) =
                tiledb_type_physical(item.data_type())
            {
                match item_cell_val {
                    CellValNum::Fixed(_) => {
                        /* whatever the cell val num may be, we need one level of offsets */
                        (dt, CellValNum::Var)
                    }
                    CellValNum::Var => {
                        /* we will not be able to do two levels of offsets */
                        return None;
                    }
                }
            } else {
                return None;
            }
        }
        ADT::LargeBinary | ADT::LargeUtf8 | ADT::LargeList(_) => {
            /* cell val num is only 32 bites, there is no way to represent 64-bit offsets */
            return None;
        }
        ADT::Struct(_)
        | ADT::Union(_, _)
        | ADT::Dictionary(_, _)
        | ADT::Map(_, _)
        | ADT::RunEndEncoded(_, _) => {
            /* complex types are not implemented */
            return None;
        }
    })
}

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy {
    use std::sync::Arc;

    use proptest::prelude::*;

    #[derive(Clone, Debug)]
    pub struct FieldParameters {
        pub min_fixed_binary_len: i32,
        pub max_fixed_binary_len: i32,
        pub min_numeric_precision: u8,
        pub max_numeric_precision: u8,
        pub min_fixed_list_len: i32,
        pub max_fixed_list_len: i32,
        pub min_struct_fields: usize,
        pub max_struct_fields: usize,
        pub min_recursion_depth: u32,
        pub max_recursion_depth: u32,
    }

    impl Default for FieldParameters {
        fn default() -> Self {
            FieldParameters {
                min_fixed_binary_len: 1,
                max_fixed_binary_len: i32::MAX,
                min_numeric_precision: 1,
                max_numeric_precision: u8::MAX,
                min_fixed_list_len: 0,
                max_fixed_list_len: i32::MAX,
                min_struct_fields: 0,
                max_struct_fields: 16,
                min_recursion_depth: 0,
                max_recursion_depth: 8,
            }
        }
    }

    pub fn any_datatype(
        params: FieldParameters,
    ) -> impl Strategy<Value = arrow::datatypes::DataType> {
        use arrow::datatypes::DataType as ADT;
        use arrow::datatypes::{Field, Fields, IntervalUnit, TimeUnit};

        let leaf = prop_oneof![
            Just(ADT::Null),
            Just(ADT::Int8),
            Just(ADT::Int16),
            Just(ADT::Int32),
            Just(ADT::Int64),
            Just(ADT::UInt8),
            Just(ADT::UInt16),
            Just(ADT::UInt32),
            Just(ADT::UInt64),
            Just(ADT::Float16),
            Just(ADT::Float32),
            Just(ADT::Float64),
            Just(ADT::Timestamp(TimeUnit::Second, None)),
            Just(ADT::Timestamp(TimeUnit::Millisecond, None)),
            Just(ADT::Timestamp(TimeUnit::Microsecond, None)),
            Just(ADT::Timestamp(TimeUnit::Nanosecond, None)),
            Just(ADT::Date32),
            Just(ADT::Date64),
            Just(ADT::Time32(TimeUnit::Second)),
            Just(ADT::Time32(TimeUnit::Millisecond)),
            Just(ADT::Time64(TimeUnit::Microsecond)),
            Just(ADT::Time64(TimeUnit::Nanosecond)),
            Just(ADT::Duration(TimeUnit::Second)),
            Just(ADT::Duration(TimeUnit::Millisecond)),
            Just(ADT::Duration(TimeUnit::Nanosecond)),
            Just(ADT::Interval(IntervalUnit::YearMonth)),
            Just(ADT::Interval(IntervalUnit::DayTime)),
            Just(ADT::Interval(IntervalUnit::MonthDayNano)),
            Just(ADT::Binary),
            (params.min_fixed_binary_len..=params.max_fixed_binary_len)
                .prop_map(|len| ADT::FixedSizeBinary(len)),
            Just(ADT::LargeBinary),
            Just(ADT::Utf8),
            Just(ADT::LargeUtf8),
            (params.min_numeric_precision..=params.max_numeric_precision)
                .prop_flat_map(|precision| (
                    Just(precision),
                    (0..precision.clamp(0, i8::MAX as u8) as i8)
                )
                    .prop_map(|(precision, scale)| ADT::Decimal128(
                        precision, scale
                    ))),
            (params.min_numeric_precision..=params.max_numeric_precision)
                .prop_flat_map(|precision| (
                    Just(precision),
                    (0..precision.clamp(0, i8::MAX as u8) as i8)
                )
                    .prop_map(|(precision, scale)| ADT::Decimal256(
                        precision, scale
                    ))),
        ];

        leaf.prop_recursive(
            params.max_recursion_depth,
            params.max_recursion_depth * 4,
            std::cmp::max(
                2,
                (params.max_struct_fields / 4).try_into().unwrap(),
            ),
            move |strategy| {
                prop_oneof![
                (strategy.clone(), any::<bool>())
                    .prop_map(|(s, b)| ADT::new_list(s, b)),
                (
                    strategy.clone(),
                    params.min_fixed_list_len..=params.max_fixed_list_len,
                    any::<bool>()
                )
                    .prop_map(|(s, l, b)| ADT::FixedSizeList(
                        Arc::new(Field::new_list_field(s, b)),
                        l
                    )),
                (strategy.clone(), any::<bool>()).prop_map(|(s, b)| {
                    ADT::LargeList(Arc::new(Field::new_list_field(s, b)))
                }),
                proptest::collection::vec(
                    (
                        crate::array::attribute::strategy::prop_attribute_name(
                        ),
                        strategy.clone(),
                        any::<bool>()
                    ),
                    params.min_struct_fields..=params.max_struct_fields
                )
                .prop_map(|v| ADT::Struct(
                    v.into_iter()
                        .map(|(n, dt, b)| Field::new(n, dt, b))
                        .collect::<Fields>()
                )) // union goes here
                   // dictionary goes here
                   // map goes here
                   // run-end encoded goes here
            ]
            },
        )
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_arrow_type_physical_single(tdb_dt in any::<Datatype>()) {
            let cell_val_num = CellValNum::single();
            let arrow_dt = arrow_type_physical(&tdb_dt, cell_val_num);
            assert!(arrow_dt.is_primitive());
            let arrow_size = arrow_dt.primitive_width().unwrap() as u64;
            assert_eq!(tdb_dt.size(), arrow_size,
                "arrow_type_physical({}, {:?}) = {}", tdb_dt, cell_val_num, arrow_dt);

            let (tdb_out, cell_val_num_out) = tiledb_type_physical(&arrow_dt)
                .expect("Arrow datatype constructed from tiledb datatype must convert back");
            /* the datatype may not match exactly but it must be the same size */
            assert_eq!(tdb_dt.size(), tdb_out.size());
            assert_eq!(cell_val_num, cell_val_num_out);
        }

        #[test]
        fn test_arrow_type_physical_nonvar(tdb_dt in any::<Datatype>()) {
            let fixed_len_in = 32u32;
            let cell_val_num = CellValNum::try_from(fixed_len_in).unwrap();
            let arrow_dt = arrow_type_physical(&tdb_dt, cell_val_num);

            use arrow::datatypes::DataType as ADT;
            match arrow_dt {
                ADT::FixedSizeList(ref item, fixed_len_out) => {
                    let item_dt  = arrow_type_physical(&tdb_dt, CellValNum::single());
                    assert_eq!(*item.data_type(), item_dt);
                    assert_eq!(fixed_len_in, fixed_len_out as u32);
                },
                ADT::FixedSizeBinary(fixed_len_out) => {
                    assert_eq!(tdb_dt, Datatype::Blob);
                    assert_eq!(fixed_len_in, fixed_len_out as u32);
                }
                adt => unreachable!("arrow_type_physical({}, {:?}) = {}", tdb_dt, cell_val_num, adt)
            }

            let (tdb_out, cell_val_num_out) = tiledb_type_physical(&arrow_dt)
                .expect("Arrow datatype constructed from tiledb datatype must convert back");
            /* the datatype may not match exactly but it must be the same size */
            assert_eq!(tdb_dt.size(), tdb_out.size());
            assert_eq!(cell_val_num, cell_val_num_out);
        }

        #[test]
        fn test_arrow_type_physical_var(tdb_dt in any::<Datatype>()) {
            let cell_val_num = CellValNum::Var;
            let arrow_dt = arrow_type_physical(&tdb_dt, cell_val_num);

            assert!(!arrow_dt.is_primitive(), "arrow_type_physical({}, {:?}) = {}",
            tdb_dt, cell_val_num, arrow_dt);

            use arrow::datatypes::DataType as ADT;
            match arrow_dt {
                ADT::List(ref item) => {
                    let item_dt = arrow_type_physical(&tdb_dt, CellValNum::single());
                    assert_eq!(*item.data_type(), item_dt);
                },
                ADT::Utf8 => assert!(matches!(tdb_dt, Datatype::StringAscii | Datatype::StringUtf8)),
                ADT::Binary => assert!(matches!(tdb_dt, Datatype::Blob)),
                adt => unreachable!("arrow_type_physical({}, {:?}) = {}", tdb_dt, cell_val_num, adt)
            }

            let (tdb_out, cell_val_num_out) = tiledb_type_physical(&arrow_dt)
                .expect("Arrow datatype constructed from tiledb datatype must convert back");
            /* the datatype may not match exactly but it must be the same size */
            assert_eq!(tdb_dt.size(), tdb_out.size());
            assert_eq!(cell_val_num, cell_val_num_out);
        }
    }
}
