use std::sync::Arc;

use arrow_array::builder::FixedSizeBinaryBuilder;
use arrow_array::types::{
    IntervalDayTimeType, IntervalMonthDayNanoType, IntervalYearMonthType,
};
use arrow_array::*;
use arrow_buffer::{i256, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, IntervalUnit, TimeUnit};
use proptest::collection::{vec as strat_vec, SizeRange};
use proptest::prelude::*;
use proptest::strategy::BoxedStrategy;

pub const DEFAULT_NONE_PROBABILITY: f64 = 0.0625f64;

#[derive(Clone, Debug)]
pub struct ArrayParameters {
    /// Strategy for choosing the number of rows in the column.
    pub num_rows: BoxedStrategy<usize>,
    /// Strategy for choosing the number of elements in variable-length column elements.
    pub num_collection_elements: SizeRange,
    /// Whether to allow null values.
    pub allow_null_values: bool,
    /// Whether to allow elements of collection types such as `DataType::LargeList`
    /// to be null. Defaults to `false`.
    pub allow_null_collection_element: bool,
}

impl Default for ArrayParameters {
    fn default() -> Self {
        Self {
            num_rows: (0..=8usize).boxed(),
            num_collection_elements: (0..=8).into(),
            allow_null_values: true,
            allow_null_collection_element: false,
        }
    }
}

pub fn prop_array(
    params: ArrayParameters,
    field: Arc<Field>,
) -> impl Strategy<Value = Arc<dyn Array>> {
    fn to_arc_dyn<T>(array: T) -> Arc<dyn Array>
    where
        T: Array + 'static,
    {
        Arc::new(array)
    }

    macro_rules! strat {
        ($nrows:expr, $strat:expr, $arraytype:ident) => {{
            strat!($nrows, $strat, $arraytype::from, $arraytype::from)
        }};
        ($nrows:expr, $strat:expr, $makearray:expr) => {{
            strat!($nrows, $strat, $makearray, $makearray)
        }};
        ($nrows:expr, $strat:expr, $nullable:expr, $nonnullable:expr) => {{
            if field.is_nullable() && params.allow_null_values {
                strat_vec(optional($strat), $nrows)
                    .prop_map($nullable)
                    .prop_map(to_arc_dyn)
                    .boxed()
            } else {
                strat_vec($strat, $nrows)
                    .prop_map($nonnullable)
                    .prop_map(to_arc_dyn)
                    .boxed()
            }
        }};
    }

    macro_rules! any {
        ($nrows:expr, $datatype:ty, $arraytype:ident) => {{
            strat!($nrows, any::<$datatype>(), $arraytype)
        }};
    }

    macro_rules! binary {
        ($nrows:expr, $eltlen:expr, $arraytype:ident) => {{
            let strat_element = strat_vec(any::<u8>(), $eltlen);
            strat!(
                $nrows,
                strat_element,
                |elts| $arraytype::from(
                    elts.iter()
                        .map(|e| e.as_ref().map(|e| e.as_ref()))
                        .collect::<Vec<Option<&[u8]>>>()
                ),
                |elts| $arraytype::from(
                    elts.iter().map(|e| e.as_ref()).collect::<Vec<&[u8]>>()
                )
            )
        }};
    }

    params.num_rows.clone().prop_flat_map(move |num_rows| {
        match field.data_type() {
            DataType::Null => {
                Just(to_arc_dyn(NullArray::new(num_rows))).boxed()
            }
            DataType::Boolean => any!(num_rows, bool, BooleanArray),
            DataType::Int8 => any!(num_rows, i8, Int8Array),
            DataType::Int16 => any!(num_rows, i16, Int16Array),
            DataType::Int32 => any!(num_rows, i32, Int32Array),
            DataType::Int64 => any!(num_rows, i64, Int64Array),
            DataType::UInt8 => any!(num_rows, u8, UInt8Array),
            DataType::UInt16 => any!(num_rows, u16, UInt16Array),
            DataType::UInt32 => any!(num_rows, u32, UInt32Array),
            DataType::UInt64 => any!(num_rows, u64, UInt64Array),
            DataType::Float16 => {
                strat!(
                    num_rows,
                    any::<f32>().prop_map(half::f16::from_f32),
                    Float16Array
                )
            }
            DataType::Float32 => any!(num_rows, f32, Float32Array),
            DataType::Float64 => any!(num_rows, f64, Float64Array),
            DataType::Timestamp(TimeUnit::Second, _) => {
                any!(num_rows, i64, TimestampSecondArray)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                any!(num_rows, i64, TimestampMillisecondArray)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                any!(num_rows, i64, TimestampMicrosecondArray)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                any!(num_rows, i64, TimestampNanosecondArray)
            }
            DataType::Date32 => any!(num_rows, i32, Date32Array),
            DataType::Date64 => any!(num_rows, i64, Date64Array),
            DataType::Time32(TimeUnit::Second) => {
                any!(num_rows, i32, Time32SecondArray)
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                any!(num_rows, i32, Time32MillisecondArray)
            }
            DataType::Time32(_) => {
                panic!("Invalid data type: {:?}", field.data_type())
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                any!(num_rows, i64, Time64MicrosecondArray)
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                any!(num_rows, i64, Time64NanosecondArray)
            }
            DataType::Duration(TimeUnit::Second) => {
                any!(num_rows, i64, DurationSecondArray)
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                any!(num_rows, i64, DurationMillisecondArray)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                any!(num_rows, i64, DurationMicrosecondArray)
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                any!(num_rows, i64, DurationNanosecondArray)
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                let strat_element = any::<i32>().prop_map(|val| {
                    IntervalYearMonthType::make_value(val / 12, val % 12)
                });
                strat!(num_rows, strat_element, IntervalYearMonthArray)
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                let strat_element =
                    (any::<i32>(), any::<i32>()).prop_map(|(days, millis)| {
                        IntervalDayTimeType::make_value(days, millis)
                    });
                strat!(num_rows, strat_element, IntervalDayTimeArray)
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                let strat_element = (any::<i32>(), any::<i32>(), any::<i64>())
                    .prop_map(|(months, days, nanos)| {
                        IntervalMonthDayNanoType::make_value(
                            months, days, nanos,
                        )
                    });
                strat!(num_rows, strat_element, IntervalMonthDayNanoArray)
            }
            DataType::Binary => binary!(
                num_rows,
                params.num_collection_elements.clone(),
                BinaryArray
            ),
            DataType::FixedSizeBinary(flen) => {
                let flen = *flen;
                let strat_element = strat_vec(any::<u8>(), flen as usize);
                strat!(
                    num_rows,
                    strat_element,
                    move |elts| {
                        let mut values = FixedSizeBinaryBuilder::with_capacity(
                            elts.len(),
                            flen,
                        );
                        elts.into_iter().for_each(|elt| {
                            if let Some(elt) = elt {
                                values.append_value(elt).unwrap();
                            } else {
                                values.append_null();
                            }
                        });
                        values.finish()
                    },
                    move |elts| FixedSizeBinaryArray::new(
                        flen,
                        elts.into_iter().flatten().collect::<Buffer>(),
                        None
                    )
                )
            }
            DataType::LargeBinary => {
                binary!(
                    num_rows,
                    params.num_collection_elements.clone(),
                    LargeBinaryArray
                )
            }
            DataType::Utf8 => any!(num_rows, String, StringArray),
            DataType::LargeUtf8 => any!(num_rows, String, LargeStringArray),
            DataType::Decimal128(p, s) => {
                let (p, s) = (*p, *s);
                strat!(num_rows, any::<i128>(), move |values| {
                    Decimal128Array::from(values)
                        .with_precision_and_scale(p, s)
                        .expect("Invalid precision and scale")
                })
            }
            DataType::Decimal256(p, s) => {
                let (p, s) = (*p, *s);
                strat!(
                    num_rows,
                    any::<[u8; 32]>().prop_map(i256::from_le_bytes),
                    move |values| Decimal256Array::from(values)
                        .with_precision_and_scale(p, s)
                        .expect("Invalid precision and scale")
                )
            }
            DataType::FixedSizeList(element, flen) => {
                let flen = *flen;
                let element = Arc::clone(&element);

                let values_parameters = ArrayParameters {
                    num_rows: Just(num_rows * (flen as usize)).boxed(),
                    allow_null_values: params.allow_null_collection_element,
                    ..params.clone()
                };

                (
                    prop_array(values_parameters, Arc::clone(&element)),
                    if field.is_nullable() {
                        strat_vec(
                            proptest::bool::weighted(
                                1.0 - DEFAULT_NONE_PROBABILITY,
                            ),
                            num_rows,
                        )
                        .prop_map(Some)
                        .boxed()
                    } else {
                        Just(None).boxed()
                    },
                )
                    .prop_map(move |(values, nulls)| {
                        FixedSizeListArray::new(
                            Arc::clone(&element),
                            flen,
                            values,
                            nulls
                                .map(|n| n.into_iter().collect::<NullBuffer>()),
                        )
                    })
                    .prop_map(to_arc_dyn)
                    .boxed()
            }
            DataType::LargeList(element) => {
                let field = Arc::clone(&field);
                let element = Arc::clone(element);
                let (min_values, max_values) = {
                    let r = params.num_collection_elements.clone();
                    (num_rows * r.start(), num_rows * r.end_incl())
                };
                let values_parameters = ArrayParameters {
                    num_rows: (min_values..=max_values).boxed(),
                    allow_null_values: params.allow_null_collection_element,
                    ..params.clone()
                };
                prop_array(values_parameters, Arc::clone(&element))
                    .prop_flat_map(move |values| {
                        let num_values = values.len();
                        (
                            Just(values),
                            strat_list_subdivisions(
                                num_values,
                                field.is_nullable(),
                            ),
                        )
                    })
                    .prop_map(move |(values, (offsets, nulls))| {
                        GenericListArray::new(
                            Arc::clone(&element),
                            offsets,
                            values,
                            nulls,
                        )
                    })
                    .prop_map(to_arc_dyn)
                    .boxed()
            }
            _ => unreachable!(
                "Not implemented in schema strategy: {}",
                field.data_type()
            ),
        }
    })
}

fn optional<T: Strategy>(strat: T) -> impl Strategy<Value = Option<T::Value>> {
    proptest::option::weighted(1.0 - DEFAULT_NONE_PROBABILITY, strat)
}

fn strat_list_subdivisions(
    num_elements: usize,
    is_nullable: bool,
) -> impl Strategy<Value = (OffsetBuffer<i64>, Option<NullBuffer>)> {
    let strat_offset = 0..=num_elements;
    let strat_num_lists = 0..=num_elements;

    strat_vec((strat_offset, any::<bool>()), strat_num_lists).prop_map(
        move |mut rows| {
            rows.sort();
            rows.push((num_elements, false));
            rows[0].0 = 0;

            let offsets = OffsetBuffer::new(
                rows.iter()
                    .map(|(o, _)| *o as i64)
                    .collect::<Vec<i64>>()
                    .into(),
            );
            let nulls = if is_nullable {
                Some(
                    rows.iter()
                        .map(|(_, b)| *b)
                        .take(rows.len() - 1)
                        .collect::<NullBuffer>(),
                )
            } else {
                None
            };
            (offsets, nulls)
        },
    )
}
