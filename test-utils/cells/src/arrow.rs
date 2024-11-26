use itertools::Itertools;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::downcast_array;
use arrow_array::types::*;
use arrow_array::{
    downcast_primitive_array, Array, ArrowPrimitiveType, LargeBinaryArray,
    LargeListArray, LargeStringArray, PrimitiveArray, RecordBatch,
};
use arrow_buffer::{ArrowNativeType, OffsetBuffer};
use arrow_schema::{DataType, Field, Fields, Schema};

use crate::{typed_field_data_go, Cells, FieldData};

pub fn to_record_batch(cells: &Cells) -> RecordBatch {
    let (fnames, columns) = cells
        .fields()
        .iter()
        .map(|(fname, fdata)| (fname, to_column(fdata)))
        .collect::<(Vec<_>, Vec<_>)>();

    let fields = fnames
        .into_iter()
        .zip(columns.iter())
        .map(|(fname, column)| {
            Field::new(
                fname.to_owned(),
                column.data_type().clone(),
                column.null_count() > 0,
            )
        })
        .collect::<Fields>();

    let schema = Schema {
        fields,
        metadata: Default::default(),
    };

    RecordBatch::try_new(schema.into(), columns).unwrap()
}

pub fn from_record_batch(batch: &RecordBatch) -> Option<Cells> {
    batch
        .schema()
        .fields
        .iter()
        .zip_eq(batch.columns().iter())
        .map(|(f, c)| from_column(c).map(|fdata| (f.name().to_owned(), fdata)))
        .collect::<Option<HashMap<String, FieldData>>>()
        .map(Cells::new)
}

pub fn to_column(fdata: &FieldData) -> Arc<dyn Array> {
    match fdata {
        FieldData::Int8(cells) => to_column_primitive::<i8, Int8Type>(cells),
        FieldData::Int16(cells) => to_column_primitive::<i16, Int16Type>(cells),
        FieldData::Int32(cells) => to_column_primitive::<i32, Int32Type>(cells),
        FieldData::Int64(cells) => to_column_primitive::<i64, Int64Type>(cells),
        FieldData::UInt8(cells) => to_column_primitive::<u8, UInt8Type>(cells),
        FieldData::UInt16(cells) => {
            to_column_primitive::<u16, UInt16Type>(cells)
        }
        FieldData::UInt32(cells) => {
            to_column_primitive::<u32, UInt32Type>(cells)
        }
        FieldData::UInt64(cells) => {
            to_column_primitive::<u64, UInt64Type>(cells)
        }
        FieldData::Float32(cells) => {
            to_column_primitive::<f32, Float32Type>(cells)
        }
        FieldData::Float64(cells) => {
            to_column_primitive::<f64, Float64Type>(cells)
        }
        FieldData::VecUInt8(cells) => to_column_list::<u8, UInt8Type>(cells),
        FieldData::VecUInt16(cells) => to_column_list::<u16, UInt16Type>(cells),
        FieldData::VecUInt32(cells) => to_column_list::<u32, UInt32Type>(cells),
        FieldData::VecUInt64(cells) => to_column_list::<u64, UInt64Type>(cells),
        FieldData::VecInt8(cells) => to_column_list::<i8, Int8Type>(cells),
        FieldData::VecInt16(cells) => to_column_list::<i16, Int16Type>(cells),
        FieldData::VecInt32(cells) => to_column_list::<i32, Int32Type>(cells),
        FieldData::VecInt64(cells) => to_column_list::<i64, Int64Type>(cells),
        FieldData::VecFloat32(cells) => {
            to_column_list::<f32, Float32Type>(cells)
        }
        FieldData::VecFloat64(cells) => {
            to_column_list::<f64, Float64Type>(cells)
        }
    }
}

fn to_column_primitive<T, A>(cells: &[T]) -> Arc<dyn Array>
where
    T: ArrowNativeType,
    A: ArrowPrimitiveType,
    PrimitiveArray<A>: From<Vec<T>>,
{
    Arc::new(PrimitiveArray::<A>::from(cells.to_vec()))
}

fn to_column_list<T, A>(cells: &[Vec<T>]) -> Arc<dyn Array>
where
    T: ArrowNativeType,
    A: ArrowPrimitiveType,
    PrimitiveArray<A>: From<Vec<T>>,
{
    let offsets =
        OffsetBuffer::<i64>::from_lengths(cells.iter().map(|c| c.len()));
    let values = PrimitiveArray::<A>::from(
        cells.iter().cloned().flatten().collect::<Vec<_>>(),
    );

    Arc::new(
        LargeListArray::try_new(
            Field::new("unused", values.data_type().clone(), false).into(),
            offsets,
            Arc::new(values),
            None,
        )
        .unwrap(),
    )
}

pub fn from_column(column: &dyn Array) -> Option<FieldData> {
    downcast_primitive_array!(
        column => {
            column.maybe_to_field_data()
        },
        DataType::LargeUtf8 => {
            let column = downcast_array::<LargeStringArray>(column);
            column.maybe_to_field_data()
        }
        DataType::LargeBinary => {
            let column = downcast_array::<LargeBinaryArray>(column);
            column.maybe_to_field_data()
        }
        DataType::LargeList(_) => {
            let column = downcast_array::<LargeListArray>(column);
            column.maybe_to_field_data()
        },
        _ => None
    )
}

trait MaybeToFieldData {
    fn maybe_to_field_data(&self) -> Option<FieldData>;
}

macro_rules! to_field_data {
    ($($primitive:ty),+) => {
        $(
            impl MaybeToFieldData for PrimitiveArray<$primitive> {
                fn maybe_to_field_data(&self) -> Option<FieldData> {
                    Some(self.values().to_vec().into())
                }
            }
        )+
    };
}

macro_rules! not_to_field_data {
    ($($array:ty),+) => {
        $(
            impl MaybeToFieldData for PrimitiveArray<$array> {
                fn maybe_to_field_data(&self) -> Option<FieldData> {
                    None
                }
            }
        )+
    };
}

to_field_data!(
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    UInt8Type,
    UInt16Type,
    UInt32Type,
    UInt64Type,
    Float32Type,
    Float64Type,
    TimestampSecondType,
    TimestampMillisecondType,
    TimestampMicrosecondType,
    TimestampNanosecondType,
    Time64MicrosecondType,
    Time64NanosecondType,
    DurationSecondType,
    DurationMillisecondType,
    DurationMicrosecondType,
    DurationNanosecondType,
    Date64Type
);

not_to_field_data!(
    Float16Type,
    Time32SecondType,
    Time32MillisecondType,
    Date32Type,
    Decimal128Type,
    Decimal256Type,
    IntervalYearMonthType,
    IntervalDayTimeType,
    IntervalMonthDayNanoType
);

impl MaybeToFieldData for LargeStringArray {
    fn maybe_to_field_data(&self) -> Option<FieldData> {
        self.iter()
            .map(|s| s.map(|s| s.bytes().collect::<Vec<_>>()))
            .collect::<Option<Vec<_>>>()
            .map(|v| v.into())
    }
}

impl MaybeToFieldData for LargeBinaryArray {
    fn maybe_to_field_data(&self) -> Option<FieldData> {
        self.iter()
            .map(|s| s.map(|s| s.to_vec()))
            .collect::<Option<Vec<_>>>()
            .map(|v| v.into())
    }
}

impl MaybeToFieldData for LargeListArray {
    fn maybe_to_field_data(&self) -> Option<FieldData> {
        typed_field_data_go!(
            from_column(self.values())?,
            _DT,
            _values,
            {
                Some(
                    self.offsets()
                        .windows(2)
                        .map(|w| _values[w[0] as usize..w[1] as usize].to_vec())
                        .collect::<Vec<_>>()
                        .into(),
                )
            },
            None
        )
    }
}
