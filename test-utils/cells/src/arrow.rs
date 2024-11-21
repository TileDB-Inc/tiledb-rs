use std::sync::Arc;

use arrow_array::types::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{
    Array, ArrowPrimitiveType, LargeListArray, PrimitiveArray, RecordBatch,
};
use arrow_buffer::{ArrowNativeType, OffsetBuffer};
use arrow_schema::{DataType, Field, Fields, Schema};

use crate::{Cells, FieldData};

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

fn to_column(fdata: &FieldData) -> Arc<dyn Array> {
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
            Field::new(
                "unused",
                DataType::new_large_list(values.data_type().clone(), false),
                false,
            )
            .into(),
            offsets,
            Arc::new(values),
            None,
        )
        .unwrap(),
    )
}
