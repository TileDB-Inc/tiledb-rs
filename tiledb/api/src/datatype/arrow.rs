use arrow::datatypes::{ArrowNativeType, ArrowPrimitiveType};

pub trait ArrowPrimitiveTypeNative: ArrowNativeType {
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
