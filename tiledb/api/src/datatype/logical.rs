use crate::datatype::physical::PhysicalType;
use crate::datatype::Datatype;
use crate::private::sealed;

/// Trait which provides statically-typed attributes for a TileDB `Datatype`
/// for use with generics.
pub trait LogicalType: crate::private::Sealed {
    const DATA_TYPE: Datatype;

    type PhysicalType: PhysicalType;
}

pub struct UInt8Type {}

impl LogicalType for UInt8Type {
    const DATA_TYPE: Datatype = <u8 as PhysicalType>::DATA_TYPE;

    type PhysicalType = u8;
}

pub struct UInt16Type {}

impl LogicalType for UInt16Type {
    const DATA_TYPE: Datatype = <u16 as PhysicalType>::DATA_TYPE;

    type PhysicalType = u16;
}

pub struct UInt32Type {}

impl LogicalType for UInt32Type {
    const DATA_TYPE: Datatype = <u32 as PhysicalType>::DATA_TYPE;

    type PhysicalType = u32;
}

pub struct UInt64Type {}

impl LogicalType for UInt64Type {
    const DATA_TYPE: Datatype = <u64 as PhysicalType>::DATA_TYPE;

    type PhysicalType = u64;
}

pub struct Int8Type {}

impl LogicalType for Int8Type {
    const DATA_TYPE: Datatype = <i8 as PhysicalType>::DATA_TYPE;

    type PhysicalType = i8;
}

pub struct Int16Type {}

impl LogicalType for Int16Type {
    const DATA_TYPE: Datatype = <i16 as PhysicalType>::DATA_TYPE;

    type PhysicalType = i16;
}

pub struct Int32Type {}

impl LogicalType for Int32Type {
    const DATA_TYPE: Datatype = <i32 as PhysicalType>::DATA_TYPE;

    type PhysicalType = i32;
}

pub struct Int64Type {}

impl LogicalType for Int64Type {
    const DATA_TYPE: Datatype = <i64 as PhysicalType>::DATA_TYPE;

    type PhysicalType = i64;
}

pub struct Float32Type {}

impl LogicalType for Float32Type {
    const DATA_TYPE: Datatype = <f32 as PhysicalType>::DATA_TYPE;

    type PhysicalType = f32;
}

pub struct Float64Type {}

impl LogicalType for Float64Type {
    const DATA_TYPE: Datatype = <f64 as PhysicalType>::DATA_TYPE;

    type PhysicalType = f64;
}

pub struct StringAsciiType {}

impl LogicalType for StringAsciiType {
    const DATA_TYPE: Datatype = Datatype::StringAscii;
    type PhysicalType = u8;
}

pub struct StringUtf8Type {}

impl LogicalType for StringUtf8Type {
    const DATA_TYPE: Datatype = Datatype::StringUtf8;
    type PhysicalType = u8;
}

pub struct StringUtf16Type {}

impl LogicalType for StringUtf16Type {
    const DATA_TYPE: Datatype = Datatype::StringUtf16;
    type PhysicalType = u16;
}
pub struct StringUtf32Type {}

impl LogicalType for StringUtf32Type {
    const DATA_TYPE: Datatype = Datatype::StringUtf32;
    type PhysicalType = u32;
}
pub struct StringUcs2Type {}

impl LogicalType for StringUcs2Type {
    const DATA_TYPE: Datatype = Datatype::StringUcs2;
    type PhysicalType = u16;
}

pub struct StringUcs4Type {}

impl LogicalType for StringUcs4Type {
    const DATA_TYPE: Datatype = Datatype::StringUcs4;
    type PhysicalType = u32;
}

sealed!(UInt8Type, UInt16Type, UInt32Type, UInt64Type);
sealed!(Int8Type, Int16Type, Int32Type, Int64Type);
sealed!(Float32Type, Float64Type);
sealed!(
    StringAsciiType,
    StringUtf8Type,
    StringUtf16Type,
    StringUtf32Type,
    StringUcs2Type,
    StringUcs4Type
);
