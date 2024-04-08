use std::fmt::{Debug, Formatter, Result as FmtResult};

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::datatype::Datatype;

pub type NonEmptyDomain = Vec<TypedRange>;
pub type MinimumBoundingRectangle = Vec<TypedRange>;

#[derive(Clone, Deserialize, Serialize)]
pub enum Range {
    UInt8Range(u8, u8),
    UInt16Range(u16, u16),
    UInt32Range(u32, u32),
    UInt64Range(u64, u64),
    Int8Range(i8, i8),
    Int16Range(i16, i16),
    Int32Range(i32, i32),
    Int64Range(i64, i64),
    Float32Range(f32, f32),
    Float64Range(f64, f64),

    VarUInt8Range(Box<[u8]>, Box<[u8]>),
    VarUInt16Range(Box<[u16]>, Box<[u16]>),
    VarUInt32Range(Box<[u32]>, Box<[u32]>),
    VarUInt64Range(Box<[u64]>, Box<[u64]>),
    VarInt8Range(Box<[i8]>, Box<[i8]>),
    VarInt16Range(Box<[i16]>, Box<[i16]>),
    VarInt32Range(Box<[i32]>, Box<[i32]>),
    VarInt64Range(Box<[i64]>, Box<[i64]>),
    VarFloat32Range(Box<[f32]>, Box<[f32]>),
    VarFloat64Range(Box<[f64]>, Box<[f64]>),
}

impl Debug for Range {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}", json!(self))
    }
}

impl From<&[u8; 2]> for Range {
    fn from(val: &[u8; 2]) -> Range {
        Range::UInt8Range(val[0], val[1])
    }
}

impl From<&[u16; 2]> for Range {
    fn from(val: &[u16; 2]) -> Range {
        Range::UInt16Range(val[0], val[1])
    }
}

impl From<&[u32; 2]> for Range {
    fn from(val: &[u32; 2]) -> Range {
        Range::UInt32Range(val[0], val[1])
    }
}

impl From<&[u64; 2]> for Range {
    fn from(val: &[u64; 2]) -> Range {
        Range::UInt64Range(val[0], val[1])
    }
}

impl From<&[i8; 2]> for Range {
    fn from(val: &[i8; 2]) -> Range {
        Range::Int8Range(val[0], val[1])
    }
}

impl From<&[i16; 2]> for Range {
    fn from(val: &[i16; 2]) -> Range {
        Range::Int16Range(val[0], val[1])
    }
}

impl From<&[i32; 2]> for Range {
    fn from(val: &[i32; 2]) -> Range {
        Range::Int32Range(val[0], val[1])
    }
}

impl From<&[i64; 2]> for Range {
    fn from(val: &[i64; 2]) -> Range {
        Range::Int64Range(val[0], val[1])
    }
}

impl From<&[f32; 2]> for Range {
    fn from(val: &[f32; 2]) -> Range {
        Range::Float32Range(val[0], val[1])
    }
}

impl From<&[f64; 2]> for Range {
    fn from(val: &[f64; 2]) -> Range {
        Range::Float64Range(val[0], val[1])
    }
}

impl From<(Box<[u8]>, Box<[u8]>)> for Range {
    fn from(val: (Box<[u8]>, Box<[u8]>)) -> Range {
        Range::VarUInt8Range(val.0, val.1)
    }
}

impl From<(Box<[u16]>, Box<[u16]>)> for Range {
    fn from(val: (Box<[u16]>, Box<[u16]>)) -> Range {
        Range::VarUInt16Range(val.0, val.1)
    }
}

impl From<(Box<[u32]>, Box<[u32]>)> for Range {
    fn from(val: (Box<[u32]>, Box<[u32]>)) -> Range {
        Range::VarUInt32Range(val.0, val.1)
    }
}

impl From<(Box<[u64]>, Box<[u64]>)> for Range {
    fn from(val: (Box<[u64]>, Box<[u64]>)) -> Range {
        Range::VarUInt64Range(val.0, val.1)
    }
}

impl From<(Box<[i8]>, Box<[i8]>)> for Range {
    fn from(val: (Box<[i8]>, Box<[i8]>)) -> Range {
        Range::VarInt8Range(val.0, val.1)
    }
}

impl From<(Box<[i16]>, Box<[i16]>)> for Range {
    fn from(val: (Box<[i16]>, Box<[i16]>)) -> Range {
        Range::VarInt16Range(val.0, val.1)
    }
}

impl From<(Box<[i32]>, Box<[i32]>)> for Range {
    fn from(val: (Box<[i32]>, Box<[i32]>)) -> Range {
        Range::VarInt32Range(val.0, val.1)
    }
}

impl From<(Box<[i64]>, Box<[i64]>)> for Range {
    fn from(val: (Box<[i64]>, Box<[i64]>)) -> Range {
        Range::VarInt64Range(val.0, val.1)
    }
}

impl From<(Box<[f32]>, Box<[f32]>)> for Range {
    fn from(val: (Box<[f32]>, Box<[f32]>)) -> Range {
        Range::VarFloat32Range(val.0, val.1)
    }
}

impl From<(Box<[f64]>, Box<[f64]>)> for Range {
    fn from(val: (Box<[f64]>, Box<[f64]>)) -> Range {
        Range::VarFloat64Range(val.0, val.1)
    }
}

#[derive(Serialize, Deserialize)]
pub struct TypedRange {
    pub datatype: Datatype,
    pub range: Range,
}

impl TypedRange {
    pub fn new(datatype: Datatype, range: Range) -> Self {
        Self { datatype, range }
    }
}

impl Debug for TypedRange {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}", json!(self))
    }
}
