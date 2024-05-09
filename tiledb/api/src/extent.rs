use serde::{Deserialize, Serialize};

use util::option::OptionSubset;

#[derive(Clone, Debug, OptionSubset, Deserialize, PartialEq, Serialize)]
pub enum Extent {
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Invalid,
}

macro_rules! extent_from {
    ($($V:ident : $U:ty),+) => {
        $(
            impl From<$U> for Extent {
                fn from(value: $U) -> Extent {
                    Extent::$V(value)
                }
            }
        )+
    }
}

extent_from!(UInt8: u8, UInt16: u16, UInt32: u32, UInt64: u64);
extent_from!(Int8: i8, Int16: i16, Int32: i32, Int64: i64);

impl From<f32> for Extent {
    fn from(_: f32) -> Extent {
        Extent::Invalid
    }
}

impl From<f64> for Extent {
    fn from(_: f64) -> Extent {
        Extent::Invalid
    }
}

#[macro_export]
macro_rules! extent_go {
    ($expr:expr, $DT:ident, $value:pat, $then:expr) => {
        match $expr {
            Extent::Int8($value) => {
                type $DT = i8;
                $then
            }
            Extent::Int16($value) => {
                type $DT = i16;
                $then
            }
            Extent::Int32($value) => {
                type $DT = i32;
                $then
            }
            Extent::Int64($value) => {
                type $DT = i64;
                $then
            }
            Extent::UInt8($value) => {
                type $DT = u8;
                $then
            }
            Extent::UInt16($value) => {
                type $DT = u16;
                $then
            }
            Extent::UInt32($value) => {
                type $DT = u32;
                $then
            }
            Extent::UInt64($value) => {
                type $DT = u64;
                $then
            }
            Extent::Invalid => {
                panic!("Unhandled invalid extent");
            }
        }
    };
}
