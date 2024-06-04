use crate::datatype::{Datatype, LogicalType};
use crate::error::DatatypeErrorKind;
use crate::fn_typed;
use crate::Result as TileDBResult;
use core::slice;
use std::convert::From;

use serde::{Deserialize, Serialize};
use util::option::OptionSubset;

#[derive(Clone, Debug, Deserialize, OptionSubset, PartialEq, Serialize)]
pub enum Value {
    UInt8Value(Vec<u8>),
    UInt16Value(Vec<u16>),
    UInt32Value(Vec<u32>),
    UInt64Value(Vec<u64>),
    Int8Value(Vec<i8>),
    Int16Value(Vec<i16>),
    Int32Value(Vec<i32>),
    Int64Value(Vec<i64>),
    Float32Value(Vec<f32>),
    Float64Value(Vec<f64>),
}

fn get_value_vec<T>(vec: &[T]) -> (*const std::ffi::c_void, usize) {
    let vec_size = vec.len();
    let vec_ptr = vec.as_ptr() as *const std::ffi::c_void;
    (vec_ptr, vec_size)
}

/// Applies a generic expression to the interior of a `Value`.
///
/// # Examples
/// ```
/// use tiledb::metadata::Value;
/// use tiledb::value_go;
///
/// fn truncate(v: &mut Value, len: usize) {
///     value_go!(v, _DT, ref mut v_inner, v_inner.truncate(len));
/// }
///
/// let mut v = Value::UInt64Value(vec![0, 24, 48]);
/// truncate(&mut v, 2);
/// assert_eq!(v, Value::UInt64Value(vec![0, 24]));
/// ```
#[macro_export]
macro_rules! value_go {
    ($valuetype:expr, $typename:ident, $vec:pat, $then: expr) => {{
        use $crate::metadata::Value;
        match $valuetype {
            Value::Int8Value($vec) => {
                type $typename = i8;
                $then
            }
            Value::Int16Value($vec) => {
                type $typename = i16;
                $then
            }
            Value::Int32Value($vec) => {
                type $typename = i32;
                $then
            }
            Value::Int64Value($vec) => {
                type $typename = i64;
                $then
            }
            Value::UInt8Value($vec) => {
                type $typename = u8;
                $then
            }
            Value::UInt16Value($vec) => {
                type $typename = u16;
                $then
            }
            Value::UInt32Value($vec) => {
                type $typename = u32;
                $then
            }
            Value::UInt64Value($vec) => {
                type $typename = u64;
                $then
            }
            Value::Float32Value($vec) => {
                type $typename = f32;
                $then
            }
            Value::Float64Value($vec) => {
                type $typename = f64;
                $then
            }
        }
    }};
}
pub use value_go;

/// Applies a generic expression to the interiors of two `Value`s with matching variants,
/// i.e. with the same physical data type. Typical usage is for comparing the insides of the two
/// `Value`s.
#[macro_export]
macro_rules! value_cmp {
    ($lexpr:expr, $rexpr:expr, $typename:ident, $lpat:pat, $rpat:pat, $same_type:expr, $else:expr) => {{
        use $crate::metadata::Value;
        match ($lexpr, $rexpr) {
            (Value::Int8Value($lpat), Value::Int8Value($rpat)) => {
                type $typename = i8;
                $same_type
            }
            (Value::Int16Value($lpat), Value::Int16Value($rpat)) => {
                type $typename = i16;
                $same_type
            }
            (Value::Int32Value($lpat), Value::Int32Value($rpat)) => {
                type $typename = i32;
                $same_type
            }
            (Value::Int64Value($lpat), Value::Int64Value($rpat)) => {
                type $typename = i64;
                $same_type
            }
            (Value::UInt8Value($lpat), Value::UInt8Value($rpat)) => {
                type $typename = u8;
                $same_type
            }
            (Value::UInt16Value($lpat), Value::UInt16Value($rpat)) => {
                type $typename = u16;
                $same_type
            }
            (Value::UInt32Value($lpat), Value::UInt32Value($rpat)) => {
                type $typename = u32;
                $same_type
            }
            (Value::UInt64Value($lpat), Value::UInt64Value($rpat)) => {
                type $typename = u64;
                $same_type
            }
            (Value::Float32Value($lpat), Value::Float32Value($rpat)) => {
                type $typename = f32;
                $same_type
            }
            (Value::Float64Value($lpat), Value::Float64Value($rpat)) => {
                type $typename = f64;
                $same_type
            }
            _ => $else,
        }
    }};
}

impl Value {
    pub(crate) fn c_vec(&self) -> (*const std::ffi::c_void, usize) {
        value_go!(self, _DT, ref vec, get_value_vec(vec))
    }

    pub fn len(&self) -> usize {
        value_go!(self, _DT, ref v, v.len())
    }

    pub fn is_empty(&self) -> bool {
        value_go!(self, _DT, ref v, v.is_empty())
    }
}

macro_rules! value_impl {
    ($ty:ty, $constructor:expr) => {
        impl From<Vec<$ty>> for Value {
            fn from(vec: Vec<$ty>) -> Self {
                $constructor(vec)
            }
        }
    };
}

value_impl!(i8, Value::Int8Value);
value_impl!(i16, Value::Int16Value);
value_impl!(i32, Value::Int32Value);
value_impl!(i64, Value::Int64Value);
value_impl!(u8, Value::UInt8Value);
value_impl!(u16, Value::UInt16Value);
value_impl!(u32, Value::UInt32Value);
value_impl!(u64, Value::UInt64Value);
value_impl!(f32, Value::Float32Value);
value_impl!(f64, Value::Float64Value);

#[derive(Debug)]
pub struct Metadata {
    pub key: String,
    pub datatype: Datatype,
    pub value: Value,
}

impl Metadata {
    pub fn new<T>(
        key: String,
        datatype: Datatype,
        vec: Vec<T>,
    ) -> TileDBResult<Self>
    where
        Value: From<Vec<T>>,
        T: 'static,
    {
        if !datatype.is_compatible_type::<T>() {
            return Err(crate::error::Error::Datatype(
                DatatypeErrorKind::TypeMismatch {
                    user_type: std::any::type_name::<T>().to_owned(),
                    tiledb_type: datatype,
                },
            ));
        }
        Ok(Metadata {
            key,
            datatype,
            value: Value::from(vec),
        })
    }

    pub(crate) fn new_raw(
        key: String,
        datatype: Datatype,
        vec_ptr: *const std::ffi::c_void,
        vec_size: u32,
    ) -> Self {
        let value = fn_typed!(datatype, LT, {
            type DT = <LT as LogicalType>::PhysicalType;
            let vec_slice = unsafe {
                slice::from_raw_parts(
                    vec_ptr as *const DT,
                    vec_size.try_into().unwrap(),
                )
            };
            let vec_value: Vec<DT> = vec_slice.to_vec();
            Value::from(vec_value)
        });

        Metadata {
            key,
            datatype,
            value,
        }
    }

    pub(crate) fn c_data(
        &self,
    ) -> (usize, *const std::ffi::c_void, ffi::tiledb_datatype_t) {
        let (vec_ptr, vec_size) = self.value.c_vec();
        let c_datatype = self.datatype.capi_enum();
        (vec_size, vec_ptr, c_datatype)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_cmp() {
        let v1 = Value::UInt8Value(vec![0, 1, 2]);
        value_cmp!(
            &v1,
            &v1,
            _DT,
            ref vl,
            ref vr,
            assert_eq!(vl, vr),
            unreachable!()
        );

        let v2 = Value::Float64Value(vec![0f64, 1f64, 2f64]);
        value_cmp!(
            &v1,
            &v1,
            _DT,
            ref vl,
            ref vr,
            assert_eq!(vl, vr),
            unreachable!()
        );

        value_cmp!(&v1, &v2, _DT, _, _, unreachable!(), {});
    }
}
