use crate::Result as TileDBResult;
use crate::{datatype::Datatype, fn_typed};
use anyhow::anyhow;
use core::slice;
use std::convert::From;

#[derive(Debug, PartialEq)]
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
    //StringAsciiValue(CString),
    //BooleanValue(bool),
    // maybe blobs?
}

fn get_value_vec<T>(vec: &[T]) -> (*const std::ffi::c_void, usize) {
    let vec_size = vec.len();
    let vec_ptr = vec.as_ptr() as *const std::ffi::c_void;
    (vec_ptr, vec_size)
}

macro_rules! value_typed {
    ($valuetype:expr, $typename:ident, $vec:ident, $then: expr) => {
        match $valuetype {
            Value::Int8Value(ref $vec) => {
                type $typename = i8;
                $then
            }
            Value::Int16Value(ref $vec) => {
                type $typename = i16;
                $then
            }
            Value::Int32Value(ref $vec) => {
                type $typename = i32;
                $then
            }
            Value::Int64Value(ref $vec) => {
                type $typename = i64;
                $then
            }
            Value::UInt8Value(ref $vec) => {
                type $typename = u8;
                $then
            }
            Value::UInt16Value(ref $vec) => {
                type $typename = u16;
                $then
            }
            Value::UInt32Value(ref $vec) => {
                type $typename = u32;
                $then
            }
            Value::UInt64Value(ref $vec) => {
                type $typename = u64;
                $then
            }
            Value::Float32Value(ref $vec) => {
                type $typename = f32;
                $then
            }
            Value::Float64Value(ref $vec) => {
                type $typename = f64;
                $then
            }
        }
    };
}

impl Value {
    pub fn c_vec(&self) -> (*const std::ffi::c_void, usize) {
        value_typed!(self, _DT, vec, get_value_vec(vec))
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
    {
        if std::mem::size_of::<T>() != datatype.size() as usize {
            return Err(crate::error::Error::InvalidArgument(anyhow!(
                "datatype size and T size are not equal"
            )));
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
        let value = fn_typed!(datatype, DT, {
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
