use crate::datatype::{Datatype, LogicalType};
use crate::error::DatatypeErrorKind;
use crate::fn_typed;
use crate::Result as TileDBResult;
use core::slice;
use std::convert::From;

use crate::typed_enum;

typed_enum!(Vec<T> => #[derive(Debug, PartialEq)] Value);

fn get_value_vec<T>(vec: &[T]) -> (*const std::ffi::c_void, usize) {
    let vec_size = vec.len();
    let vec_ptr = vec.as_ptr() as *const std::ffi::c_void;
    (vec_ptr, vec_size)
}

impl Value {
    pub fn c_vec(&self) -> (*const std::ffi::c_void, usize) {
        value_go!(self, _DT, vec, get_value_vec(vec))
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

value_impl!(i8, Value::Int8);
value_impl!(i16, Value::Int16);
value_impl!(i32, Value::Int32);
value_impl!(i64, Value::Int64);
value_impl!(u8, Value::UInt8);
value_impl!(u16, Value::UInt16);
value_impl!(u32, Value::UInt32);
value_impl!(u64, Value::UInt64);
value_impl!(f32, Value::Float32);
value_impl!(f64, Value::Float64);

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
                    user_type: std::any::type_name::<T>(),
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
