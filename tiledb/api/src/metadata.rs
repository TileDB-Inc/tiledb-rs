use crate::{datatype::Datatype, fn_typed};
use core::slice;

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

fn get_value_vec<T>(vec: &Vec<T>) -> (*const std::ffi::c_void, usize) {
    let vec_size = vec.len();
    let vec_ptr = vec.as_ptr() as *const std::ffi::c_void;
    (vec_ptr, vec_size)
}

impl Value {
    pub fn c_vec(
        &self,
    ) -> (*const std::ffi::c_void, usize) {
        match self {
            Value::Int8Value(vec) => get_value_vec(vec),
            Value::Int16Value(vec) => get_value_vec(vec),
            Value::Int32Value(vec) => get_value_vec(vec),
            Value::Int64Value(vec) => get_value_vec(vec),
            Value::UInt8Value(vec) => get_value_vec(vec),
            Value::UInt16Value(vec) => get_value_vec(vec),
            Value::UInt32Value(vec) => get_value_vec(vec),
            Value::UInt64Value(vec) => get_value_vec(vec),
            Value::Float32Value(vec) => get_value_vec(vec),
            Value::Float64Value(vec) => get_value_vec(vec),
        }
    }
}

trait ValueType {
    fn get_value(vec : Vec<Self>) -> Value where Self: Sized;
}

impl ValueType for i8 {
    fn get_value(vec : Vec<Self>) -> Value where Self: Sized {
        Value::Int8Value(vec)
    }
}

impl ValueType for i16 {
    fn get_value(vec : Vec<Self>) -> Value where Self: Sized {
        Value::Int16Value(vec)
    }
}

impl ValueType for i32 {
    fn get_value(vec : Vec<Self>) -> Value where Self: Sized {
        Value::Int32Value(vec)
    }
}

impl ValueType for i64 {
    fn get_value(vec : Vec<Self>) -> Value where Self: Sized {
        Value::Int64Value(vec)
    }
}

impl ValueType for u8 {
    fn get_value(vec : Vec<Self>) -> Value where Self: Sized {
        Value::UInt8Value(vec)
    }
}

impl ValueType for u16 {
    fn get_value(vec : Vec<Self>) -> Value where Self: Sized {
        Value::UInt16Value(vec)
    }
}

impl ValueType for u32 {
    fn get_value(vec : Vec<Self>) -> Value where Self: Sized {
        Value::UInt32Value(vec)
    }
}

impl ValueType for u64 {
    fn get_value(vec : Vec<Self>) -> Value where Self: Sized {
        Value::UInt64Value(vec)
    }
}

impl ValueType for f32 {
    fn get_value(vec : Vec<Self>) -> Value where Self: Sized {
        Value::Float32Value(vec)
    }
}

impl ValueType for f64 {
    fn get_value(vec : Vec<Self>) -> Value where Self: Sized {
        Value::Float64Value(vec)
    }
}

pub struct Metadata {
    pub key: String,
    pub datatype: Datatype,
    pub value: Value,
}

impl Metadata {
    pub fn new(
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
            let vec_value : Vec<DT> = vec_slice.to_vec();
            DT::get_value(vec_value)
        });

        Metadata {
            key,
            datatype,
            value
        }
    }

    pub fn c_data(&self) -> (usize, *const std::ffi::c_void, ffi::tiledb_datatype_t) 
    {
        let (vec_ptr, vec_size ) = self.value.c_vec();
        let c_datatype = self.datatype.capi_enum();
        (vec_size, vec_ptr, c_datatype)
    }
}
