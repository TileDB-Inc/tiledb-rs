use core::slice;
use crate::datatype::Datatype;

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
    //StringUtf8Value(CString),
    //StringUtf16Value(CString),
    //StringUtf32Value(CString),
    //StringUcs2Value(CString),
    //StringUcs4Value(CString),
    //AnyValue,
    //DateTimeYearValue(i64),
    //DateTimeMonthValue(i64),
    //DateTimeWeekValue(i64),
    //DateTimeDayValue(i64),
    //DateTimeHourValue(i64),
    //DateTimeMinuteValue(i64),
    //DateTimeSecondValue(i64),
    //DateTimeMillisecondValue(i64),
    //DateTimeMicrosecondValue(i64),
    //DateTimeNanosecondValue(i64),
    //DateTimePicosecondValue(i64),
    //DateTimeFemtosecondValue(i64),
    //DateTimeAttosecondValue(i64),
    //TimeHourValue(i64),
    //TimeMinuteValue(i64),
    //TimeSecondValue(i64),
    //TimeMillisecondValue(i64),
    //TimeMicrosecondValue(i64),
    //TimeNanosecondValue(i64),
    //TimePicosecondValue(i64),
    //TimeFemtosecondValue(i64),
    //TimeAttosecondValue(i64),
    //BlobValue(Box<[u8]>),
    //BooleanValue(bool),
    //GeometryWkbValue(),
    //GeometryWktValue(),
}

impl Value {
    pub fn c_vec(&self) -> (usize, *const std::ffi::c_void, ffi::tiledb_datatype_t) {
        match self {
            Value::Int8Value(vec) => {
                let vec_size = vec.len();
                let vec_ptr = vec.as_ptr() as *const std::ffi::c_void;
                let datatype = ffi::tiledb_datatype_t_TILEDB_INT8;
                (vec_size, vec_ptr, datatype)
            }
            Value::Int16Value(vec) => {
                let vec_size = vec.len();
                let vec_ptr = vec.as_ptr() as *const std::ffi::c_void;
                let datatype = ffi::tiledb_datatype_t_TILEDB_INT16;
                (vec_size, vec_ptr, datatype)
            }
            Value::Int32Value(vec) => {
                let vec_size = vec.len();
                let vec_ptr = vec.as_ptr() as *const std::ffi::c_void;
                let datatype = ffi::tiledb_datatype_t_TILEDB_INT32;
                (vec_size, vec_ptr, datatype)
            }
            Value::Int64Value(vec) => {
                let vec_size = vec.len();
                let vec_ptr = vec.as_ptr() as *const std::ffi::c_void;
                let datatype = ffi::tiledb_datatype_t_TILEDB_INT64;
                (vec_size, vec_ptr, datatype)
            }
            Value::UInt8Value(vec) => {
                let vec_size = vec.len();
                let vec_ptr = vec.as_ptr() as *const std::ffi::c_void;
                let datatype = ffi::tiledb_datatype_t_TILEDB_UINT8;
                (vec_size, vec_ptr, datatype)
            }
            Value::UInt16Value(vec) => {
                let vec_size = vec.len();
                let vec_ptr = vec.as_ptr() as *const std::ffi::c_void;
                let datatype = ffi::tiledb_datatype_t_TILEDB_UINT16;
                (vec_size, vec_ptr, datatype)
            }
            Value::UInt32Value(vec) => {
                let vec_size = vec.len();
                let vec_ptr = vec.as_ptr() as *const std::ffi::c_void;
                let datatype = ffi::tiledb_datatype_t_TILEDB_UINT32;
                (vec_size, vec_ptr, datatype)
            }
            Value::UInt64Value(vec) => {
                let vec_size = vec.len();
                let vec_ptr = vec.as_ptr() as *const std::ffi::c_void;
                let datatype = ffi::tiledb_datatype_t_TILEDB_UINT64;
                (vec_size, vec_ptr, datatype)
            }
            Value::Float32Value(vec) => {
                let vec_size = vec.len();
                let vec_ptr = vec.as_ptr() as *const std::ffi::c_void;
                let datatype = ffi::tiledb_datatype_t_TILEDB_FLOAT32;
                (vec_size, vec_ptr, datatype)
            }
            Value::Float64Value(vec) => {
                let vec_size = vec.len();
                let vec_ptr = vec.as_ptr() as *const std::ffi::c_void;
                let datatype = ffi::tiledb_datatype_t_TILEDB_FLOAT64;
                (vec_size, vec_ptr, datatype)
            }
        }
    }
}

pub struct Metadata {
    pub key : String,
    pub datatype : Datatype,
    pub value : Value
}

impl Metadata {
    pub fn new(key : String, datatype : Datatype, vec_ptr : *const std::ffi::c_void, vec_size : u32) -> Self {
        let value = match datatype {
            Datatype::Int8 => {
                let vec_slice = unsafe {slice::from_raw_parts(vec_ptr as *const i8, vec_size.try_into().unwrap())};
                Value::Int8Value(vec_slice.to_vec())
            },
            Datatype::Int16 => {
                let vec_slice = unsafe {slice::from_raw_parts(vec_ptr as *const i16, vec_size.try_into().unwrap())};
                Value::Int16Value(vec_slice.to_vec())
            },
            Datatype::Int32 => {
                let vec_slice = unsafe {slice::from_raw_parts(vec_ptr as *const i32, vec_size.try_into().unwrap())};
                Value::Int32Value(vec_slice.to_vec())
            },
            Datatype::Int64 => {
                let vec_slice = unsafe {slice::from_raw_parts(vec_ptr as *const i64, vec_size.try_into().unwrap())};
                Value::Int64Value(vec_slice.to_vec())
            },
            Datatype::UInt8 => {
                let vec_slice = unsafe {slice::from_raw_parts(vec_ptr as *const u8, vec_size.try_into().unwrap())};
                Value::UInt8Value(vec_slice.to_vec())
            },
            Datatype::UInt16 => {
                let vec_slice = unsafe {slice::from_raw_parts(vec_ptr as *const u16, vec_size.try_into().unwrap())};
                Value::UInt16Value(vec_slice.to_vec())
            },
            Datatype::UInt32 => {
                let vec_slice = unsafe {slice::from_raw_parts(vec_ptr as *const u32, vec_size.try_into().unwrap())};
                Value::UInt32Value(vec_slice.to_vec())
            },
            Datatype::UInt64 => {
                let vec_slice = unsafe {slice::from_raw_parts(vec_ptr as *const u64, vec_size.try_into().unwrap())};
                Value::UInt64Value(vec_slice.to_vec())
            },
            Datatype::Float32 => {
                let vec_slice = unsafe {slice::from_raw_parts(vec_ptr as *const f32, vec_size.try_into().unwrap())};
                Value::Float32Value(vec_slice.to_vec())
            }
            Datatype::Float64 => {
                let vec_slice = unsafe {slice::from_raw_parts(vec_ptr as *const f64, vec_size.try_into().unwrap())};
                Value::Float64Value(vec_slice.to_vec())
            }
            _ => unimplemented!()
        };

        Metadata {
            key: key,
            datatype : datatype,
            value : value
        }
    }
}