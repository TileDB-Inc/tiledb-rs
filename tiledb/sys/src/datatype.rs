use std::any::TypeId;
use std::fmt::{Display, Formatter, Result as FmtResult};

use crate::constants::TILEDB_OK;
use crate::types::capi_return_t;

extern "C" {
    pub fn tiledb_datatype_to_str(
        datatype: u32,
        str_: *mut *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_datatype_from_str(
        str_: *const ::std::os::raw::c_char,
        datatype: *mut u32,
    ) -> capi_return_t;

    pub fn tiledb_datatype_size(type_: u32) -> u64;
}

#[allow(non_snake_case)]
pub type tiledb_datatype_t = ::std::os::raw::c_uint;

// When I find the time, I should come back and turn these into a macro
// so that we can auto-generate the Datatype::from_u32 generation.

#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(u64)]
pub enum Datatype {
    #[doc = " 32-bit signed integer"]
    Int32 = 0,
    #[doc = " 64-bit signed integer"]
    Int64 = 1,
    #[doc = " 32-bit floating point value"]
    Float32 = 2,
    #[doc = " 64-bit floating point value"]
    Float64 = 3,
    #[doc = " Character"]
    Char = 4,
    #[doc = " 8-bit signed integer"]
    Int8 = 5,
    #[doc = " 8-bit unsigned integer"]
    UInt8 = 6,
    #[doc = " 16-bit signed integer"]
    Int16 = 7,
    #[doc = " 16-bit unsigned integer"]
    UInt16 = 8,
    #[doc = " 32-bit unsigned integer"]
    UInt32 = 9,
    #[doc = " 64-bit unsigned integer"]
    UInt64 = 10,
    #[doc = " ASCII string"]
    StringAscii = 11,
    #[doc = " UTF-8 string"]
    StringUtf8 = 12,
    #[doc = " UTF-16 string"]
    StringUtf16 = 13,
    #[doc = " UTF-32 string"]
    StringUtf32 = 14,
    #[doc = " UCS2 string"]
    StringUcs2 = 15,
    #[doc = " UCS4 string"]
    StringUcs4 = 16,
    #[doc = " This can be any datatype. Must store (type tag, value) pairs."]
    Any = 17,
    #[doc = " Datetime with year resolution"]
    DateTimeYear = 18,
    #[doc = " Datetime with month resolution"]
    DateTimeMonth = 19,
    #[doc = " Datetime with week resolution"]
    DateTimeWeek = 20,
    #[doc = " Datetime with day resolution"]
    DateTimeDay = 21,
    #[doc = " Datetime with hour resolution"]
    DateTimeHour = 22,
    #[doc = " Datetime with minute resolution"]
    DateTimeMinute = 23,
    #[doc = " Datetime with second resolution"]
    DateTimeSecond = 24,
    #[doc = " Datetime with millisecond resolution"]
    DateTimeMillisecond = 25,
    #[doc = " Datetime with microsecond resolution"]
    DateTimeMicrosecond = 26,
    #[doc = " Datetime with nanosecond resolution"]
    DateTimeNanosecond = 27,
    #[doc = " Datetime with picosecond resolution"]
    DateTimePicosecond = 28,
    #[doc = " Datetime with femtosecond resolution"]
    DateTimeFemtosecond = 29,
    #[doc = " Datetime with attosecond resolution"]
    DateTimeAttosecond = 30,
    #[doc = " Time with hour resolution"]
    TimeHour = 31,
    #[doc = " Time with minute resolution"]
    TimeMinute = 32,
    #[doc = " Time with second resolution"]
    TimeSecond = 33,
    #[doc = " Time with millisecond resolution"]
    TimeMillisecond = 34,
    #[doc = " Time with microsecond resolution"]
    TimeMicrosecond = 35,
    #[doc = " Time with nanosecond resolution"]
    TimeNanosecond = 36,
    #[doc = " Time with picosecond resolution"]
    TimePicosecond = 37,
    #[doc = " Time with femtosecond resolution"]
    TimeFemtosecond = 38,
    #[doc = " Time with attosecond resolution"]
    TimeAttosecond = 39,
    #[doc = " std::byte"]
    Blob = 40,
    #[doc = " Boolean"]
    Boolean = 41,
    #[doc = " Geometry data in well-known binary (WKB) format, stored as std::byte"]
    GeometryWkb = 42,
    #[doc = " Geometry data in well-known text (WKT) format, stored as std::byte"]
    GeometryWkt = 43,
}

impl Datatype {
    pub fn size(&self) -> u64 {
        let copy = (*self).clone();
        unsafe { tiledb_datatype_size(copy as tiledb_datatype_t) }
    }

    /// TODO: this should not be exposed outside of the tiledb-rs library
    pub fn capi_enum(&self) -> tiledb_datatype_t {
        *self as tiledb_datatype_t
    }

    pub fn from_capi_enum(c_datatype: tiledb_datatype_t) -> Self {
        Self::from_u32(c_datatype).unwrap()
    }

    pub fn to_string(&self) -> Option<String> {
        let copy = (*self).clone();
        let c_dtype = copy as tiledb_datatype_t;
        let mut c_str = std::ptr::null::<std::os::raw::c_char>();
        let res = unsafe { tiledb_datatype_to_str(c_dtype, &mut c_str) };
        if res == TILEDB_OK {
            let c_msg = unsafe { std::ffi::CStr::from_ptr(c_str) };
            Some(String::from(c_msg.to_string_lossy()))
        } else {
            None
        }
    }

    pub fn from_string(dtype: &str) -> Option<Datatype> {
        let c_dtype =
            std::ffi::CString::new(dtype).expect("Error creating CString");
        let mut c_ret: u32 = 0;
        let res = unsafe {
            tiledb_datatype_from_str(c_dtype.as_c_str().as_ptr(), &mut c_ret)
        };

        if res == TILEDB_OK {
            Datatype::from_u32(c_ret)
        } else {
            None
        }
    }

    pub fn from_u32(dtype: u32) -> Option<Datatype> {
        match dtype {
            0 => Some(Datatype::Int32),
            1 => Some(Datatype::Int64),
            2 => Some(Datatype::Float32),
            3 => Some(Datatype::Float64),
            4 => Some(Datatype::Char),
            5 => Some(Datatype::Int8),
            6 => Some(Datatype::UInt8),
            7 => Some(Datatype::Int16),
            8 => Some(Datatype::UInt16),
            9 => Some(Datatype::UInt32),
            10 => Some(Datatype::UInt64),
            11 => Some(Datatype::StringAscii),
            12 => Some(Datatype::StringUtf8),
            13 => Some(Datatype::StringUtf16),
            14 => Some(Datatype::StringUtf32),
            15 => Some(Datatype::StringUcs2),
            16 => Some(Datatype::StringUcs4),
            17 => Some(Datatype::Any),
            18 => Some(Datatype::DateTimeYear),
            19 => Some(Datatype::DateTimeMonth),
            20 => Some(Datatype::DateTimeWeek),
            21 => Some(Datatype::DateTimeDay),
            22 => Some(Datatype::DateTimeHour),
            23 => Some(Datatype::DateTimeMinute),
            24 => Some(Datatype::DateTimeSecond),
            25 => Some(Datatype::DateTimeMillisecond),
            26 => Some(Datatype::DateTimeMicrosecond),
            27 => Some(Datatype::DateTimeNanosecond),
            28 => Some(Datatype::DateTimePicosecond),
            29 => Some(Datatype::DateTimeFemtosecond),
            30 => Some(Datatype::DateTimeAttosecond),
            31 => Some(Datatype::TimeHour),
            32 => Some(Datatype::TimeMinute),
            33 => Some(Datatype::TimeSecond),
            34 => Some(Datatype::TimeMillisecond),
            35 => Some(Datatype::TimeMicrosecond),
            36 => Some(Datatype::TimeNanosecond),
            37 => Some(Datatype::TimePicosecond),
            38 => Some(Datatype::TimeFemtosecond),
            39 => Some(Datatype::TimeAttosecond),
            40 => Some(Datatype::Blob),
            41 => Some(Datatype::Boolean),
            42 => Some(Datatype::GeometryWkb),
            43 => Some(Datatype::GeometryWkt),
            _ => None,
        }
    }

    pub fn is_compatible_type<T: 'static>(&self) -> bool {
        let tid = TypeId::of::<T>();
        if tid == TypeId::of::<f32>() {
            match self {
                Datatype::Float32 => true,
                _ => false,
            }
        } else if tid == TypeId::of::<f64>() {
            match self {
                Datatype::Float64 => true,
                _ => false,
            }
        } else if tid == TypeId::of::<i8>() {
            match self {
                Datatype::Char => true,
                Datatype::Int8 => true,
                _ => false,
            }
        } else if tid == TypeId::of::<u8>() {
            match self {
                Datatype::Any => true,
                Datatype::Blob => true,
                Datatype::Boolean => true,
                Datatype::GeometryWkb => true,
                Datatype::GeometryWkt => true,
                Datatype::StringAscii => true,
                Datatype::StringUtf8 => true,
                Datatype::UInt8 => true,
                _ => false,
            }
        } else if tid == TypeId::of::<i16>() {
            match self {
                Datatype::Int16 => true,
                _ => false,
            }
        } else if tid == TypeId::of::<u16>() {
            match self {
                Datatype::StringUtf16 => true,
                Datatype::StringUcs2 => true,
                Datatype::UInt16 => true,
                _ => false,
            }
        } else if tid == TypeId::of::<i32>() {
            match self {
                Datatype::Int32 => true,
                _ => false,
            }
        } else if tid == TypeId::of::<u32>() {
            match self {
                Datatype::StringUtf32 => true,
                Datatype::StringUcs4 => true,
                Datatype::UInt32 => true,
                _ => false,
            }
        } else if tid == TypeId::of::<i64>() {
            match self {
                Datatype::Int64 => true,
                Datatype::DateTimeYear => true,
                Datatype::DateTimeMonth => true,
                Datatype::DateTimeWeek => true,
                Datatype::DateTimeDay => true,
                Datatype::DateTimeHour => true,
                Datatype::DateTimeMinute => true,
                Datatype::DateTimeSecond => true,
                Datatype::DateTimeMillisecond => true,
                Datatype::DateTimeMicrosecond => true,
                Datatype::DateTimeNanosecond => true,
                Datatype::DateTimePicosecond => true,
                Datatype::DateTimeFemtosecond => true,
                Datatype::DateTimeAttosecond => true,
                Datatype::TimeHour => true,
                Datatype::TimeMinute => true,
                Datatype::TimeSecond => true,
                Datatype::TimeMillisecond => true,
                Datatype::TimeMicrosecond => true,
                Datatype::TimeNanosecond => true,
                Datatype::TimePicosecond => true,
                Datatype::TimeFemtosecond => true,
                Datatype::TimeAttosecond => true,
                _ => false,
            }
        } else if tid == TypeId::of::<u64>() {
            match self {
                Datatype::UInt64 => true,
                _ => false,
            }
        } else {
            false
        }
    }
}

impl Display for Datatype {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(
            f,
            "{}",
            match self.to_string() {
                Some(s) => s,
                None => String::from("<UNKNOWN DATA TYPE>"),
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datatype_test() {
        for i in 0..256 {
            println!("I: {}", i);
            if i <= 43 {
                let dt = Datatype::from_u32(i as u32)
                    .expect("Error converting value to Datatype");
                assert_ne!(
                    format!("{}", dt),
                    "<UNKNOWN DATA TYPE>".to_string()
                );
                assert!(check_valid(&dt));
            } else {
                assert!(Datatype::from_u32(i as u32).is_none());
            }
        }
    }

    fn check_valid(dt: &Datatype) -> bool {
        let mut count = 0;

        if dt.is_compatible_type::<f32>() {
            count += 1;
        }

        if dt.is_compatible_type::<f64>() {
            count += 1;
        }

        if dt.is_compatible_type::<i8>() {
            count += 1;
        }

        if dt.is_compatible_type::<u8>() {
            count += 1;
        }

        if dt.is_compatible_type::<i16>() {
            count += 1;
        }

        if dt.is_compatible_type::<u16>() {
            count += 1;
        }

        if dt.is_compatible_type::<i32>() {
            count += 1;
        }

        if dt.is_compatible_type::<u32>() {
            count += 1;
        }

        if dt.is_compatible_type::<i64>() {
            count += 1;
        }

        if dt.is_compatible_type::<u64>() {
            count += 1;
        }

        count == 1
    }
}
