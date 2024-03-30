use std::fmt::{Debug, Display, Formatter, Result as FmtResult};

use serde::{Deserialize, Serialize};

use crate::error::DatatypeErrorKind;
use crate::Result as TileDBResult;

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[repr(u64)]
pub enum Datatype {
    #[doc = " 32-bit signed integer"]
    Int32,
    #[doc = " 64-bit signed integer"]
    Int64,
    #[doc = " 32-bit floating point value"]
    Float32,
    #[doc = " 64-bit floating point value"]
    Float64,
    #[doc = " Character"]
    Char,
    #[doc = " 8-bit signed integer"]
    Int8,
    #[doc = " 8-bit unsigned integer"]
    UInt8,
    #[doc = " 16-bit signed integer"]
    Int16,
    #[doc = " 16-bit unsigned integer"]
    UInt16,
    #[doc = " 32-bit unsigned integer"]
    UInt32,
    #[doc = " 64-bit unsigned integer"]
    UInt64,
    #[doc = " ASCII string"]
    StringAscii,
    #[doc = " UTF-8 string"]
    StringUtf8,
    #[doc = " UTF-16 string"]
    StringUtf16,
    #[doc = " UTF-32 string"]
    StringUtf32,
    #[doc = " UCS2 string"]
    StringUcs2,
    #[doc = " UCS4 string"]
    StringUcs4,
    #[doc = " This can be any datatype. Must store (type tag, value) pairs."]
    Any,
    #[doc = " DateTime with year resolution"]
    DateTimeYear,
    #[doc = " DateTime with month resolution"]
    DateTimeMonth,
    #[doc = " DateTime with week resolution"]
    DateTimeWeek,
    #[doc = " DateTime with day resolution"]
    DateTimeDay,
    #[doc = " DateTime with hour resolution"]
    DateTimeHour,
    #[doc = " DateTime with minute resolution"]
    DateTimeMinute,
    #[doc = " DateTime with second resolution"]
    DateTimeSecond,
    #[doc = " DateTime with millisecond resolution"]
    DateTimeMillisecond,
    #[doc = " DateTime with microsecond resolution"]
    DateTimeMicrosecond,
    #[doc = " DateTime with nanosecond resolution"]
    DateTimeNanosecond,
    #[doc = " DateTime with picosecond resolution"]
    DateTimePicosecond,
    #[doc = " DateTime with femtosecond resolution"]
    DateTimeFemtosecond,
    #[doc = " DateTime with attosecond resolution"]
    DateTimeAttosecond,
    #[doc = " Time with hour resolution"]
    TimeHour,
    #[doc = " Time with minute resolution"]
    TimeMinute,
    #[doc = " Time with second resolution"]
    TimeSecond,
    #[doc = " Time with millisecond resolution"]
    TimeMillisecond,
    #[doc = " Time with microsecond resolution"]
    TimeMicrosecond,
    #[doc = " Time with nanosecond resolution"]
    TimeNanosecond,
    #[doc = " Time with picosecond resolution"]
    TimePicosecond,
    #[doc = " Time with femtosecond resolution"]
    TimeFemtosecond,
    #[doc = " Time with attosecond resolution"]
    TimeAttosecond,
    #[doc = " Byte sequence"]
    Blob,
    #[doc = " Boolean"]
    Boolean,
    #[doc = " Geometry data in well-known binary (WKB) format, stored as std::byte"]
    GeometryWkb,
    #[doc = " Geometry data in well-known text (WKT) format, stored as std::byte"]
    GeometryWkt,
}

impl Datatype {
    pub(crate) fn capi_enum(&self) -> ffi::tiledb_datatype_t {
        match *self {
            Datatype::Int8 => ffi::tiledb_datatype_t_TILEDB_INT8,
            Datatype::Int16 => ffi::tiledb_datatype_t_TILEDB_INT16,
            Datatype::Int32 => ffi::tiledb_datatype_t_TILEDB_INT32,
            Datatype::Int64 => ffi::tiledb_datatype_t_TILEDB_INT64,
            Datatype::Float32 => ffi::tiledb_datatype_t_TILEDB_FLOAT32,
            Datatype::Float64 => ffi::tiledb_datatype_t_TILEDB_FLOAT64,
            Datatype::Char => ffi::tiledb_datatype_t_TILEDB_CHAR,
            Datatype::UInt8 => ffi::tiledb_datatype_t_TILEDB_UINT8,
            Datatype::UInt16 => ffi::tiledb_datatype_t_TILEDB_UINT16,
            Datatype::UInt32 => ffi::tiledb_datatype_t_TILEDB_UINT32,
            Datatype::UInt64 => ffi::tiledb_datatype_t_TILEDB_UINT64,
            Datatype::StringAscii => ffi::tiledb_datatype_t_TILEDB_STRING_ASCII,
            Datatype::StringUtf8 => ffi::tiledb_datatype_t_TILEDB_STRING_UTF8,
            Datatype::StringUtf16 => ffi::tiledb_datatype_t_TILEDB_STRING_UTF16,
            Datatype::StringUtf32 => ffi::tiledb_datatype_t_TILEDB_STRING_UTF32,
            Datatype::StringUcs2 => ffi::tiledb_datatype_t_TILEDB_STRING_UCS2,
            Datatype::StringUcs4 => ffi::tiledb_datatype_t_TILEDB_STRING_UCS4,
            Datatype::Any => ffi::tiledb_datatype_t_TILEDB_ANY,
            Datatype::DateTimeYear => {
                ffi::tiledb_datatype_t_TILEDB_DATETIME_YEAR
            }
            Datatype::DateTimeMonth => {
                ffi::tiledb_datatype_t_TILEDB_DATETIME_MONTH
            }
            Datatype::DateTimeWeek => {
                ffi::tiledb_datatype_t_TILEDB_DATETIME_WEEK
            }
            Datatype::DateTimeDay => ffi::tiledb_datatype_t_TILEDB_DATETIME_DAY,
            Datatype::DateTimeHour => ffi::tiledb_datatype_t_TILEDB_DATETIME_HR,
            Datatype::DateTimeMinute => {
                ffi::tiledb_datatype_t_TILEDB_DATETIME_MIN
            }
            Datatype::DateTimeSecond => {
                ffi::tiledb_datatype_t_TILEDB_DATETIME_SEC
            }
            Datatype::DateTimeMillisecond => {
                ffi::tiledb_datatype_t_TILEDB_DATETIME_MS
            }
            Datatype::DateTimeMicrosecond => {
                ffi::tiledb_datatype_t_TILEDB_DATETIME_US
            }
            Datatype::DateTimeNanosecond => {
                ffi::tiledb_datatype_t_TILEDB_DATETIME_NS
            }
            Datatype::DateTimePicosecond => {
                ffi::tiledb_datatype_t_TILEDB_DATETIME_PS
            }
            Datatype::DateTimeFemtosecond => {
                ffi::tiledb_datatype_t_TILEDB_DATETIME_FS
            }
            Datatype::DateTimeAttosecond => {
                ffi::tiledb_datatype_t_TILEDB_DATETIME_AS
            }
            Datatype::TimeHour => ffi::tiledb_datatype_t_TILEDB_TIME_HR,
            Datatype::TimeMinute => ffi::tiledb_datatype_t_TILEDB_TIME_MIN,
            Datatype::TimeSecond => ffi::tiledb_datatype_t_TILEDB_TIME_SEC,
            Datatype::TimeMillisecond => ffi::tiledb_datatype_t_TILEDB_TIME_MS,
            Datatype::TimeMicrosecond => ffi::tiledb_datatype_t_TILEDB_TIME_US,
            Datatype::TimeNanosecond => ffi::tiledb_datatype_t_TILEDB_TIME_NS,
            Datatype::TimePicosecond => ffi::tiledb_datatype_t_TILEDB_TIME_PS,
            Datatype::TimeFemtosecond => ffi::tiledb_datatype_t_TILEDB_TIME_FS,
            Datatype::TimeAttosecond => ffi::tiledb_datatype_t_TILEDB_TIME_AS,
            Datatype::Blob => ffi::tiledb_datatype_t_TILEDB_BLOB,
            Datatype::Boolean => ffi::tiledb_datatype_t_TILEDB_BOOL,
            Datatype::GeometryWkb => ffi::tiledb_datatype_t_TILEDB_GEOM_WKB,
            Datatype::GeometryWkt => ffi::tiledb_datatype_t_TILEDB_GEOM_WKT,
        }
    }

    pub fn size(&self) -> u64 {
        let copy = *self;
        unsafe { ffi::tiledb_datatype_size(copy as ffi::tiledb_datatype_t) }
    }

    pub fn to_string(&self) -> Option<String> {
        let copy = *self;
        let c_dtype = copy as ffi::tiledb_datatype_t;
        let mut c_str = std::ptr::null::<std::os::raw::c_char>();
        let res = unsafe { ffi::tiledb_datatype_to_str(c_dtype, &mut c_str) };
        if res == ffi::TILEDB_OK {
            let c_msg = unsafe { std::ffi::CStr::from_ptr(c_str) };
            Some(String::from(c_msg.to_string_lossy()))
        } else {
            None
        }
    }

    pub fn from_string(dtype: &str) -> Option<Self> {
        let c_dtype =
            std::ffi::CString::new(dtype).expect("Error creating CString");
        let mut c_ret: ffi::tiledb_datatype_t = out_ptr!();
        let res = unsafe {
            ffi::tiledb_datatype_from_str(
                c_dtype.as_c_str().as_ptr(),
                &mut c_ret,
            )
        };

        if res == ffi::TILEDB_OK {
            match Datatype::try_from(c_ret) {
                Ok(dt) => Some(dt),
                Err(_) => None,
            }
        } else {
            None
        }
    }

    pub fn is_compatible_type<T: 'static>(&self) -> bool {
        use std::any::TypeId;

        let tid = TypeId::of::<T>();
        if tid == TypeId::of::<f32>() {
            matches!(*self, Datatype::Float32)
        } else if tid == TypeId::of::<f64>() {
            matches!(*self, Datatype::Float64)
        } else if tid == TypeId::of::<i8>() {
            matches!(*self, Datatype::Char | Datatype::Int8)
        } else if tid == TypeId::of::<u8>() {
            matches!(
                *self,
                Datatype::Any
                    | Datatype::Blob
                    | Datatype::Boolean
                    | Datatype::GeometryWkb
                    | Datatype::GeometryWkt
                    | Datatype::StringAscii
                    | Datatype::StringUtf8
                    | Datatype::UInt8
            )
        } else if tid == TypeId::of::<i16>() {
            matches!(*self, Datatype::Int16)
        } else if tid == TypeId::of::<u16>() {
            matches!(
                *self,
                Datatype::StringUtf16 | Datatype::StringUcs2 | Datatype::UInt16
            )
        } else if tid == TypeId::of::<i32>() {
            matches!(*self, Datatype::Int32)
        } else if tid == TypeId::of::<u32>() {
            matches!(
                *self,
                Datatype::StringUtf32 | Datatype::StringUcs4 | Datatype::UInt32
            )
        } else if tid == TypeId::of::<i64>() {
            matches!(
                *self,
                Datatype::Int64
                    | Datatype::DateTimeYear
                    | Datatype::DateTimeMonth
                    | Datatype::DateTimeWeek
                    | Datatype::DateTimeDay
                    | Datatype::DateTimeHour
                    | Datatype::DateTimeMinute
                    | Datatype::DateTimeSecond
                    | Datatype::DateTimeMillisecond
                    | Datatype::DateTimeMicrosecond
                    | Datatype::DateTimeNanosecond
                    | Datatype::DateTimePicosecond
                    | Datatype::DateTimeFemtosecond
                    | Datatype::DateTimeAttosecond
                    | Datatype::TimeHour
                    | Datatype::TimeMinute
                    | Datatype::TimeSecond
                    | Datatype::TimeMillisecond
                    | Datatype::TimeMicrosecond
                    | Datatype::TimeNanosecond
                    | Datatype::TimePicosecond
                    | Datatype::TimeFemtosecond
                    | Datatype::TimeAttosecond
            )
        } else if tid == TypeId::of::<u64>() {
            matches!(*self, Datatype::UInt64)
        } else {
            false
        }
    }

    /// Returns whether this type is an integral type (i.e. integer)
    // Keep in sync with sm/enums/datatype.h::datatype_is_integer
    pub fn is_integral_type(&self) -> bool {
        matches!(
            *self,
            Datatype::Boolean
                | Datatype::Int8
                | Datatype::Int16
                | Datatype::Int32
                | Datatype::Int64
                | Datatype::UInt8
                | Datatype::UInt16
                | Datatype::UInt32
                | Datatype::UInt64
        )
    }

    /// Returns whether this type is a real number (i.e. floating point)
    // Keep in sync with sm/enums/datatype.h::datatype_is_real
    pub fn is_real_type(&self) -> bool {
        matches!(*self, Datatype::Float32 | Datatype::Float64)
    }

    /// Returns whether this type is a variable-length string type
    // Keep in sync with sm/enums/datatype.h::datatype_is_string
    pub fn is_string_type(&self) -> bool {
        matches!(
            *self,
            Datatype::StringAscii
                | Datatype::StringUtf8
                | Datatype::StringUtf16
                | Datatype::StringUtf32
                | Datatype::StringUcs2
                | Datatype::StringUcs4
        )
    }

    /// Returns whether this type is a DateTime type of any resolution
    // Keep in sync with sm/enums/datatype.h::datatype_is_datetime
    pub fn is_datetime_type(&self) -> bool {
        matches!(
            *self,
            Datatype::DateTimeYear
                | Datatype::DateTimeMonth
                | Datatype::DateTimeWeek
                | Datatype::DateTimeDay
                | Datatype::DateTimeHour
                | Datatype::DateTimeMinute
                | Datatype::DateTimeSecond
                | Datatype::DateTimeMillisecond
                | Datatype::DateTimeMicrosecond
                | Datatype::DateTimeNanosecond
                | Datatype::DateTimePicosecond
                | Datatype::DateTimeFemtosecond
                | Datatype::DateTimeAttosecond
        )
    }

    /// Returns whether this type is a Time type of any resolution
    // Keep in sync with sm/enums/datatype.h::datatype_is_time
    pub fn is_time_type(&self) -> bool {
        matches!(
            *self,
            Datatype::TimeHour
                | Datatype::TimeMinute
                | Datatype::TimeSecond
                | Datatype::TimeMillisecond
                | Datatype::TimeMicrosecond
                | Datatype::TimeNanosecond
                | Datatype::TimePicosecond
                | Datatype::TimeFemtosecond
                | Datatype::TimeAttosecond
        )
    }

    /// Returns whether this type is a byte
    // Keep in sync with sm/enums/datatype.h:datatype_is_byte
    pub fn is_byte_type(&self) -> bool {
        matches!(
            *self,
            Datatype::Boolean | Datatype::GeometryWkb | Datatype::GeometryWkt
        )
    }

    /// Returns whether this type can be used as a dimension type of a sparse array
    pub fn is_allowed_dimension_type_sparse(&self) -> bool {
        self.is_integral_type()
            || self.is_datetime_type()
            || self.is_time_type()
            || matches!(
                *self,
                Datatype::Float32 | Datatype::Float64 | Datatype::StringAscii
            )
    }

    /// Returns whether this type can be used as a dimension type of a dense array
    pub fn is_allowed_dimension_type_dense(&self) -> bool {
        self.is_integral_type()
            || self.is_datetime_type()
            || self.is_time_type()
    }
}

impl Debug for Datatype {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        <Self as Display>::fmt(self, f)
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

impl TryFrom<ffi::tiledb_datatype_t> for Datatype {
    type Error = crate::error::Error;

    fn try_from(value: ffi::tiledb_datatype_t) -> TileDBResult<Self> {
        Ok(match value {
            ffi::tiledb_datatype_t_TILEDB_INT8 => Datatype::Int8,
            ffi::tiledb_datatype_t_TILEDB_INT16 => Datatype::Int16,
            ffi::tiledb_datatype_t_TILEDB_INT32 => Datatype::Int32,
            ffi::tiledb_datatype_t_TILEDB_INT64 => Datatype::Int64,
            ffi::tiledb_datatype_t_TILEDB_FLOAT32 => Datatype::Float32,
            ffi::tiledb_datatype_t_TILEDB_FLOAT64 => Datatype::Float64,
            ffi::tiledb_datatype_t_TILEDB_CHAR => Datatype::Char,
            ffi::tiledb_datatype_t_TILEDB_UINT8 => Datatype::UInt8,
            ffi::tiledb_datatype_t_TILEDB_UINT16 => Datatype::UInt16,
            ffi::tiledb_datatype_t_TILEDB_UINT32 => Datatype::UInt32,
            ffi::tiledb_datatype_t_TILEDB_UINT64 => Datatype::UInt64,
            ffi::tiledb_datatype_t_TILEDB_STRING_ASCII => Datatype::StringAscii,
            ffi::tiledb_datatype_t_TILEDB_STRING_UTF8 => Datatype::StringUtf8,
            ffi::tiledb_datatype_t_TILEDB_STRING_UTF16 => Datatype::StringUtf16,
            ffi::tiledb_datatype_t_TILEDB_STRING_UTF32 => Datatype::StringUtf32,
            ffi::tiledb_datatype_t_TILEDB_STRING_UCS2 => Datatype::StringUcs2,
            ffi::tiledb_datatype_t_TILEDB_STRING_UCS4 => Datatype::StringUcs4,
            ffi::tiledb_datatype_t_TILEDB_ANY => Datatype::Any,
            ffi::tiledb_datatype_t_TILEDB_DATETIME_YEAR => {
                Datatype::DateTimeYear
            }
            ffi::tiledb_datatype_t_TILEDB_DATETIME_MONTH => {
                Datatype::DateTimeMonth
            }
            ffi::tiledb_datatype_t_TILEDB_DATETIME_WEEK => {
                Datatype::DateTimeWeek
            }
            ffi::tiledb_datatype_t_TILEDB_DATETIME_DAY => Datatype::DateTimeDay,
            ffi::tiledb_datatype_t_TILEDB_DATETIME_HR => Datatype::DateTimeHour,
            ffi::tiledb_datatype_t_TILEDB_DATETIME_MIN => {
                Datatype::DateTimeMinute
            }
            ffi::tiledb_datatype_t_TILEDB_DATETIME_SEC => {
                Datatype::DateTimeSecond
            }
            ffi::tiledb_datatype_t_TILEDB_DATETIME_MS => {
                Datatype::DateTimeMillisecond
            }
            ffi::tiledb_datatype_t_TILEDB_DATETIME_US => {
                Datatype::DateTimeMicrosecond
            }
            ffi::tiledb_datatype_t_TILEDB_DATETIME_NS => {
                Datatype::DateTimeNanosecond
            }
            ffi::tiledb_datatype_t_TILEDB_DATETIME_PS => {
                Datatype::DateTimePicosecond
            }
            ffi::tiledb_datatype_t_TILEDB_DATETIME_FS => {
                Datatype::DateTimeFemtosecond
            }
            ffi::tiledb_datatype_t_TILEDB_DATETIME_AS => {
                Datatype::DateTimeAttosecond
            }
            ffi::tiledb_datatype_t_TILEDB_TIME_HR => Datatype::TimeHour,
            ffi::tiledb_datatype_t_TILEDB_TIME_MIN => Datatype::TimeMinute,
            ffi::tiledb_datatype_t_TILEDB_TIME_SEC => Datatype::TimeSecond,
            ffi::tiledb_datatype_t_TILEDB_TIME_MS => Datatype::TimeMillisecond,
            ffi::tiledb_datatype_t_TILEDB_TIME_US => Datatype::TimeMicrosecond,
            ffi::tiledb_datatype_t_TILEDB_TIME_NS => Datatype::TimeNanosecond,
            ffi::tiledb_datatype_t_TILEDB_TIME_PS => Datatype::TimePicosecond,
            ffi::tiledb_datatype_t_TILEDB_TIME_FS => Datatype::TimeFemtosecond,
            ffi::tiledb_datatype_t_TILEDB_TIME_AS => Datatype::TimeAttosecond,
            ffi::tiledb_datatype_t_TILEDB_BLOB => Datatype::Blob,
            ffi::tiledb_datatype_t_TILEDB_BOOL => Datatype::Boolean,
            ffi::tiledb_datatype_t_TILEDB_GEOM_WKB => Datatype::GeometryWkb,
            ffi::tiledb_datatype_t_TILEDB_GEOM_WKT => Datatype::GeometryWkt,
            _ => {
                return Err(crate::error::Error::Datatype(
                    DatatypeErrorKind::InvalidDiscriminant(value as u64),
                ))
            }
        })
    }
}

/// Apply a generic function `$func` to data which implements `$datatype` and then run
/// the expression `$then` on the result.
/// The `$then` expression may use the function name as an identifier for the function result.
///
/// Variants:
/// - fn_typed!(my_function, my_datatype, arg1, ..., argN => then_expr)
///   Calls the function on the supplied arguments with a generic type parameter, and afterwards
///   runs `then_expr` on the result. The result is bound to an identifier which shadows the
///   function name.
/// - fn_typed!(obj.my_function, my_datatype, arg1, ..., argN => then_expr)
///   Calls the method on the supplied arguments with a generic type parameter, and afterwards
///   runs `then_expr` on the result. The result is bound to an identifier which shadows the
///   method name.
/// - fn_typed!(my_datatype, TypeName, then_expr)
///   Binds the type which implements `my_datatype` to `TypeName` for use in `then_expr`.

// note to developers: this is mimicking the C++ code
//      template <class Fn, class... Args>
//      inline auto apply_with_type(Fn&& f, Datatype type, Args&&... args)
//
// Also we probably only need the third variation since that can easily implement the other ones
//
#[macro_export]
macro_rules! fn_typed {
    ($datatype:expr, $typename:ident, $then:expr) => {{
        type Datatype = $crate::Datatype;
        match $datatype {
            Datatype::Int8 => { type $typename = i8; $then },
            Datatype::Int16 => { type $typename = i16; $then },
            Datatype::Int32 => { type $typename = i32; $then },
            Datatype::Int64 => { type $typename = i64; $then },
            Datatype::UInt8 => { type $typename = u8; $then },
            Datatype::UInt16 => { type $typename = u16; $then },
            Datatype::UInt32 => { type $typename = u32; $then },
            Datatype::UInt64 => { type $typename = u64; $then },
            Datatype::Float32 => { type $typename = f32; $then },
            Datatype::Float64 => { type $typename = f64; $then },
            Datatype::Char => unimplemented!(),
            Datatype::StringAscii => unimplemented!(),
            Datatype::StringUtf8 => unimplemented!(),
            Datatype::StringUtf16 => unimplemented!(),
            Datatype::StringUtf32 => unimplemented!(),
            Datatype::StringUcs2 => unimplemented!(),
            Datatype::StringUcs4 => unimplemented!(),
            Datatype::Any => unimplemented!(),
            Datatype::DateTimeYear => unimplemented!(),
            Datatype::DateTimeMonth => unimplemented!(),
            Datatype::DateTimeWeek => unimplemented!(),
            Datatype::DateTimeDay => unimplemented!(),
            Datatype::DateTimeHour => unimplemented!(),
            Datatype::DateTimeMinute => unimplemented!(),
            Datatype::DateTimeSecond => unimplemented!(),
            Datatype::DateTimeMillisecond => unimplemented!(),
            Datatype::DateTimeMicrosecond => unimplemented!(),
            Datatype::DateTimeNanosecond => unimplemented!(),
            Datatype::DateTimePicosecond => unimplemented!(),
            Datatype::DateTimeFemtosecond => unimplemented!(),
            Datatype::DateTimeAttosecond => unimplemented!(),
            Datatype::TimeHour => unimplemented!(),
            Datatype::TimeMinute => unimplemented!(),
            Datatype::TimeSecond => unimplemented!(),
            Datatype::TimeMillisecond => unimplemented!(),
            Datatype::TimeMicrosecond => unimplemented!(),
            Datatype::TimeNanosecond => unimplemented!(),
            Datatype::TimePicosecond => unimplemented!(),
            Datatype::TimeFemtosecond => unimplemented!(),
            Datatype::TimeAttosecond => unimplemented!(),
            Datatype::Blob => unimplemented!(),
            Datatype::Boolean => unimplemented!(),
            Datatype::GeometryWkb => unimplemented!(),
            Datatype::GeometryWkt => unimplemented!(),
        }
    }};

    ($func:ident, $datatype:expr$(, $arg:expr)* => $then:expr) => {{
        type Datatype = $crate::Datatype;
        match $datatype {
            Datatype::Int8 => {
                let $func = $func::<i8>($($arg,)*);
                $then
            }
            Datatype::Int16 => {
                let $func = $func::<i16>($($arg,)*);
                $then
            }
            Datatype::Int32 => {
                let $func = $func::<i32>($($arg,)*);
                $then
            }
            Datatype::Int64 => {
                let $func = $func::<i64>($($arg,)*);
                $then
            }
            Datatype::UInt8 => {
                let $func = $func::<u8>($($arg,)*);
                $then
            }
            Datatype::UInt16 => {
                let $func = $func::<u16>($($arg,)*);
                $then
            }
            Datatype::UInt32 => {
                let $func = $func::<u32>($($arg,)*);
                $then
            }
            Datatype::UInt64 => {
                let $func = $func::<u64>($($arg,)*);
                $then
            }
            Datatype::Float32 => {
                let $func = $func::<f32>($($arg,)*);
                $then
            }
            Datatype::Float64 => {
                let $func = $func::<f64>($($arg,)*);
                $then
            }
            Datatype::Char => unimplemented!(),
            Datatype::StringAscii => unimplemented!(),
            Datatype::StringUtf8 => unimplemented!(),
            Datatype::StringUtf16 => unimplemented!(),
            Datatype::StringUtf32 => unimplemented!(),
            Datatype::StringUcs2 => unimplemented!(),
            Datatype::StringUcs4 => unimplemented!(),
            Datatype::Any => unimplemented!(),
            Datatype::DateTimeYear => unimplemented!(),
            Datatype::DateTimeMonth => unimplemented!(),
            Datatype::DateTimeWeek => unimplemented!(),
            Datatype::DateTimeDay => unimplemented!(),
            Datatype::DateTimeHour => unimplemented!(),
            Datatype::DateTimeMinute => unimplemented!(),
            Datatype::DateTimeSecond => unimplemented!(),
            Datatype::DateTimeMillisecond => unimplemented!(),
            Datatype::DateTimeMicrosecond => unimplemented!(),
            Datatype::DateTimeNanosecond => unimplemented!(),
            Datatype::DateTimePicosecond => unimplemented!(),
            Datatype::DateTimeFemtosecond => unimplemented!(),
            Datatype::DateTimeAttosecond => unimplemented!(),
            Datatype::TimeHour => unimplemented!(),
            Datatype::TimeMinute => unimplemented!(),
            Datatype::TimeSecond => unimplemented!(),
            Datatype::TimeMillisecond => unimplemented!(),
            Datatype::TimeMicrosecond => unimplemented!(),
            Datatype::TimeNanosecond => unimplemented!(),
            Datatype::TimePicosecond => unimplemented!(),
            Datatype::TimeFemtosecond => unimplemented!(),
            Datatype::TimeAttosecond => unimplemented!(),
            Datatype::Blob => unimplemented!(),
            Datatype::Boolean => unimplemented!(),
            Datatype::GeometryWkb => unimplemented!(),
            Datatype::GeometryWkt => unimplemented!(),
        }
    }};
    ($obj:ident.$func:ident, $datatype:expr$(, $arg:expr)* => $then:expr) => {{
        type Datatype = $crate::Datatype;
        match $datatype {
            Datatype::Int8 => {
                let $func = $obj.$func::<i8>($($arg,)*);
                $then
            }
            Datatype::Int16 => {
                let $func = $obj.$func::<i16>($($arg,)*);
                $then
            }
            Datatype::Int32 => {
                let $func = $obj.$func::<i32>($($arg,)*);
                $then
            }
            Datatype::Int64 => {
                let $func = $obj.$func::<i64>($($arg,)*);
                $then
            }
            Datatype::UInt8 => {
                let $func = $obj.$func::<u8>($($arg,)*);
                $then
            }
            Datatype::UInt16 => {
                let $func = $obj.$func::<u16>($($arg,)*);
                $then
            }
            Datatype::UInt32 => {
                let $func = $obj.$func::<u32>($($arg,)*);
                $then
            }
            Datatype::UInt64 => {
                let $func = $obj.$func::<u64>($($arg,)*);
                $then
            }
            Datatype::Float32 => {
                let $func = $obj.$func::<f32>($($arg,)*);
                $then
            }
            Datatype::Float64 => {
                let $func = $obj.$func::<f64>($($arg,)*);
                $then
            }
            Datatype::Char => unimplemented!(),
            Datatype::StringAscii => unimplemented!(),
            Datatype::StringUtf8 => unimplemented!(),
            Datatype::StringUtf16 => unimplemented!(),
            Datatype::StringUtf32 => unimplemented!(),
            Datatype::StringUcs2 => unimplemented!(),
            Datatype::StringUcs4 => unimplemented!(),
            Datatype::Any => unimplemented!(),
            Datatype::DateTimeYear => unimplemented!(),
            Datatype::DateTimeMonth => unimplemented!(),
            Datatype::DateTimeWeek => unimplemented!(),
            Datatype::DateTimeDay => unimplemented!(),
            Datatype::DateTimeHour => unimplemented!(),
            Datatype::DateTimeMinute => unimplemented!(),
            Datatype::DateTimeSecond => unimplemented!(),
            Datatype::DateTimeMillisecond => unimplemented!(),
            Datatype::DateTimeMicrosecond => unimplemented!(),
            Datatype::DateTimeNanosecond => unimplemented!(),
            Datatype::DateTimePicosecond => unimplemented!(),
            Datatype::DateTimeFemtosecond => unimplemented!(),
            Datatype::DateTimeAttosecond => unimplemented!(),
            Datatype::TimeHour => unimplemented!(),
            Datatype::TimeMinute => unimplemented!(),
            Datatype::TimeSecond => unimplemented!(),
            Datatype::TimeMillisecond => unimplemented!(),
            Datatype::TimeMicrosecond => unimplemented!(),
            Datatype::TimeNanosecond => unimplemented!(),
            Datatype::TimePicosecond => unimplemented!(),
            Datatype::TimeFemtosecond => unimplemented!(),
            Datatype::TimeAttosecond => unimplemented!(),
            Datatype::Blob => unimplemented!(),
            Datatype::Boolean => unimplemented!(),
            Datatype::GeometryWkb => unimplemented!(),
            Datatype::GeometryWkt => unimplemented!(),
        }
    }};
}

#[cfg(feature = "proptest-strategies")]
pub mod strategy {
    use proptest::prelude::*;

    use crate::Datatype;

    pub fn prop_datatype() -> impl Strategy<Value = Datatype> {
        prop_oneof![
            Just(Datatype::Int8),
            Just(Datatype::Int16),
            Just(Datatype::Int32),
            Just(Datatype::Int64),
            Just(Datatype::UInt8),
            Just(Datatype::UInt16),
            Just(Datatype::UInt32),
            Just(Datatype::UInt64),
            Just(Datatype::Float32),
            Just(Datatype::Float64),
            Just(Datatype::Char),
            Just(Datatype::StringAscii),
            Just(Datatype::StringUtf8),
            Just(Datatype::StringUtf16),
            Just(Datatype::StringUtf32),
            Just(Datatype::StringUcs2),
            Just(Datatype::StringUcs4),
            Just(Datatype::Any),
            Just(Datatype::DateTimeYear),
            Just(Datatype::DateTimeMonth),
            Just(Datatype::DateTimeWeek),
            Just(Datatype::DateTimeDay),
            Just(Datatype::DateTimeHour),
            Just(Datatype::DateTimeMinute),
            Just(Datatype::DateTimeSecond),
            Just(Datatype::DateTimeMillisecond),
            Just(Datatype::DateTimeMicrosecond),
            Just(Datatype::DateTimeNanosecond),
            Just(Datatype::DateTimePicosecond),
            Just(Datatype::DateTimeFemtosecond),
            Just(Datatype::DateTimeAttosecond),
            Just(Datatype::TimeHour),
            Just(Datatype::TimeMinute),
            Just(Datatype::TimeSecond),
            Just(Datatype::TimeMillisecond),
            Just(Datatype::TimeMicrosecond),
            Just(Datatype::TimeNanosecond),
            Just(Datatype::TimePicosecond),
            Just(Datatype::TimeFemtosecond),
            Just(Datatype::TimeAttosecond),
            Just(Datatype::Blob),
            Just(Datatype::Boolean),
            Just(Datatype::GeometryWkb),
            Just(Datatype::GeometryWkt),
        ]
    }

    /// Choose an arbitrary datatype which is implemented
    /// (satisfies CAPIConv, and has cases in fn_typed)
    // TODO: make sure to keep this list up to date as we add more types
    pub fn prop_datatype_implemented() -> impl Strategy<Value = Datatype> {
        prop_oneof![
            Just(Datatype::Int8),
            Just(Datatype::Int16),
            Just(Datatype::Int32),
            Just(Datatype::Int64),
            Just(Datatype::UInt8),
            Just(Datatype::UInt16),
            Just(Datatype::UInt32),
            Just(Datatype::UInt64),
            Just(Datatype::Float32),
            Just(Datatype::Float64),
        ]
    }

    pub fn prop_datatype_for_dense_dimension() -> impl Strategy<Value = Datatype>
    {
        prop_datatype_implemented().prop_filter(
            "Type is not a valid dimension type for dense arrays",
            |dt| dt.is_allowed_dimension_type_dense(),
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
                let dt = Datatype::try_from(i as u32)
                    .expect("Error converting value to Datatype");
                assert_ne!(
                    format!("{}", dt),
                    "<UNKNOWN DATA TYPE>".to_string()
                );
                assert!(check_valid(&dt));
            } else {
                assert!(Datatype::try_from(i as u32).is_err());
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
