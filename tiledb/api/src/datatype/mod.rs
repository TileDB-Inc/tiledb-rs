pub mod logical;
pub mod physical;

pub use logical::*;
pub use physical::PhysicalType;

use std::fmt::{Debug, Display, Formatter, Result as FmtResult};

use serde::{Deserialize, Serialize};
use util::option::OptionSubset;

use crate::error::DatatypeErrorKind;
use crate::Result as TileDBResult;

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[repr(u64)]
pub enum Datatype {
    /// A 32-bit signed integer
    Int32,
    /// A 64-bit signed integer
    Int64,
    /// A 32-bit floating point value
    Float32,
    /// A 64-bit floating point value
    Float64,
    /// An 8-bit character value
    Char,
    /// An 8-bit signed integer
    Int8,
    /// An 8-bit unsigned integer
    UInt8,
    /// A 16-bit signed integer
    Int16,
    /// A 16-bit unsigned integer
    UInt16,
    /// A 32-bit unsigned integer
    UInt32,
    /// A 64-bit unsigned integer
    UInt64,
    /// An ASCII string
    StringAscii,
    /// A UTF-8 string
    StringUtf8,
    /// A UTF-16 string
    StringUtf16,
    /// A UTF-32 string
    StringUtf32,
    /// A UCS2 string
    StringUcs2,
    /// A UCS4 string
    StringUcs4,
    /// An arbitrary type
    Any,
    /// DateTime with year resolution
    DateTimeYear,
    /// DateTime with month resolution
    DateTimeMonth,
    /// DateTime with week resolution
    DateTimeWeek,
    /// DateTime with day resolution
    DateTimeDay,
    /// DateTime with hour resolution
    DateTimeHour,
    /// DateTime with minute resolution
    DateTimeMinute,
    /// DateTime with second resolution
    DateTimeSecond,
    /// DateTime with millisecond resolution
    DateTimeMillisecond,
    /// DateTime with microsecond resolution
    DateTimeMicrosecond,
    /// DateTime with nanosecond resolution
    DateTimeNanosecond,
    /// DateTime with picosecond resolution
    DateTimePicosecond,
    /// DateTime with femtosecond resolution
    DateTimeFemtosecond,
    /// DateTime with attosecond resolution
    DateTimeAttosecond,
    /// Time with hour resolution
    TimeHour,
    /// Time with minute resolution
    TimeMinute,
    /// Time with second resolution
    TimeSecond,
    /// Time with millisecond resolution
    TimeMillisecond,
    /// Time with nanosecond resolution
    TimeMicrosecond,
    /// Time with nanosecond resolution
    TimeNanosecond,
    /// Time with picosecond resolution
    TimePicosecond,
    /// Time with femtosecond resolution
    TimeFemtosecond,
    /// Time with attosecond resolution
    TimeAttosecond,
    /// Byte sequence
    Blob,
    /// Boolean
    Boolean,
    /// A Geometry in well-known binary (WKB) format
    GeometryWkb,
    /// A Geometry in well-known text (WKT) format
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

impl OptionSubset for Datatype {
    fn option_subset(&self, other: &Self) -> bool {
        if let Datatype::Any = *self {
            true
        } else {
            self == other
        }
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
            Datatype::Int8 => {
                type $typename = $crate::datatype::logical::Int8Type;
                $then
            }
            Datatype::Int16 => {
                type $typename = $crate::datatype::logical::Int16Type;
                $then
            }
            Datatype::Int32 => {
                type $typename = $crate::datatype::logical::Int32Type;
                $then
            }
            Datatype::Int64 => {
                type $typename = $crate::datatype::logical::Int64Type;
                $then
            }
            Datatype::UInt8 => {
                type $typename = $crate::datatype::logical::UInt8Type;
                $then
            }
            Datatype::UInt16 => {
                type $typename = $crate::datatype::logical::UInt16Type;
                $then
            }
            Datatype::UInt32 => {
                type $typename = $crate::datatype::logical::UInt32Type;
                $then
            }
            Datatype::UInt64 => {
                type $typename = $crate::datatype::logical::UInt64Type;
                $then
            }
            Datatype::Float32 => {
                type $typename = $crate::datatype::logical::Float32Type;
                $then
            }
            Datatype::Float64 => {
                type $typename = $crate::datatype::logical::Float64Type;
                $then
            }
            Datatype::Char => {
                type $typename = $crate::datatype::logical::CharType;
                $then
            }
            Datatype::StringAscii => {
                type $typename = $crate::datatype::logical::StringAsciiType;
                $then
            }
            Datatype::StringUtf8 => {
                type $typename = $crate::datatype::logical::StringUtf8Type;
                $then
            }
            Datatype::StringUtf16 => {
                type $typename = $crate::datatype::logical::StringUtf16Type;
                $then
            }
            Datatype::StringUtf32 => {
                type $typename = $crate::datatype::logical::StringUtf32Type;
                $then
            }
            Datatype::StringUcs2 => {
                type $typename = $crate::datatype::logical::StringUcs2Type;
                $then
            }
            Datatype::StringUcs4 => {
                type $typename = $crate::datatype::logical::StringUcs4Type;
                $then
            }
            Datatype::Any => {
                type $typename = $crate::datatype::logical::AnyType;
                $then
            }
            Datatype::DateTimeYear => {
                type $typename = $crate::datatype::logical::DateTimeYearType;
                $then
            }
            Datatype::DateTimeMonth => {
                type $typename = $crate::datatype::logical::DateTimeMonthType;
                $then
            }
            Datatype::DateTimeWeek => {
                type $typename = $crate::datatype::logical::DateTimeWeekType;
                $then
            }
            Datatype::DateTimeDay => {
                type $typename = $crate::datatype::logical::DateTimeDayType;
                $then
            }
            Datatype::DateTimeHour => {
                type $typename = $crate::datatype::logical::DateTimeHourType;
                $then
            }
            Datatype::DateTimeMinute => {
                type $typename = $crate::datatype::logical::DateTimeMinuteType;
                $then
            }
            Datatype::DateTimeSecond => {
                type $typename = $crate::datatype::logical::DateTimeSecondType;
                $then
            }
            Datatype::DateTimeMillisecond => {
                type $typename =
                    $crate::datatype::logical::DateTimeMillisecondType;
                $then
            }
            Datatype::DateTimeMicrosecond => {
                type $typename =
                    $crate::datatype::logical::DateTimeMicrosecondType;
                $then
            }
            Datatype::DateTimeNanosecond => {
                type $typename =
                    $crate::datatype::logical::DateTimeNanosecondType;
                $then
            }
            Datatype::DateTimePicosecond => {
                type $typename =
                    $crate::datatype::logical::DateTimePicosecondType;
                $then
            }
            Datatype::DateTimeFemtosecond => {
                type $typename =
                    $crate::datatype::logical::DateTimeFemtosecondType;
                $then
            }
            Datatype::DateTimeAttosecond => {
                type $typename =
                    $crate::datatype::logical::DateTimeAttosecondType;
                $then
            }
            Datatype::TimeHour => {
                type $typename = $crate::datatype::logical::TimeHourType;
                $then
            }
            Datatype::TimeMinute => {
                type $typename = $crate::datatype::logical::TimeMinuteType;
                $then
            }
            Datatype::TimeSecond => {
                type $typename = $crate::datatype::logical::TimeSecondType;
                $then
            }
            Datatype::TimeMillisecond => {
                type $typename = $crate::datatype::logical::TimeMillisecondType;
                $then
            }
            Datatype::TimeMicrosecond => {
                type $typename = $crate::datatype::logical::TimeMicrosecondType;
                $then
            }
            Datatype::TimeNanosecond => {
                type $typename = $crate::datatype::logical::TimeNanosecondType;
                $then
            }
            Datatype::TimePicosecond => {
                type $typename = $crate::datatype::logical::TimePicosecondType;
                $then
            }
            Datatype::TimeFemtosecond => {
                type $typename = $crate::datatype::logical::TimeFemtosecondType;
                $then
            }
            Datatype::TimeAttosecond => {
                type $typename = $crate::datatype::logical::TimeAttosecondType;
                $then
            }
            Datatype::Blob => {
                type $typename = $crate::datatype::logical::BlobType;
                $then
            }
            Datatype::Boolean => {
                type $typename = $crate::datatype::logical::BooleanType;
                $then
            }
            Datatype::GeometryWkb => {
                type $typename = $crate::datatype::logical::GeometryWkbType;
                $then
            }
            Datatype::GeometryWkt => {
                type $typename = $crate::datatype::logical::GeometryWktType;
                $then
            }
        }
    }};
}

#[macro_export]
macro_rules! typed_enum {
    ($src:ident < $range:ident > => $(#[ $($attrs:meta),+ ])? $dst:ident) => {
        typed_enum!($src <() enum $range> => $(#[ $($attrs),+ ])? $dst);
    };
    ($src:ident < $g1:tt, $range:ident > => $(#[ $($attrs:meta),+ ])? $dst:ident) => {
        typed_enum!($src <($g1) enum $range> => $(#[ $($attrs),+ ])? $dst);
    };
    ($src:ident < $g1:tt, $g2:tt, $range:ident > => $(#[ $($attrs:meta),+ ])? $dst:ident) => {
        typed_enum!($src <($g1, $g2) enum $range> => $(#[ $($attrs),+ ])? $dst);
    };
    ($src:ident < $g1:tt, $g2:tt, $g3:tt, $range:ident > => $(#[ $($attrs:meta),+ ])? $dst:ident) => {
        typed_enum!($src <($g1, $g2, $g3) enum $range> => $(#[ $($attrs),+ ])? $dst);
    };
    ($src:ident < $g1:tt, $g2:tt, $g3:tt, $g4:tt, $range:ident > => $(#[ $($attrs:meta),+ ])? $dst:ident) => {
        typed_enum!($src <($g1, $g2, $g3, $g4) enum $range> => $(#[ $($attrs),+ ])? $dst);
    };
    ($src:ident < ($($generics:tt),*) enum $range:ident > => $(#[ $($attrs:meta),+ ])? $dst:ident) => {
        $(#[ $($attrs),+ ])?
        pub enum $dst<$($generics),*> {
            Int8($src<$($generics,)* <$crate::datatype::logical::Int8Type as $crate::datatype::logical::LogicalType>::PhysicalType>),
            Int16($src<$($generics,)* <$crate::datatype::logical::Int16Type as $crate::datatype::logical::LogicalType>::PhysicalType>),
            Int32($src<$($generics,)* <$crate::datatype::logical::Int32Type as $crate::datatype::logical::LogicalType>::PhysicalType>),
            Int64($src<$($generics,)* <$crate::datatype::logical::Int64Type as $crate::datatype::logical::LogicalType>::PhysicalType>),
            UInt8($src<$($generics,)* <$crate::datatype::logical::UInt8Type as $crate::datatype::logical::LogicalType>::PhysicalType>),
            UInt16($src<$($generics,)* <$crate::datatype::logical::UInt16Type as $crate::datatype::logical::LogicalType>::PhysicalType>),
            UInt32($src<$($generics,)* <$crate::datatype::logical::UInt32Type as $crate::datatype::logical::LogicalType>::PhysicalType>),
            UInt64($src<$($generics,)* <$crate::datatype::logical::UInt64Type as $crate::datatype::logical::LogicalType>::PhysicalType>),
            Float32($src<$($generics,)* <$crate::datatype::logical::Float32Type as $crate::datatype::logical::LogicalType>::PhysicalType>),
            Float64($src<$($generics,)* <$crate::datatype::logical::Float64Type as $crate::datatype::logical::LogicalType>::PhysicalType>),
            Char($src<$($generics,)* <$crate::datatype::logical::CharType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            StringAscii($src<$($generics,)* <$crate::datatype::logical::StringAsciiType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            StringUtf8($src<$($generics,)* <$crate::datatype::logical::StringUtf8Type as $crate::datatype::logical::LogicalType>::PhysicalType>),
            StringUtf16($src<$($generics,)* <$crate::datatype::logical::StringUtf16Type as $crate::datatype::logical::LogicalType>::PhysicalType>),
            StringUtf32($src<$($generics,)* <$crate::datatype::logical::StringUtf32Type as $crate::datatype::logical::LogicalType>::PhysicalType>),
            StringUcs2($src<$($generics,)* <$crate::datatype::logical::StringUcs2Type as $crate::datatype::logical::LogicalType>::PhysicalType>),
            StringUcs4($src<$($generics,)* <$crate::datatype::logical::StringUcs4Type as $crate::datatype::logical::LogicalType>::PhysicalType>),
            DateTimeYear($src<$($generics,)* <$crate::datatype::logical::DateTimeYearType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            DateTimeMonth($src<$($generics,)* <$crate::datatype::logical::DateTimeMonthType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            DateTimeWeek($src<$($generics,)* <$crate::datatype::logical::DateTimeWeekType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            DateTimeDay($src<$($generics,)* <$crate::datatype::logical::DateTimeDayType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            DateTimeHour($src<$($generics,)* <$crate::datatype::logical::DateTimeHourType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            DateTimeMinute($src<$($generics,)* <$crate::datatype::logical::DateTimeMinuteType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            DateTimeSecond($src<$($generics,)* <$crate::datatype::logical::DateTimeSecondType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            DateTimeMillisecond($src<$($generics,)* <$crate::datatype::logical::DateTimeMillisecondType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            DateTimeMicrosecond($src<$($generics,)* <$crate::datatype::logical::DateTimeMicrosecondType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            DateTimeNanosecond($src<$($generics,)* <$crate::datatype::logical::DateTimeNanosecondType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            DateTimePicosecond($src<$($generics,)* <$crate::datatype::logical::DateTimePicosecondType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            DateTimeFemtosecond($src<$($generics,)* <$crate::datatype::logical::DateTimeFemtosecondType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            DateTimeAttosecond($src<$($generics,)* <$crate::datatype::logical::DateTimeAttosecondType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            TimeHour($src<$($generics,)* <$crate::datatype::logical::TimeHourType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            TimeMinute($src<$($generics,)* <$crate::datatype::logical::TimeMinuteType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            TimeSecond($src<$($generics,)* <$crate::datatype::logical::TimeSecondType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            TimeMillisecond($src<$($generics,)* <$crate::datatype::logical::TimeMillisecondType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            TimeMicrosecond($src<$($generics,)* <$crate::datatype::logical::TimeMicrosecondType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            TimeNanosecond($src<$($generics,)* <$crate::datatype::logical::TimeNanosecondType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            TimePicosecond($src<$($generics,)* <$crate::datatype::logical::TimePicosecondType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            TimeFemtosecond($src<$($generics,)* <$crate::datatype::logical::TimeFemtosecondType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            TimeAttosecond($src<$($generics,)* <$crate::datatype::logical::TimeAttosecondType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            Blob($src<$($generics,)* <$crate::datatype::logical::BlobType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            Boolean($src<$($generics,)* <$crate::datatype::logical::BooleanType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            GeometryWkb($src<$($generics,)* <$crate::datatype::logical::GeometryWkbType as $crate::datatype::logical::LogicalType>::PhysicalType>),
            GeometryWkt($src<$($generics,)* <$crate::datatype::logical::GeometryWktType as $crate::datatype::logical::LogicalType>::PhysicalType>),
        }

        ::paste::paste! {
            #[macro_export]
            macro_rules! [< $dst:snake _go >] {
                ($thing:expr, $typename:ident, $binding:pat, $then:expr) => {
                    match $thing {
                        $dst::Int8($binding) => {
                            type $typename = $crate::datatype::logical::Int8Type;
                            $then
                        },
                        $dst::Int16($binding) => {
                            type $typename = $crate::datatype::logical::Int16Type;
                            $then
                        },
                        $dst::Int32($binding) => {
                            type $typename = $crate::datatype::logical::Int32Type;
                            $then
                        },
                        $dst::Int64($binding) => {
                            type $typename = $crate::datatype::logical::Int64Type;
                            $then
                        },
                        $dst::UInt8($binding) => {
                            type $typename = $crate::datatype::logical::UInt8Type;
                            $then
                        },
                        $dst::UInt16($binding) => {
                            type $typename = $crate::datatype::logical::UInt16Type;
                            $then
                        },
                        $dst::UInt32($binding) => {
                            type $typename = $crate::datatype::logical::UInt32Type;
                            $then
                        },
                        $dst::UInt64($binding) => {
                            type $typename = $crate::datatype::logical::UInt64Type;
                            $then
                        },
                        $dst::Float32($binding) => {
                            type $typename = $crate::datatype::logical::Float32Type;
                            $then
                        },
                        $dst::Float64($binding) => {
                            type $typename = $crate::datatype::logical::Float64Type;
                            $then
                        },
                        $dst::Char($binding) => {
                            type $typename = $crate::datatype::logical::CharType;
                            $then
                        },
                        $dst::StringAscii($binding) => {
                            type $typename = $crate::datatype::logical::StringAsciiType;
                            $then
                        },
                        $dst::StringUtf8($binding) => {
                            type $typename = $crate::datatype::logical::StringUtf8Type;
                            $then
                        },
                        $dst::StringUtf16($binding) => {
                            type $typename = $crate::datatype::logical::StringUtf16Type;
                            $then
                        },
                        $dst::StringUtf32($binding) => {
                            type $typename = $crate::datatype::logical::StringUtf32Type;
                            $then
                        },
                        $dst::StringUcs2($binding) => {
                            type $typename = $crate::datatype::logical::StringUcs2Type;
                            $then
                        },
                        $dst::StringUcs4($binding) => {
                            type $typename = $crate::datatype::logical::StringUcs4Type;
                            $then
                        },
                        $dst::DateTimeYear($binding) => {
                            type $typename = $crate::datatype::logical::DateTimeYearType;
                            $then
                        },
                        $dst::DateTimeMonth($binding) => {
                            type $typename = $crate::datatype::logical::DateTimeMonthType;
                            $then
                        },
                        $dst::DateTimeWeek($binding) => {
                            type $typename = $crate::datatype::logical::DateTimeWeekType;
                            $then
                        },
                        $dst::DateTimeDay($binding) => {
                            type $typename = $crate::datatype::logical::DateTimeDayType;
                            $then
                        },
                        $dst::DateTimeHour($binding) => {
                            type $typename = $crate::datatype::logical::DateTimeHourType;
                            $then
                        },
                        $dst::DateTimeMinute($binding) => {
                            type $typename = $crate::datatype::logical::DateTimeMinuteType;
                            $then
                        },
                        $dst::DateTimeSecond($binding) => {
                            type $typename = $crate::datatype::logical::DateTimeSecondType;
                            $then
                        },
                        $dst::DateTimeMillisecond($binding) => {
                            type $typename = $crate::datatype::logical::DateTimeMillisecondType;
                            $then
                        },
                        $dst::DateTimeMicrosecond($binding) => {
                            type $typename = $crate::datatype::logical::DateTimeMicrosecondType;
                            $then
                        },
                        $dst::DateTimeNanosecond($binding) => {
                            type $typename = $crate::datatype::logical::DateTimeNanosecondType;
                            $then
                        },
                        $dst::DateTimePicosecond($binding) => {
                            type $typename = $crate::datatype::logical::DateTimePicosecondType;
                            $then
                        },
                        $dst::DateTimeFemtosecond($binding) => {
                            type $typename = $crate::datatype::logical::DateTimeFemtosecondType;
                            $then
                        },
                        $dst::DateTimeAttosecond($binding) => {
                            type $typename = $crate::datatype::logical::DateTimeAttosecondType;
                            $then
                        },
                        $dst::TimeHour($binding) => {
                            type $typename = $crate::datatype::logical::TimeHourType;
                            $then
                        },
                        $dst::TimeMinute($binding) => {
                            type $typename = $crate::datatype::logical::TimeMinuteType;
                            $then
                        },
                        $dst::TimeSecond($binding) => {
                            type $typename = $crate::datatype::logical::TimeSecondType;
                            $then
                        },
                        $dst::TimeMillisecond($binding) => {
                            type $typename = $crate::datatype::logical::TimeMillisecondType;
                            $then
                        },
                        $dst::TimeMicrosecond($binding) => {
                            type $typename = $crate::datatype::logical::TimeMicrosecondType;
                            $then
                        },
                        $dst::TimeNanosecond($binding) => {
                            type $typename = $crate::datatype::logical::TimeNanosecondType;
                            $then
                        },
                        $dst::TimePicosecond($binding) => {
                            type $typename = $crate::datatype::logical::TimePicosecondType;
                            $then
                        },
                        $dst::TimeFemtosecond($binding) => {
                            type $typename = $crate::datatype::logical::TimeFemtosecondType;
                            $then
                        },
                        $dst::TimeAttosecond($binding) => {
                            type $typename = $crate::datatype::logical::TimeAttosecondType;
                            $then
                        },
                        $dst::Blob($binding) => {
                            type $typename = $crate::datatype::logical::BlobType;
                            $then
                        },
                        $dst::Boolean($binding) => {
                            type $typename = $crate::datatype::logical::BooleanType;
                            $then
                        },
                        $dst::GeometryWkb($binding) => {
                            type $typename = $crate::datatype::logical::GeometryWkbType;
                            $then
                        },
                        $dst::GeometryWkt($binding) => {
                            type $typename = $crate::datatype::logical::GeometryWktType;
                            $then
                        },
                    }
                }
            }
            pub use [< $dst:snake _go >];
        }
    };
}

#[cfg(feature = "arrow")]
pub mod arrow;

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use util::{assert_not_option_subset, assert_option_subset};

    #[test]
    fn datatype_roundtrips() {
        for i in 0..256 {
            let maybe_dt = Datatype::try_from(i);
            if maybe_dt.is_ok() {
                let dt = maybe_dt.unwrap();
                let dt_str = dt.to_string().expect("Error creating string.");
                let str_dt = Datatype::from_string(&dt_str)
                    .expect("Error round tripping datatype string.");
                assert_eq!(str_dt, dt);
            }
        }
    }

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

    #[test]
    fn option_subset() {
        assert_option_subset!(Datatype::Any, Datatype::Any);
        assert_option_subset!(Datatype::Any, Datatype::UInt16);
        assert_option_subset!(Datatype::Any, Datatype::UInt32);
        assert_option_subset!(Datatype::UInt16, Datatype::UInt16);
        assert_option_subset!(Datatype::UInt32, Datatype::UInt32);
        assert_not_option_subset!(Datatype::UInt32, Datatype::Any);
        assert_not_option_subset!(Datatype::UInt32, Datatype::UInt16);
        assert_not_option_subset!(Datatype::UInt16, Datatype::Any);
        assert_not_option_subset!(Datatype::UInt16, Datatype::UInt32);
    }

    proptest! {
        #[test]
        fn logical_type(dt in any::<Datatype>()) {
            fn_typed!(dt, LT, {
                let lt_constant = <LT as LogicalType>::DATA_TYPE;
                assert_eq!(dt, lt_constant);

                assert!(dt.is_compatible_type::<<LT as LogicalType>::PhysicalType>());
            })
        }
    }
}
