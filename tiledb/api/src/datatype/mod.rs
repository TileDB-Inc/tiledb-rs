pub mod logical;
pub mod physical;

pub use logical::*;
pub use physical::{PhysicalType, PhysicalValue};

use std::fmt::{Debug, Display, Formatter, Result as FmtResult};

use serde::{Deserialize, Serialize};
use util::option::OptionSubset;

use crate::error::DatatypeErrorKind;
use crate::Result as TileDBResult;

#[derive(Clone, Copy, Default, Deserialize, Eq, Hash, PartialEq, Serialize)]
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
    // Any is default to cause an error if we forget to set it on either a
    // DimensionData or AttributeData instance.
    #[default]
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
            Datatype::Blob | Datatype::GeometryWkb | Datatype::GeometryWkt
        )
    }

    /// Returns whether this type can be used as a dimension type of a sparse array
    pub fn is_allowed_dimension_type_sparse(&self) -> bool {
        !matches!(self, Datatype::Boolean)
            && (self.is_integral_type()
                || self.is_datetime_type()
                || self.is_time_type()
                || matches!(
                    *self,
                    Datatype::Float32
                        | Datatype::Float64
                        | Datatype::StringAscii
                ))
    }

    /// Returns whether this type can be used as a dimension type of a dense array
    pub fn is_allowed_dimension_type_dense(&self) -> bool {
        !matches!(self, Datatype::Boolean)
            && (self.is_integral_type()
                || self.is_datetime_type()
                || self.is_time_type())
    }

    pub fn same_physical_type(&self, other: &Datatype) -> bool {
        crate::physical_type_go!(self, MyPhysicalType, {
            crate::physical_type_go!(other, TheirPhysicalType, {
                std::any::TypeId::of::<MyPhysicalType>()
                    == std::any::TypeId::of::<TheirPhysicalType>()
            })
        })
    }

    /// Returns an `Iterator` which yields each variant of `Datatype`
    /// exactly once in an unspecified order.
    pub fn iter() -> impl Iterator<Item = Datatype> {
        static DATATYPES: [Datatype; 43] = [
            Datatype::Int32,
            Datatype::Int64,
            Datatype::Float32,
            Datatype::Float64,
            Datatype::Char,
            Datatype::Int8,
            Datatype::UInt8,
            Datatype::Int16,
            Datatype::UInt16,
            Datatype::UInt32,
            Datatype::UInt64,
            Datatype::StringAscii,
            Datatype::StringUtf8,
            Datatype::StringUtf16,
            Datatype::StringUtf32,
            Datatype::StringUcs2,
            Datatype::StringUcs4,
            Datatype::DateTimeYear,
            Datatype::DateTimeMonth,
            Datatype::DateTimeWeek,
            Datatype::DateTimeDay,
            Datatype::DateTimeHour,
            Datatype::DateTimeMinute,
            Datatype::DateTimeSecond,
            Datatype::DateTimeMillisecond,
            Datatype::DateTimeMicrosecond,
            Datatype::DateTimeNanosecond,
            Datatype::DateTimePicosecond,
            Datatype::DateTimeFemtosecond,
            Datatype::DateTimeAttosecond,
            Datatype::TimeHour,
            Datatype::TimeMinute,
            Datatype::TimeSecond,
            Datatype::TimeMillisecond,
            Datatype::TimeMicrosecond,
            Datatype::TimeNanosecond,
            Datatype::TimePicosecond,
            Datatype::TimeFemtosecond,
            Datatype::TimeAttosecond,
            Datatype::Blob,
            Datatype::Boolean,
            Datatype::GeometryWkb,
            Datatype::GeometryWkt,
        ];
        DATATYPES.iter().copied()
    }
}

impl Debug for Datatype {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        <Self as Display>::fmt(self, f)
    }
}

impl Display for Datatype {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let copy = *self;
        let c_dtype = copy as ffi::tiledb_datatype_t;
        let mut c_str = std::ptr::null::<std::os::raw::c_char>();
        let res = unsafe { ffi::tiledb_datatype_to_str(c_dtype, &mut c_str) };

        /*
         * this cannot error if you provide a valid value, and the strong Rust
         * enum ensures that we have a valid value
         */
        assert_eq!(res, ffi::TILEDB_OK);

        let c_msg = unsafe { std::ffi::CStr::from_ptr(c_str) };
        write!(f, "{}", c_msg.to_string_lossy())
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

/// Apply a generic expression `$then` with a static type binding in the identifier `$typename`
/// for a logical type corresponding to the dynamic `$datatype`.
///
/// This is similar to `physical_type_go!` but binds the logical type
/// instead of the physical type.
// note to developers: this is mimicking the C++ code
//      template <class Fn, class... Args>
//      inline auto apply_with_type(Fn&& f, Datatype type, Args&&... args)
//
#[macro_export]
macro_rules! logical_type_go {
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

/// Apply a generic expression `$then` with a static type binding in the identifier `$typename`
/// for a physical type corresponding to the dynamic `$datatype`.
///
/// This is similar to `logical_type_go!` but binds the physical type instead of logical
/// type which is useful for calling generic functions and methods with a `PhysicalType`
/// trait bound.
///
/// # Examples
///
/// ```
/// use tiledb::{physical_type_go, Context, Datatype};
/// use tiledb::array::dimension::{Dimension, DimensionConstraints, Builder};
///
/// fn dimension_num_cells(d: &Dimension) -> Option<u64> {
///     physical_type_go!(d.datatype().unwrap(), DT, {
///         d.domain::<DT>().unwrap().map(|[low, high]| (high - low) as u64 + 1)
///     })
/// }
/// let ctx = Context::new().unwrap();
///
/// let d1 = Builder::new(&ctx, "d1", Datatype::UInt32,
///                       DimensionConstraints::UInt32([0, 16], Some(4))).unwrap().build();
/// assert_eq!(Some(17), dimension_num_cells(&d1));
///
/// let d2 = Builder::new(&ctx, "d2", Datatype::Int8,
///                       DimensionConstraints::Int8([-4, 4], Some(2))).unwrap().build();
/// assert_eq!(Some(9), dimension_num_cells(&d2));
/// ```
#[macro_export]
macro_rules! physical_type_go {
    ($datatype:expr, $typename:ident, $then:expr) => {{
        $crate::logical_type_go!($datatype, PhysicalTypeGoLogicalType, {
            type $typename = <PhysicalTypeGoLogicalType as $crate::datatype::LogicalType>::PhysicalType;
            $then
        })
    }};
}

#[cfg(feature = "arrow")]
pub mod arrow;

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use proptest::prelude::*;
    use util::{assert_not_option_subset, assert_option_subset};

    use super::*;

    #[test]
    fn datatype_roundtrips() {
        for i in 0..256 {
            let maybe_dt = Datatype::try_from(i);
            if maybe_dt.is_ok() {
                let dt = maybe_dt.unwrap();
                let dt_str = dt.to_string();
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

    #[test]
    fn iter() {
        let mut yielded = HashSet::<Datatype>::new();
        for dt in Datatype::iter() {
            let prev = yielded.insert(dt);
            assert!(prev);
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
            logical_type_go!(dt, LT, {
                let lt_constant = <LT as LogicalType>::DATA_TYPE;
                assert_eq!(dt, lt_constant);

                assert!(dt.is_compatible_type::<<LT as LogicalType>::PhysicalType>());
            })
        }
    }
}
