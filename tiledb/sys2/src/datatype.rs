use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::str::FromStr;

use thiserror::Error;

use crate::error::TryFromFFIError;

#[cxx::bridge(namespace = "tiledb::rs")]
mod ffi {
    #[derive(Debug)]
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
}

pub use ffi::Datatype as FFIDatatype;

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum DatatypeError {
    #[error(
        "Physical type mismatch: expected {actual_type}, found {requested_type}"
    )]
    PhysicalTypeMismatch {
        requested_type: &'static str,
        actual_type: &'static str,
    },
    #[error(
        "Physical type '{physical_type}' is not compatible with logical type '{logical_type}'"
    )]
    PhysicalTypeIncompatible {
        physical_type: &'static str,
        logical_type: Datatype,
    },
    #[error(
        "Logical type mismatch: expected {target_type}, found {source_type}"
    )]
    LogicalTypeMismatch {
        source_type: Datatype,
        target_type: Datatype,
    },
}

impl DatatypeError {
    pub fn physical_type_mismatch<T, U>() -> Self {
        Self::PhysicalTypeMismatch {
            requested_type: std::any::type_name::<T>(),
            actual_type: std::any::type_name::<U>(),
        }
    }

    pub fn physical_type_incompatible<T>(logical_type: Datatype) -> Self {
        Self::PhysicalTypeIncompatible {
            physical_type: std::any::type_name::<T>(),
            logical_type,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
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

pub const DATATYPES: [Datatype; 43] = [
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

pub const DENSE_DIMENSION_DATATYPES: [Datatype; 30] = [
    Datatype::Int8,
    Datatype::Int16,
    Datatype::Int32,
    Datatype::Int64,
    Datatype::UInt8,
    Datatype::UInt16,
    Datatype::UInt32,
    Datatype::UInt64,
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
];

pub const SPARSE_DIMENSION_DATATYPES: [Datatype; 33] = [
    Datatype::Int8,
    Datatype::Int16,
    Datatype::Int32,
    Datatype::Int64,
    Datatype::UInt8,
    Datatype::UInt16,
    Datatype::UInt32,
    Datatype::UInt64,
    Datatype::Float32,
    Datatype::Float64,
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
    Datatype::StringAscii,
];

impl Datatype {
    pub fn size(&self) -> usize {
        crate::physical_type_go!(self, DT, std::mem::size_of::<DT>())
    }

    pub fn is_compatible_type<T: 'static>(&self) -> bool {
        use std::any::TypeId;

        let tid = TypeId::of::<T>();
        if matches!(*self, Datatype::Char) {
            // NB: some architectures this is signed, some it is unsigned,
            // so it needs this special case
            tid == TypeId::of::<std::ffi::c_char>()
        } else if tid == TypeId::of::<f32>() {
            matches!(*self, Datatype::Float32)
        } else if tid == TypeId::of::<f64>() {
            matches!(*self, Datatype::Float64)
        } else if tid == TypeId::of::<i8>() {
            matches!(*self, Datatype::Int8)
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

    /// Returns whether an attribute of this type can have an enumeration attached to it.
    // See `ArraySchema::add_attribute` and keep in sync with that
    pub fn is_allowed_attribute_type_for_enumeration(&self) -> bool {
        self.is_integral_type()
    }

    pub fn max_enumeration_variants(&self) -> Option<usize> {
        if matches!(self, Self::Boolean) {
            Some(2)
        } else if self.is_allowed_attribute_type_for_enumeration() {
            crate::physical_type_go!(self, DT, {
                // NB: see core `add_attribute`
                Some(DT::MAX as usize - 1)
            })
        } else {
            None
        }
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
        DATATYPES.iter().copied()
    }
}

impl Display for Datatype {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        Debug::fmt(self, f)
    }
}

impl FromStr for Datatype {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // NB: we don't use [ffi::tiledb_datatype_from_str]
        // so that the [tiledb_common] crate can run without
        // linking to libtiledb.so

        let s = s.to_ascii_lowercase();
        match s.as_ref() {
            "int8" => Ok(Datatype::Int8),
            "int16" => Ok(Datatype::Int16),
            "int32" => Ok(Datatype::Int32),
            "int64" => Ok(Datatype::Int64),
            "float32" => Ok(Datatype::Float32),
            "float64" => Ok(Datatype::Float64),
            "char" => Ok(Datatype::Char),
            "uint8" => Ok(Datatype::UInt8),
            "uint16" => Ok(Datatype::UInt16),
            "uint32" => Ok(Datatype::UInt32),
            "uint64" => Ok(Datatype::UInt64),
            "stringascii" => Ok(Datatype::StringAscii),
            "stringutf8" => Ok(Datatype::StringUtf8),
            "stringutf16" => Ok(Datatype::StringUtf16),
            "stringutf32" => Ok(Datatype::StringUtf32),
            "stringucs2" => Ok(Datatype::StringUcs2),
            "stringucs4" => Ok(Datatype::StringUcs4),
            "any" => Ok(Datatype::Any),
            "datetimeyear" => Ok(Datatype::DateTimeYear),
            "datetimemonth" => Ok(Datatype::DateTimeMonth),
            "datetimeweek" => Ok(Datatype::DateTimeWeek),
            "datetimeday" => Ok(Datatype::DateTimeDay),
            "datetimehour" => Ok(Datatype::DateTimeHour),
            "datetimeminute" => Ok(Datatype::DateTimeMinute),
            "datetimesecond" => Ok(Datatype::DateTimeSecond),
            "datetimemillisecond" => Ok(Datatype::DateTimeMillisecond),
            "datetimemicrosecond" => Ok(Datatype::DateTimeMicrosecond),
            "datetimenanosecond" => Ok(Datatype::DateTimeNanosecond),
            "datetimepicosecond" => Ok(Datatype::DateTimePicosecond),
            "datetimefemtosecond" => Ok(Datatype::DateTimeFemtosecond),
            "datetimeattosecond" => Ok(Datatype::DateTimeAttosecond),
            "timehour" => Ok(Datatype::TimeHour),
            "timeminute" => Ok(Datatype::TimeMinute),
            "timesecond" => Ok(Datatype::TimeSecond),
            "timemillisecond" => Ok(Datatype::TimeMillisecond),
            "timemicrosecond" => Ok(Datatype::TimeMicrosecond),
            "timenanosecond" => Ok(Datatype::TimeNanosecond),
            "timepicosecond" => Ok(Datatype::TimePicosecond),
            "timefemtosecond" => Ok(Datatype::TimeFemtosecond),
            "timeattosecond" => Ok(Datatype::TimeAttosecond),
            "blob" => Ok(Datatype::Blob),
            "boolean" => Ok(Datatype::Boolean),
            "geometrywkb" => Ok(Datatype::GeometryWkb),
            "geometrywkt" => Ok(Datatype::GeometryWkt),
            _ => Err(s),
        }
    }
}

impl From<Datatype> for ffi::Datatype {
    fn from(value: Datatype) -> Self {
        match value {
            Datatype::Int8 => ffi::Datatype::Int8,
            Datatype::Int16 => ffi::Datatype::Int16,
            Datatype::Int32 => ffi::Datatype::Int32,
            Datatype::Int64 => ffi::Datatype::Int64,
            Datatype::Float32 => ffi::Datatype::Float32,
            Datatype::Float64 => ffi::Datatype::Float64,
            Datatype::Char => ffi::Datatype::Char,
            Datatype::UInt8 => ffi::Datatype::UInt8,
            Datatype::UInt16 => ffi::Datatype::UInt16,
            Datatype::UInt32 => ffi::Datatype::UInt32,
            Datatype::UInt64 => ffi::Datatype::UInt64,
            Datatype::StringAscii => ffi::Datatype::StringAscii,
            Datatype::StringUtf8 => ffi::Datatype::StringUtf8,
            Datatype::StringUtf16 => ffi::Datatype::StringUtf16,
            Datatype::StringUtf32 => ffi::Datatype::StringUtf32,
            Datatype::StringUcs2 => ffi::Datatype::StringUcs2,
            Datatype::StringUcs4 => ffi::Datatype::StringUcs4,
            Datatype::Any => ffi::Datatype::Any,
            Datatype::DateTimeYear => ffi::Datatype::DateTimeYear,
            Datatype::DateTimeMonth => ffi::Datatype::DateTimeMonth,
            Datatype::DateTimeWeek => ffi::Datatype::DateTimeWeek,
            Datatype::DateTimeDay => ffi::Datatype::DateTimeDay,
            Datatype::DateTimeHour => ffi::Datatype::DateTimeHour,
            Datatype::DateTimeMinute => ffi::Datatype::DateTimeMinute,
            Datatype::DateTimeSecond => ffi::Datatype::DateTimeSecond,
            Datatype::DateTimeMillisecond => ffi::Datatype::DateTimeMillisecond,
            Datatype::DateTimeMicrosecond => ffi::Datatype::DateTimeMicrosecond,
            Datatype::DateTimeNanosecond => ffi::Datatype::DateTimeNanosecond,
            Datatype::DateTimePicosecond => ffi::Datatype::DateTimePicosecond,
            Datatype::DateTimeFemtosecond => ffi::Datatype::DateTimeFemtosecond,
            Datatype::DateTimeAttosecond => ffi::Datatype::DateTimeAttosecond,
            Datatype::TimeHour => ffi::Datatype::TimeHour,
            Datatype::TimeMinute => ffi::Datatype::TimeMinute,
            Datatype::TimeSecond => ffi::Datatype::TimeSecond,
            Datatype::TimeMillisecond => ffi::Datatype::TimeMillisecond,
            Datatype::TimeMicrosecond => ffi::Datatype::TimeMicrosecond,
            Datatype::TimeNanosecond => ffi::Datatype::TimeNanosecond,
            Datatype::TimePicosecond => ffi::Datatype::TimePicosecond,
            Datatype::TimeFemtosecond => ffi::Datatype::TimeFemtosecond,
            Datatype::TimeAttosecond => ffi::Datatype::TimeAttosecond,
            Datatype::Blob => ffi::Datatype::Blob,
            Datatype::Boolean => ffi::Datatype::Boolean,
            Datatype::GeometryWkb => ffi::Datatype::GeometryWkb,
            Datatype::GeometryWkt => ffi::Datatype::GeometryWkt,
        }
    }
}

impl TryFrom<FFIDatatype> for Datatype {
    type Error = TryFromFFIError;

    fn try_from(value: FFIDatatype) -> Result<Self, Self::Error> {
        Ok(match value {
            ffi::Datatype::Int8 => Datatype::Int8,
            ffi::Datatype::Int16 => Datatype::Int16,
            ffi::Datatype::Int32 => Datatype::Int32,
            ffi::Datatype::Int64 => Datatype::Int64,
            ffi::Datatype::Float32 => Datatype::Float32,
            ffi::Datatype::Float64 => Datatype::Float64,
            ffi::Datatype::Char => Datatype::Char,
            ffi::Datatype::UInt8 => Datatype::UInt8,
            ffi::Datatype::UInt16 => Datatype::UInt16,
            ffi::Datatype::UInt32 => Datatype::UInt32,
            ffi::Datatype::UInt64 => Datatype::UInt64,
            ffi::Datatype::StringAscii => Datatype::StringAscii,
            ffi::Datatype::StringUtf8 => Datatype::StringUtf8,
            ffi::Datatype::StringUtf16 => Datatype::StringUtf16,
            ffi::Datatype::StringUtf32 => Datatype::StringUtf32,
            ffi::Datatype::StringUcs2 => Datatype::StringUcs2,
            ffi::Datatype::StringUcs4 => Datatype::StringUcs4,
            ffi::Datatype::Any => Datatype::Any,
            ffi::Datatype::DateTimeYear => Datatype::DateTimeYear,
            ffi::Datatype::DateTimeMonth => Datatype::DateTimeMonth,
            ffi::Datatype::DateTimeWeek => Datatype::DateTimeWeek,
            ffi::Datatype::DateTimeDay => Datatype::DateTimeDay,
            ffi::Datatype::DateTimeHour => Datatype::DateTimeHour,
            ffi::Datatype::DateTimeMinute => Datatype::DateTimeMinute,
            ffi::Datatype::DateTimeSecond => Datatype::DateTimeSecond,
            ffi::Datatype::DateTimeMillisecond => Datatype::DateTimeMillisecond,
            ffi::Datatype::DateTimeMicrosecond => Datatype::DateTimeMicrosecond,
            ffi::Datatype::DateTimeNanosecond => Datatype::DateTimeNanosecond,
            ffi::Datatype::DateTimePicosecond => Datatype::DateTimePicosecond,
            ffi::Datatype::DateTimeFemtosecond => Datatype::DateTimeFemtosecond,
            ffi::Datatype::DateTimeAttosecond => Datatype::DateTimeAttosecond,
            ffi::Datatype::TimeHour => Datatype::TimeHour,
            ffi::Datatype::TimeMinute => Datatype::TimeMinute,
            ffi::Datatype::TimeSecond => Datatype::TimeSecond,
            ffi::Datatype::TimeMillisecond => Datatype::TimeMillisecond,
            ffi::Datatype::TimeMicrosecond => Datatype::TimeMicrosecond,
            ffi::Datatype::TimeNanosecond => Datatype::TimeNanosecond,
            ffi::Datatype::TimePicosecond => Datatype::TimePicosecond,
            ffi::Datatype::TimeFemtosecond => Datatype::TimeFemtosecond,
            ffi::Datatype::TimeAttosecond => Datatype::TimeAttosecond,
            ffi::Datatype::Blob => Datatype::Blob,
            ffi::Datatype::Boolean => Datatype::Boolean,
            ffi::Datatype::GeometryWkb => Datatype::GeometryWkb,
            ffi::Datatype::GeometryWkt => Datatype::GeometryWkt,
            _ => {
                return Err(TryFromFFIError::from_datatype(value));
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::types::LogicalType;

    #[test]
    fn datatype_roundtrips() {
        for i in 0u8..255 {
            let maybe_dt = Datatype::try_from(FFIDatatype { repr: i });
            if let Ok(dt) = maybe_dt {
                assert_eq!(i, FFIDatatype::from(dt).repr);
            }
        }
    }

    #[test]
    fn datatype_test() {
        const NUM_DATATYPES: usize = DATATYPES.len() + 1; // for Datatype::Any
        for i in 0u8..255 {
            if (i as usize) < NUM_DATATYPES {
                let dt = Datatype::try_from(FFIDatatype { repr: i })
                    .expect("Error converting value to Datatype");
                assert_ne!(format!("{dt}"), "<UNKNOWN DATA TYPE>".to_string());
                assert!(check_valid(&dt));
            } else {
                assert!(Datatype::try_from(FFIDatatype { repr: i }).is_err());
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
    fn logical_type() -> Result<(), TryFromFFIError> {
        const NUM_DATATYPES: u8 = (DATATYPES.len() + 1) as u8; // for Datatype::Any
        for i in 0u8..NUM_DATATYPES {
            let dt = Datatype::try_from(FFIDatatype { repr: i })?;
            crate::logical_type_go!(dt, LT, {
                let lt_constant = <LT as LogicalType>::DATA_TYPE;
                assert_eq!(dt, lt_constant);
                assert!(
                    dt.is_compatible_type::<<LT as LogicalType>::PhysicalType>(
                    )
                );
            })
        }

        Ok(())
    }

    #[test]
    fn from_str() {
        for datatype in Datatype::iter() {
            let s_in = datatype.to_string();
            let s_out = Datatype::from_str(&s_in);

            assert_eq!(Ok(datatype), s_out);
        }
    }
}
