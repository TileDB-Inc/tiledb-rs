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

pub use ffi::*;
