use proptest::prelude::*;

use tiledb::datatype::Datatype;

pub fn prop_all_datatypes() -> impl Strategy<Value = Datatype> {
    proptest::strategy::Union::new(
        all_datatypes_vec().iter().map(|dt| Just(*dt)),
    )
}

pub fn all_datatypes_vec() -> Vec<Datatype> {
    vec![
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
        Datatype::Char,
        Datatype::StringAscii,
        Datatype::StringUtf8,
        Datatype::StringUtf16,
        Datatype::StringUtf32,
        Datatype::StringUcs2,
        Datatype::StringUcs4,
        Datatype::Any,
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
    ]
}

pub fn prop_dense_dimension_datatypes() -> impl Strategy<Value = Datatype> {
    proptest::strategy::Union::new(
        dense_dimension_datatypes_vec().iter().map(|dt| Just(*dt)),
    )
}

pub fn dense_dimension_datatypes_vec() -> Vec<Datatype> {
    vec![
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
    ]
}

pub fn prop_sparse_dimension_datatypes() -> impl Strategy<Value = Datatype> {
    proptest::strategy::Union::new(
        sparse_dimension_datatypes_vec().iter().map(|dt| Just(*dt)),
    )
}

pub fn sparse_dimension_datatypes_vec() -> Vec<Datatype> {
    vec![
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
        Datatype::StringAscii,
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
    ]
}

pub fn prop_delta_filter_datatypes() -> impl Strategy<Value = Datatype> {
    proptest::strategy::Union::new(
        delta_datatypes_vec().iter().map(|dt| Just(*dt)),
    )
}

pub fn delta_datatypes_vec() -> Vec<Datatype> {
    // Everything but Float32/Float64
    vec![
        Datatype::Int8,
        Datatype::Int16,
        Datatype::Int32,
        Datatype::Int64,
        Datatype::UInt8,
        Datatype::UInt16,
        Datatype::UInt32,
        Datatype::UInt64,
        Datatype::Char,
        Datatype::StringAscii,
        Datatype::StringUtf8,
        Datatype::StringUtf16,
        Datatype::StringUtf32,
        Datatype::StringUcs2,
        Datatype::StringUcs4,
        Datatype::Any,
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
    ]
}
