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

pub fn prop_datatype_for_dense_dimension() -> impl Strategy<Value = Datatype> {
    prop_datatype_implemented().prop_filter(
        "Type is not a valid dimension type for dense arrays",
        |dt| dt.is_allowed_dimension_type_dense(),
    )
}
