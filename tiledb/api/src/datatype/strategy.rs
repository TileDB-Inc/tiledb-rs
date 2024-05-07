use proptest::prelude::*;

use crate::Datatype;

fn prop_datatype() -> impl Strategy<Value = Datatype> {
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

fn prop_datatype_for_dense_dimension() -> impl Strategy<Value = Datatype> {
    /* see `Datatype::is_allowed_dimension_type_dense` */
    prop_oneof![
        Just(Datatype::Int8),
        Just(Datatype::Int16),
        Just(Datatype::Int32),
        Just(Datatype::Int64),
        Just(Datatype::UInt8),
        Just(Datatype::UInt16),
        Just(Datatype::UInt32),
        Just(Datatype::UInt64),
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
    ]
}

fn prop_datatype_for_sparse_dimension() -> impl Strategy<Value = Datatype> {
    /* see `Datatype::is_allowed_dimension_type_sparse` */
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
        Just(Datatype::StringAscii),
    ]
}

#[derive(Clone, Debug, Default)]
pub enum DatatypeContext {
    #[default]
    Any,
    DenseDimension,
    SparseDimension,
}

impl Arbitrary for Datatype {
    type Parameters = DatatypeContext;
    type Strategy = BoxedStrategy<Datatype>;

    fn arbitrary_with(p: Self::Parameters) -> Self::Strategy {
        match p {
            DatatypeContext::Any => prop_datatype().boxed(),
            DatatypeContext::DenseDimension => {
                prop_datatype_for_dense_dimension().boxed()
            }
            DatatypeContext::SparseDimension => {
                prop_datatype_for_sparse_dimension().boxed()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    proptest! {
        #[test]
        fn dense_dimension(dt in any_with::<Datatype>(DatatypeContext::DenseDimension)) {
            assert!(dt.is_allowed_dimension_type_dense())
        }

        #[test]
        fn sparse_dimension(dt in any_with::<Datatype>(DatatypeContext::SparseDimension)) {
            assert!(dt.is_allowed_dimension_type_sparse())
        }
    }
}
