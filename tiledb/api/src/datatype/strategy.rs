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

const DENSE_DIMENSION_DATATYPES: [Datatype; 30] = [
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

const SPARSE_DIMENSION_DATATYPES: [Datatype; 33] = [
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

fn prop_datatype_for_dense_dimension() -> impl Strategy<Value = Datatype> {
    /* see `Datatype::is_allowed_dimension_type_dense` */
    proptest::strategy::Union::new(
        DENSE_DIMENSION_DATATYPES.iter().map(|dt| Just(*dt)),
    )
}

fn prop_datatype_for_sparse_dimension() -> impl Strategy<Value = Datatype> {
    /* see `Datatype::is_allowed_dimension_type_sparse` */
    proptest::strategy::Union::new(
        SPARSE_DIMENSION_DATATYPES.iter().map(|dt| Just(*dt)),
    )
}

const DELTA_FILTER_REINTERPRET_DATATYPES: [Datatype; 37] = [
    Datatype::Any,
    Datatype::UInt8,
    Datatype::UInt16,
    Datatype::UInt32,
    Datatype::UInt64,
    Datatype::Int8,
    Datatype::Int16,
    Datatype::Int32,
    Datatype::Int64,
    Datatype::Float32,
    Datatype::Float64,
    Datatype::Boolean,
    Datatype::Blob,
    Datatype::GeometryWkb,
    Datatype::GeometryWkt,
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

fn prop_datatype_for_delta_filter() -> impl Strategy<Value = Datatype> {
    // see core `FilterBuffer::buffers_as`
    proptest::strategy::Union::new(
        DELTA_FILTER_REINTERPRET_DATATYPES
            .iter()
            .map(|dt| Just(*dt)),
    )
}

#[derive(Clone, Debug, Default)]
pub enum DatatypeContext {
    #[default]
    Any,
    DenseDimension,
    SparseDimension,
    DeltaFilterReinterpretDatatype,
    Fixed(Datatype),
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
            DatatypeContext::DeltaFilterReinterpretDatatype => {
                prop_datatype_for_delta_filter().boxed()
            }
            DatatypeContext::Fixed(dt) => Just(dt).boxed(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::dimension::DimensionConstraints;
    use crate::array::{
        ArrayType, AttributeBuilder, DimensionBuilder, DomainBuilder, Schema,
        SchemaBuilder,
    };
    use crate::datatype::LogicalType;
    use crate::error::Error;
    use crate::{Context, Result as TileDBResult};

    /// Creates a schema with a single dimension of the given `Datatype` with one attribute.
    /// Used by the test to check if the `Datatype` can be used in this way.
    fn dimension_comprehensive_schema(
        context: &Context,
        array_type: ArrayType,
        datatype: Datatype,
    ) -> TileDBResult<Schema> {
        let dim = fn_typed!(datatype, LT, {
            if matches!(datatype, Datatype::StringAscii) {
                DimensionBuilder::new(
                    context,
                    "d",
                    datatype,
                    DimensionConstraints::StringAscii,
                )
            } else {
                type DT = <LT as LogicalType>::PhysicalType;
                let domain: [DT; 2] = [0 as DT, 127 as DT];
                let extent: DT = 16 as DT;
                DimensionBuilder::new(context, "d", datatype, (domain, extent))
            }
        })?
        .build();

        let attr = AttributeBuilder::new(context, "a", Datatype::Any)?.build();

        let domain = DomainBuilder::new(context)?.add_dimension(dim)?.build();
        SchemaBuilder::new(context, array_type, domain)?
            .add_attribute(attr)?
            .build()
    }

    fn do_dense_dimension_comprehensive(datatype: Datatype) {
        let allowed = DENSE_DIMENSION_DATATYPES.contains(&datatype);
        assert_eq!(allowed, datatype.is_allowed_dimension_type_dense());

        let context = Context::new().unwrap();
        let r = dimension_comprehensive_schema(
            &context,
            ArrayType::Dense,
            datatype,
        );
        assert_eq!(allowed, r.is_ok(), "try_construct => {:?}", r.err());
        if let Err(Error::LibTileDB(s)) = r {
            assert!(
                s.contains("not a valid Dimension Datatype")
                    || s.contains("do not support dimension datatype"),
                "Expected dimension datatype error, received: {}",
                s
            );
        } else {
            assert!(
                r.is_ok(),
                "Found error other than LibTileDB: {}",
                r.err().unwrap()
            );
        }
    }

    fn do_sparse_dimension_comprehensive(datatype: Datatype) {
        let allowed = SPARSE_DIMENSION_DATATYPES.contains(&datatype);
        assert_eq!(allowed, datatype.is_allowed_dimension_type_sparse());

        let context = Context::new().unwrap();
        let r = dimension_comprehensive_schema(
            &context,
            ArrayType::Sparse,
            datatype,
        );
        assert_eq!(allowed, r.is_ok(), "try_construct => {:?}", r.err());
        if let Err(Error::LibTileDB(s)) = r {
            assert!(
                s.contains("not a valid Dimension Datatype")
                    || s.contains("do not support dimension datatype"),
                "Expected dimension datatype error, received: {}",
                s
            );
        } else {
            assert!(
                r.is_ok(),
                "Found error other than LibTileDB: {}",
                r.err().unwrap()
            );
        }
    }

    proptest! {
        #[test]
        fn dense_dimension(dt in any_with::<Datatype>(DatatypeContext::DenseDimension)) {
            assert!(dt.is_allowed_dimension_type_dense())
        }

        #[test]
        fn dense_dimension_comprehensive(dt in any::<Datatype>()) {
            do_dense_dimension_comprehensive(dt)
        }

        #[test]
        fn sparse_dimension(dt in any_with::<Datatype>(DatatypeContext::SparseDimension)) {
            assert!(dt.is_allowed_dimension_type_sparse())
        }

        #[test]
        fn sparse_dimension_comprehensive(dt in any::<Datatype>()) {
            do_sparse_dimension_comprehensive(dt)
        }
    }
}
