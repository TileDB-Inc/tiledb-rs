/// For a TileDB type, returns an Arrow type if the bits of the canonical input type match.
/// If this returns Some(arrow_dt), then values of arrow_dt can be used in functions which expect tdb_dt, and vice verse.
pub fn arrow_type_physical(
    tdb_dt: &tiledb::Datatype,
) -> Option<arrow_schema::DataType> {
    match *tdb_dt {
        tiledb::Datatype::Int8 => Some(arrow_schema::DataType::Int8),
        tiledb::Datatype::Int16 => Some(arrow_schema::DataType::Int16),
        tiledb::Datatype::Int32 => Some(arrow_schema::DataType::Int32),
        tiledb::Datatype::Int64 => Some(arrow_schema::DataType::Int64),
        tiledb::Datatype::UInt8 => Some(arrow_schema::DataType::UInt8),
        tiledb::Datatype::UInt16 => Some(arrow_schema::DataType::UInt16),
        tiledb::Datatype::UInt32 => Some(arrow_schema::DataType::UInt32),
        tiledb::Datatype::UInt64 => Some(arrow_schema::DataType::UInt64),
        tiledb::Datatype::Float32 => Some(arrow_schema::DataType::Float32),
        tiledb::Datatype::Float64 => Some(arrow_schema::DataType::Float64),
        tiledb::Datatype::Char => None,
        tiledb::Datatype::StringAscii => None,
        tiledb::Datatype::StringUtf8 => None,
        tiledb::Datatype::StringUtf16 => None,
        tiledb::Datatype::StringUtf32 => None,
        tiledb::Datatype::StringUcs2 => None,
        tiledb::Datatype::StringUcs4 => None,
        tiledb::Datatype::Any => None,
        tiledb::Datatype::DateTimeYear => None,
        tiledb::Datatype::DateTimeMonth => None,
        tiledb::Datatype::DateTimeWeek => None,
        tiledb::Datatype::DateTimeDay => None,
        tiledb::Datatype::DateTimeHour => None,
        tiledb::Datatype::DateTimeMinute => None,
        tiledb::Datatype::DateTimeSecond => {
            Some(arrow_schema::DataType::Timestamp(
                arrow_schema::TimeUnit::Second,
                None,
            ))
        }
        tiledb::Datatype::DateTimeMillisecond => {
            Some(arrow_schema::DataType::Timestamp(
                arrow_schema::TimeUnit::Millisecond,
                None,
            ))
        }
        tiledb::Datatype::DateTimeMicrosecond => {
            Some(arrow_schema::DataType::Timestamp(
                arrow_schema::TimeUnit::Microsecond,
                None,
            ))
        }
        tiledb::Datatype::DateTimeNanosecond => {
            Some(arrow_schema::DataType::Timestamp(
                arrow_schema::TimeUnit::Nanosecond,
                None,
            ))
        }
        tiledb::Datatype::DateTimePicosecond => None,
        tiledb::Datatype::DateTimeFemtosecond => None,
        tiledb::Datatype::DateTimeAttosecond => None,
        tiledb::Datatype::TimeHour => None,
        tiledb::Datatype::TimeMinute => None,
        tiledb::Datatype::TimeSecond => None, // TODO: arrow type is 32 bits, is tiledb type?
        tiledb::Datatype::TimeMillisecond => None,
        tiledb::Datatype::TimeMicrosecond => Some(
            arrow_schema::DataType::Time64(arrow_schema::TimeUnit::Microsecond),
        ),
        tiledb::Datatype::TimeNanosecond => Some(
            arrow_schema::DataType::Time64(arrow_schema::TimeUnit::Nanosecond),
        ),
        tiledb::Datatype::TimePicosecond => None,
        tiledb::Datatype::TimeFemtosecond => None,
        tiledb::Datatype::TimeAttosecond => None,
        tiledb::Datatype::Blob => None,
        tiledb::Datatype::Boolean => None,
        tiledb::Datatype::GeometryWkb => None,
        tiledb::Datatype::GeometryWkt => None,
    }
}

/// For an Arrow type, returns a TileDB type if the bits of the canonical input type match.
/// If this returns Some(tdb_t), then values for tdb_t can be used in functions which expect
/// arrow_dt and vice verse.
pub fn tiledb_type_physical(
    arrow_dt: &arrow_schema::DataType,
) -> Option<tiledb::Datatype> {
    match *arrow_dt {
        arrow_schema::DataType::Int8 => Some(tiledb::Datatype::Int8),
        arrow_schema::DataType::Int16 => Some(tiledb::Datatype::Int16),
        arrow_schema::DataType::Int32 => Some(tiledb::Datatype::Int32),
        arrow_schema::DataType::Int64 => Some(tiledb::Datatype::Int64),
        arrow_schema::DataType::UInt8 => Some(tiledb::Datatype::UInt8),
        arrow_schema::DataType::UInt16 => Some(tiledb::Datatype::UInt16),
        arrow_schema::DataType::UInt32 => Some(tiledb::Datatype::UInt32),
        arrow_schema::DataType::UInt64 => Some(tiledb::Datatype::UInt64),
        arrow_schema::DataType::Float32 => Some(tiledb::Datatype::Float32),
        arrow_schema::DataType::Float64 => Some(tiledb::Datatype::Float64),
        arrow_schema::DataType::Timestamp(
            arrow_schema::TimeUnit::Second,
            None,
        ) => Some(tiledb::Datatype::DateTimeSecond),
        arrow_schema::DataType::Timestamp(
            arrow_schema::TimeUnit::Millisecond,
            None,
        ) => Some(tiledb::Datatype::DateTimeMillisecond),
        arrow_schema::DataType::Timestamp(
            arrow_schema::TimeUnit::Microsecond,
            None,
        ) => Some(tiledb::Datatype::DateTimeMicrosecond),
        arrow_schema::DataType::Timestamp(
            arrow_schema::TimeUnit::Nanosecond,
            None,
        ) => Some(tiledb::Datatype::DateTimeNanosecond),
        arrow_schema::DataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
            Some(tiledb::Datatype::TimeMicrosecond)
        }
        arrow_schema::DataType::Time64(arrow_schema::TimeUnit::Nanosecond) => {
            Some(tiledb::Datatype::TimeNanosecond)
        }
        _ => None, // TODO
    }
}

pub fn is_same_physical_type(
    tdb_dt: &tiledb::Datatype,
    arrow_dt: &arrow_schema::DataType,
) -> bool {
    if let Some(tdb_to_arrow) = arrow_type_physical(tdb_dt) {
        tdb_to_arrow == *arrow_dt
    } else {
        false
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Returns a strategy for generating any Arrow data type
    /// which corresponds to a TileDB Datatype
    pub fn prop_arrow_invertible(
    ) -> impl Strategy<Value = arrow_schema::DataType> {
        use arrow_schema::DataType as DT;
        use arrow_schema::TimeUnit as TU;
        prop_oneof![
            Just(DT::Int8),
            Just(DT::Int16),
            Just(DT::Int32),
            Just(DT::Int64),
            Just(DT::UInt8),
            Just(DT::UInt16),
            Just(DT::UInt32),
            Just(DT::UInt64),
            Just(DT::Float32),
            Just(DT::Float64),
            Just(DT::Timestamp(TU::Second, None)),
            Just(DT::Timestamp(TU::Millisecond, None)),
            Just(DT::Timestamp(TU::Microsecond, None)),
            Just(DT::Timestamp(TU::Nanosecond, None)),
            Just(DT::Time64(TU::Microsecond)),
            Just(DT::Time64(TU::Nanosecond))
        ]
    }

    pub fn prop_arrow_implemented(
    ) -> impl Strategy<Value = arrow_schema::DataType> {
        tiledb::strategy::prop_datatype_implemented()
            .prop_map(|dt| arrow_type_physical(&dt)
                .expect("Datatype claims to be implemented but does not have an arrow equivalent"))
    }

    mod strategy {
        use super::*;

        proptest! {
            /// Test that anything generated by the invertible strategy actually is
            #[test]
            fn test_arbitrary_arrow_invertible_to_tiledb(arrow_dt_in
                    in prop_arrow_invertible()) {
                let tdb_dt = tiledb_type_physical(&arrow_dt_in);
                assert!(tdb_dt.is_some());

                let tdb_dt = tdb_dt.unwrap();
                let arrow_dt_out = arrow_type_physical(&tdb_dt);
                assert!(arrow_dt_out.is_some());
                let arrow_dt_out = arrow_dt_out.unwrap();
                assert_eq!(arrow_dt_in, arrow_dt_out);
            }
        }
    }

    proptest! {
        #[test]
        fn test_physical(tdb_dt in tiledb::strategy::prop_datatype()) {
            if let Some(arrow_dt) = arrow_type_physical(&tdb_dt) {
                assert!(is_same_physical_type(&tdb_dt, &arrow_dt));
                if let Some(adt_width) = arrow_dt.primitive_width() {
                    let tdb_width : usize = tdb_dt.size().try_into().unwrap();
                    assert_eq!(adt_width, tdb_width);
                } else {
                    // TODO: assert that `tdb_dt` is variable-length
                }

                let inverted_dt = tiledb_type_physical(&arrow_dt);
                assert_eq!(Some(tdb_dt), inverted_dt);
            }
        }
    }
}
