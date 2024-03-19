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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_physical(tdb_dt in tiledb_test::datatype::arbitrary()) {
            if let Some(arrow_dt) = arrow_type_physical(&tdb_dt) {
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
