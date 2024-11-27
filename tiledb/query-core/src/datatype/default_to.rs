use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use tiledb_common::array::CellValNum;
use tiledb_common::Datatype;

use super::TypeConversion::{self, LogicalMatch, PhysicalMatch};

#[derive(Debug, thiserror::Error)]
pub enum NoMatchDetail {
    #[error("Invalid fixed size cell val num: {0}")]
    InvalidFixedSize(u32),
}

pub fn default_arrow_type(
    dtype: Datatype,
    cell_val_num: CellValNum,
) -> Result<TypeConversion<ArrowDataType>, NoMatchDetail> {
    Ok(match (dtype, cell_val_num) {
        (Datatype::Blob, CellValNum::Fixed(nz)) if nz.get() != 1 => {
            if let Ok(fl) = i32::try_from(nz.get()) {
                LogicalMatch(ArrowDataType::FixedSizeBinary(fl))
            } else {
                return Err(NoMatchDetail::InvalidFixedSize(nz.get()));
            }
        }
        (
            Datatype::GeometryWkb | Datatype::GeometryWkt,
            CellValNum::Fixed(nz),
        ) if nz.get() != 1 => {
            if let Ok(fl) = i32::try_from(nz.get()) {
                PhysicalMatch(ArrowDataType::FixedSizeBinary(fl))
            } else {
                return Err(NoMatchDetail::InvalidFixedSize(nz.get()));
            }
        }
        (Datatype::StringAscii, CellValNum::Var) => {
            PhysicalMatch(ArrowDataType::LargeUtf8)
        }
        (Datatype::StringUtf8, CellValNum::Var) => {
            LogicalMatch(ArrowDataType::LargeUtf8)
        }
        (Datatype::Blob, CellValNum::Var) => {
            LogicalMatch(ArrowDataType::LargeBinary)
        }

        // then the general cases
        (_, CellValNum::Fixed(nz)) if nz.get() == 1 => {
            single_valued_type(dtype)
        }
        (_, CellValNum::Fixed(nz)) => {
            if let Ok(fl) = i32::try_from(nz.get()) {
                match single_valued_type(dtype) {
                    PhysicalMatch(adt) => PhysicalMatch(
                        ArrowDataType::new_fixed_size_list(adt, fl, false),
                    ),
                    LogicalMatch(adt) => LogicalMatch(
                        ArrowDataType::new_fixed_size_list(adt, fl, false),
                    ),
                }
            } else {
                return Err(NoMatchDetail::InvalidFixedSize(nz.get()));
            }
        }
        (_, CellValNum::Var) => match single_valued_type(dtype) {
            PhysicalMatch(adt) => {
                PhysicalMatch(ArrowDataType::new_large_list(adt, false))
            }
            LogicalMatch(adt) => {
                LogicalMatch(ArrowDataType::new_large_list(adt, false))
            }
        },
    })
}

fn single_valued_type(tiledb: Datatype) -> TypeConversion<ArrowDataType> {
    use arrow::datatypes::DataType as arrow;
    use tiledb_common::Datatype as tiledb;

    match tiledb {
        // Any is basically blob
        tiledb::Any => PhysicalMatch(arrow::UInt8),

        // Boolean
        // NB: this requires a byte array to bit array conversion,
        // it is a weird case of being a logical match but not a physical
        // match, we'll just handle it specially
        tiledb::Boolean => LogicalMatch(arrow::Boolean),

        // Char -> Int8
        tiledb::Char => PhysicalMatch(arrow::Int8),

        // Standard primitive types
        tiledb::Int8 => LogicalMatch(arrow::Int8),
        tiledb::Int16 => LogicalMatch(arrow::Int16),
        tiledb::Int32 => LogicalMatch(arrow::Int32),
        tiledb::Int64 => LogicalMatch(arrow::Int64),
        tiledb::UInt8 => LogicalMatch(arrow::UInt8),
        tiledb::UInt16 => LogicalMatch(arrow::UInt16),
        tiledb::UInt32 => LogicalMatch(arrow::UInt32),
        tiledb::UInt64 => LogicalMatch(arrow::UInt64),
        tiledb::Float32 => LogicalMatch(arrow::Float32),
        tiledb::Float64 => LogicalMatch(arrow::Float64),

        // string types
        // NB: with `CellValNum::Var` these map to `LargeUtf8`
        tiledb::StringAscii => PhysicalMatch(arrow::UInt8),
        tiledb::StringUtf8 => PhysicalMatch(arrow::UInt8),

        // string types with no exact match
        tiledb::StringUtf16 | tiledb::StringUcs2 => {
            PhysicalMatch(arrow::UInt16)
        }
        tiledb::StringUtf32 | tiledb::StringUcs4 => {
            PhysicalMatch(arrow::UInt32)
        }

        // datetime types with logical matches
        tiledb::DateTimeSecond => {
            LogicalMatch(arrow::Timestamp(TimeUnit::Second, None))
        }
        tiledb::DateTimeMillisecond => {
            LogicalMatch(arrow::Timestamp(TimeUnit::Millisecond, None))
        }
        tiledb::DateTimeMicrosecond => {
            LogicalMatch(arrow::Timestamp(TimeUnit::Microsecond, None))
        }
        tiledb::DateTimeNanosecond => {
            LogicalMatch(arrow::Timestamp(TimeUnit::Nanosecond, None))
        }
        tiledb::TimeSecond => LogicalMatch(arrow::Time64(TimeUnit::Second)),
        tiledb::TimeMillisecond => {
            LogicalMatch(arrow::Time64(TimeUnit::Millisecond))
        }
        tiledb::TimeMicrosecond => {
            LogicalMatch(arrow::Time64(TimeUnit::Microsecond))
        }
        tiledb::TimeNanosecond => {
            LogicalMatch(arrow::Time64(TimeUnit::Nanosecond))
        }

        // datetime types with no logical matches
        // NB: these can lose data if converted to an Arrow logical date/time type resolution
        tiledb::DateTimeYear
        | tiledb::DateTimeMonth
        | tiledb::DateTimeWeek
        | tiledb::DateTimeDay
        | tiledb::DateTimeHour
        | tiledb::DateTimeMinute
        | tiledb::DateTimePicosecond
        | tiledb::DateTimeFemtosecond
        | tiledb::DateTimeAttosecond
        | tiledb::TimeHour
        | tiledb::TimeMinute
        | tiledb::TimePicosecond
        | tiledb::TimeFemtosecond
        | tiledb::TimeAttosecond => PhysicalMatch(arrow::Int64),

        // Supported string types

        // Blob
        // NB: with other cell val nums this maps to `FixedSizeBinary` or `LargeBinary`
        tiledb::Blob => PhysicalMatch(arrow::UInt8),

        // Geometry
        // NB: with other cell val nums this maps to `FixedSizeBinary` or `LargeBinary`
        tiledb::GeometryWkb | tiledb::GeometryWkt => {
            PhysicalMatch(arrow::UInt8)
        }
    }
}
