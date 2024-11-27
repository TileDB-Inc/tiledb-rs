use arrow::datatypes::{DataType as ArrowLogicalType, TimeUnit};
use tiledb_common::array::CellValNum;
use tiledb_common::Datatype;

/// Returns whether an arrow [DataType] can be used
/// to query fields with a particular [Datatype] and [CellValNum].
///
/// Both arrow's [DataType] and tiledb's [Datatype] are "logical"
/// types, i.e. they prescribe both the physical shape of the data
/// as well as how it should be interpreted. The variants of each
/// do not perfectly overlap, meaning that there are some logical
/// types which have a natural representation in arrow but not in
/// tiledb, and vice verse.
///
/// To enable using arrow as a means of querying tiledb data,
/// the tiledb logical types which do not have a corresponding
/// arrow type may be queried using the arrow [DataType] which
/// matches the _physical_ type of the tiledb logical type.
/// ```
/// use arrow::datatypes::DataType as ArrowLogicalType;
/// use tiledb_common::Datatype as TileDBLogicalType;
///
/// // `DateTimeFemtosecond` has no corresponding arrow type
/// let tiledb = TileDBLogicalType::DateTimeFemtosecond;
///
/// assert!(is_physically_compatible(&ArrowLogicalType::Int64,
///             tiledb, CellValNum::single()));
/// ```
///
/// If an application uses tiledb as the storage engine for data
/// which is described by an arrow schema, then it may need
/// to query an arrow logical type which does not have a corresponding
/// tiledb logical type.  As above, the tiledb [Datatype] which
/// matches the corresponding [ArrowNativeType] can be used.
/// ```
/// use arrow::datatypes::DataType as ArrowLogicalType;
/// use tiledb_common::Datatype as TileDBLogicalType;
///
/// // `Date32` has no corresponding tiledb type
/// let arrow = ArrowLogicalType::Date32;
///
/// assert!(is_physically_compatible(&arrow,
///             TileDBLogicalType::Int32, CellValNum::single()));
/// ```
pub fn is_physically_compatible(
    arrow_datatype: &ArrowLogicalType,
    tiledb_datatype: Datatype,
    tiledb_cell_val_num: CellValNum,
) -> bool {
    let is_single = tiledb_cell_val_num == CellValNum::single();
    let is_var = tiledb_cell_val_num == CellValNum::Var;

    match arrow_datatype {
        ArrowLogicalType::Null => false,
        ArrowLogicalType::Boolean => {
            matches!(tiledb_datatype, Datatype::Boolean) && is_single
        }

        ArrowLogicalType::Int8 => {
            matches!(tiledb_datatype, Datatype::Int8 | Datatype::Char)
                && is_single
        }
        ArrowLogicalType::Int16 => {
            matches!(tiledb_datatype, Datatype::Int16) && is_single
        }
        ArrowLogicalType::Int32 => {
            matches!(tiledb_datatype, Datatype::Int32) && is_single
        }
        ArrowLogicalType::Int64 => {
            matches!(
                tiledb_datatype,
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
            ) && is_single
        }
        ArrowLogicalType::UInt8 => {
            matches!(
                tiledb_datatype,
                Datatype::UInt8
                    | Datatype::StringAscii
                    | Datatype::StringUtf8
                    | Datatype::Any
                    | Datatype::Blob
                    | Datatype::Boolean
                    | Datatype::GeometryWkb
                    | Datatype::GeometryWkt
            ) && is_single
        }
        ArrowLogicalType::UInt16 => {
            matches!(
                tiledb_datatype,
                Datatype::UInt16 | Datatype::StringUtf16 | Datatype::StringUcs2
            ) && is_single
        }
        ArrowLogicalType::UInt32 => {
            matches!(
                tiledb_datatype,
                Datatype::UInt32 | Datatype::StringUtf32 | Datatype::StringUcs4
            ) && is_single
        }
        ArrowLogicalType::UInt64 => {
            matches!(tiledb_datatype, Datatype::UInt64) && is_single
        }
        ArrowLogicalType::Float32 => {
            matches!(tiledb_datatype, Datatype::Float32) && is_single
        }
        ArrowLogicalType::Float64 => {
            matches!(tiledb_datatype, Datatype::Float64) && is_single
        }
        ArrowLogicalType::Timestamp(TimeUnit::Second, None) => {
            matches!(tiledb_datatype, Datatype::DateTimeSecond) && is_single
        }
        ArrowLogicalType::Timestamp(TimeUnit::Millisecond, None) => {
            matches!(tiledb_datatype, Datatype::DateTimeMillisecond)
                && is_single
        }
        ArrowLogicalType::Timestamp(TimeUnit::Microsecond, None) => {
            matches!(tiledb_datatype, Datatype::DateTimeMicrosecond)
                && is_single
        }
        ArrowLogicalType::Timestamp(TimeUnit::Nanosecond, None) => {
            matches!(tiledb_datatype, Datatype::DateTimeNanosecond) && is_single
        }
        ArrowLogicalType::Timestamp(_, Some(_)) => false,
        ArrowLogicalType::Time64(TimeUnit::Second) => {
            matches!(tiledb_datatype, Datatype::TimeSecond) && is_single
        }
        ArrowLogicalType::Time64(TimeUnit::Millisecond) => {
            matches!(tiledb_datatype, Datatype::TimeMillisecond) && is_single
        }
        ArrowLogicalType::Time64(TimeUnit::Microsecond) => {
            matches!(tiledb_datatype, Datatype::TimeMicrosecond) && is_single
        }
        ArrowLogicalType::Time64(TimeUnit::Nanosecond) => {
            matches!(tiledb_datatype, Datatype::TimeNanosecond) && is_single
        }

        ArrowLogicalType::Utf8 | ArrowLogicalType::LargeUtf8 => {
            matches!(
                tiledb_datatype,
                Datatype::StringAscii | Datatype::StringUtf8
            ) && is_var
        }
        ArrowLogicalType::Binary | ArrowLogicalType::LargeBinary => {
            is_physically_compatible(
                &ArrowLogicalType::UInt8,
                tiledb_datatype,
                CellValNum::single(),
            ) && is_var
        }
        ArrowLogicalType::FixedSizeBinary(cvn) => {
            if tiledb_cell_val_num != *cvn as u32 {
                false
            } else {
                is_physically_compatible(
                    &ArrowLogicalType::UInt8,
                    tiledb_datatype,
                    CellValNum::single(),
                )
            }
        }

        ArrowLogicalType::List(field) | ArrowLogicalType::LargeList(field) => {
            // NB: any cell val num is allowed
            is_physically_compatible(
                field.data_type(),
                tiledb_datatype,
                CellValNum::single(),
            )
        }

        ArrowLogicalType::FixedSizeList(field, cvn) => {
            tiledb_cell_val_num == *cvn as u32
                && is_physically_compatible(
                    field.data_type(),
                    tiledb_datatype,
                    CellValNum::single(),
                )
        }
        ArrowLogicalType::Date32 => {
            matches!(tiledb_datatype, Datatype::Int32) && is_single
        }
        ArrowLogicalType::Date64 => {
            matches!(tiledb_datatype, Datatype::Int64) && is_single
        }
        ArrowLogicalType::Time32(_) => {
            matches!(tiledb_datatype, Datatype::Int32) && is_single
        }

        // TODO: Duration and Interval can be represented
        // Decimal128 and Decimal256 could be blobs...

        // Notes on other possible relaxed conversions:
        //
        // Duration and some intervals are likely supportable, but
        // leaving them off for now as the docs aren't clear.
        //
        // Views are also likely supportable, but will likely require
        // separate buffer allocations since individual values are not
        // contiguous.
        //
        // Struct and Union are never supportable (given current core)
        //
        // Dictionary is, but they should be handled higher up the stack
        // to ensure that things line up with enumerations.
        //
        // Decimal128 and Decimal256 might be supportable using Float64
        // and 2 or 4 fixed length cell val num. Though it'd be fairly
        // hacky.
        //
        // Map isn't supported in TileDB (given current core)
        //
        // RunEndEncoded is probably supportable, but like views will
        // require separate buffer allocations so leaving for now.
        ArrowLogicalType::Float16
        | ArrowLogicalType::Duration(_)
        | ArrowLogicalType::Interval(_)
        | ArrowLogicalType::BinaryView
        | ArrowLogicalType::Utf8View
        | ArrowLogicalType::ListView(_)
        | ArrowLogicalType::LargeListView(_)
        | ArrowLogicalType::Struct(_)
        | ArrowLogicalType::Union(_, _)
        | ArrowLogicalType::Dictionary(_, _)
        | ArrowLogicalType::Decimal128(_, _)
        | ArrowLogicalType::Decimal256(_, _)
        | ArrowLogicalType::Map(_, _)
        | ArrowLogicalType::RunEndEncoded(_, _) => false,
    }
}
