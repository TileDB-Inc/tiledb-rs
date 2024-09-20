use std::sync::Arc;

use arrow::datatypes as adt;

use thiserror::Error;

use crate::array::schema::CellValNum;
use crate::datatype::Datatype;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum Error {
    #[error("Cell value size '{0}' is out of range.")]
    CellValNumOutOfRange(u32),

    #[error("Internal type error: Unhandled Arrow type: {0}")]
    InternalTypeError(adt::DataType),

    #[error("Invalid fixed sized length: {0}")]
    InvalidFixedSize(i32),

    #[error("Invalid Arrow type for conversion: '{0}'")]
    InvalidTargetType(adt::DataType),

    #[error("Failed to convert Arrow list element type: {0}")]
    ListElementTypeConversionFailed(Box<Error>),

    #[error(
        "The TileDB datatype '{0}' does not have a default Arrow DataType."
    )]
    NoDefaultArrowType(Datatype),

    #[error("Arrow type '{0} requires the TileDB field to be single valued.")]
    RequiresSingleValued(adt::DataType),

    #[error("Arrow type {0} requires the TileDB field be var sized.")]
    RequiresVarSized(adt::DataType),

    #[error("TileDB does not support timezones on timestamps")]
    TimeZonesNotSupported,

    #[error(
        "TileDB type '{0}' and Arrow type '{1}' have different physical sizes"
    )]
    PhysicalSizeMismatch(Datatype, adt::DataType),

    #[error("Unsupported Arrow DataType: {0}")]
    UnsupportedArrowDataType(adt::DataType),

    #[error("TileDB does not support lists with element type: '{0}'")]
    UnsupportedListElementType(adt::DataType),

    #[error("The Arrow DataType '{0}' is not supported.")]
    ArrowTypeNotSupported(adt::DataType),
    #[error("DataFusion does not support multi-value cells.")]
    InvalidMultiCellValNum,
    #[error("The TileDB Datatype '{0}' is not supported by DataFusion")]
    UnsupportedTileDBDatatype(Datatype),
    #[error("Variable-length datatypes as list type elements are not supported by TileDB")]
    UnsupportedListVariableLengthElement,
}

pub type Result<T> = std::result::Result<T, Error>;

/// ConversionMode dictates whether certain conversions are allowed
pub enum ConversionMode {
    /// Only allow conversions that are semantically equivalent
    Strict,
    /// Allow conversions as long as the physical type is maintained.
    Relaxed,
}

pub struct ToArrowConverter {
    mode: ConversionMode,
}

impl ToArrowConverter {
    pub fn strict() -> Self {
        Self {
            mode: ConversionMode::Strict,
        }
    }

    pub fn physical() -> Self {
        Self {
            mode: ConversionMode::Relaxed,
        }
    }

    pub fn convert_datatype(
        &self,
        dtype: &Datatype,
        cvn: &CellValNum,
        nullable: bool,
    ) -> Result<adt::DataType> {
        if let Some(arrow_type) = self.default_arrow_type(dtype) {
            self.convert_datatype_to(dtype, cvn, nullable, arrow_type)
        } else {
            Err(Error::NoDefaultArrowType(*dtype))
        }
    }

    pub fn convert_datatype_to(
        &self,
        dtype: &Datatype,
        cvn: &CellValNum,
        nullable: bool,
        arrow_type: adt::DataType,
    ) -> Result<adt::DataType> {
        if matches!(arrow_type, adt::DataType::Null) {
            return Err(Error::InvalidTargetType(arrow_type));
        }

        if arrow_type.is_primitive() {
            let width = arrow_type.primitive_width().unwrap();
            if width != dtype.size() as usize {
                return Err(Error::PhysicalSizeMismatch(*dtype, arrow_type));
            }

            if cvn.is_single_valued() {
                return Ok(arrow_type);
            } else if cvn.is_var_sized() {
                let field =
                    Arc::new(adt::Field::new("item", arrow_type, nullable));
                return Ok(adt::DataType::LargeList(field));
            } else {
                // SAFETY: Due to the logic above we can guarantee that this
                // is a fixed length cvn.
                let cvn = cvn.fixed().unwrap().get();
                if cvn > i32::MAX as u32 {
                    return Err(Error::CellValNumOutOfRange(cvn));
                }
                let field =
                    Arc::new(adt::Field::new("item", arrow_type, nullable));
                return Ok(adt::DataType::FixedSizeList(field, cvn as i32));
            }
        } else if matches!(arrow_type, adt::DataType::Boolean) {
            if !cvn.is_single_valued() {
                return Err(Error::RequiresSingleValued(arrow_type));
            }
            return Ok(arrow_type);
        } else if matches!(
            arrow_type,
            adt::DataType::LargeBinary | adt::DataType::LargeUtf8
        ) {
            if !cvn.is_var_sized() {
                return Err(Error::RequiresVarSized(arrow_type));
            }
            return Ok(arrow_type);
        } else {
            return Err(Error::InternalTypeError(arrow_type));
        }
    }

    fn default_arrow_type(&self, dtype: &Datatype) -> Option<adt::DataType> {
        use crate::datatype::Datatype as tiledb;
        use arrow::datatypes::DataType as arrow;
        let arrow_type = match dtype {
            // Any <-> Null, both indicate lack of a type
            tiledb::Any => Some(arrow::Null),

            // Boolean, n.b., this requires a byte array to bit array converesion
            tiledb::Boolean => Some(arrow::Boolean),

            // Char -> Int8
            tiledb::Char => Some(arrow::Int8),

            // Standard primitive types
            tiledb::Int8 => Some(arrow::Int8),
            tiledb::Int16 => Some(arrow::Int16),
            tiledb::Int32 => Some(arrow::Int32),
            tiledb::Int64 => Some(arrow::Int64),
            tiledb::UInt8 => Some(arrow::UInt8),
            tiledb::UInt16 => Some(arrow::UInt16),
            tiledb::UInt32 => Some(arrow::UInt32),
            tiledb::UInt64 => Some(arrow::UInt64),
            tiledb::Float32 => Some(arrow::Float32),
            tiledb::Float64 => Some(arrow::Float64),

            // Supportable datetime types
            tiledb::DateTimeSecond => {
                Some(arrow::Timestamp(adt::TimeUnit::Second, None))
            }
            tiledb::DateTimeMillisecond => {
                Some(arrow::Timestamp(adt::TimeUnit::Millisecond, None))
            }
            tiledb::DateTimeMicrosecond => {
                Some(arrow::Timestamp(adt::TimeUnit::Microsecond, None))
            }
            tiledb::DateTimeNanosecond => {
                Some(arrow::Timestamp(adt::TimeUnit::Nanosecond, None))
            }

            // Supportable time types
            tiledb::TimeSecond => Some(arrow::Time64(adt::TimeUnit::Second)),
            tiledb::TimeMillisecond => {
                Some(arrow::Time64(adt::TimeUnit::Millisecond))
            }
            tiledb::TimeMicrosecond => {
                Some(arrow::Time64(adt::TimeUnit::Microsecond))
            }
            tiledb::TimeNanosecond => {
                Some(arrow::Time64(adt::TimeUnit::Nanosecond))
            }

            // Supported string types
            tiledb::StringAscii => Some(arrow::LargeUtf8),
            tiledb::StringUtf8 => Some(arrow::LargeUtf8),

            // Blob <-> Binary
            tiledb::Blob => Some(arrow::LargeBinary),

            tiledb::StringUtf16
            | tiledb::StringUtf32
            | tiledb::StringUcs2
            | tiledb::StringUcs4
            | tiledb::DateTimeYear
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
            | tiledb::TimeAttosecond
            | tiledb::GeometryWkb
            | tiledb::GeometryWkt => None,
        };

        if arrow_type.is_some() {
            return arrow_type;
        }

        // If we're doing a strict semantic conversion we don't attempt to find
        // a matching physical type.
        if matches!(self.mode, ConversionMode::Strict) {
            return None;
        }

        // Assert in case we add more conversion modes in the future.
        assert!(matches!(self.mode, ConversionMode::Relaxed));

        // Physical conversions means we'll allow dropping the TileDB semantic
        // information to allow for raw data access.
        match dtype {
            // Uncommon string types
            tiledb::StringUtf16 => Some(arrow::UInt16),
            tiledb::StringUtf32 => Some(arrow::UInt32),
            tiledb::StringUcs2 => Some(arrow::UInt16),
            tiledb::StringUcs4 => Some(arrow::UInt32),

            // Time types that could lose data if converted to Arrow's
            // time resolution.
            tiledb::DateTimeYear => Some(arrow::Int64),
            tiledb::DateTimeMonth => Some(arrow::Int64),
            tiledb::DateTimeWeek => Some(arrow::Int64),
            tiledb::DateTimeDay => Some(arrow::Int64),
            tiledb::DateTimeHour => Some(arrow::Int64),
            tiledb::DateTimeMinute => Some(arrow::Int64),
            tiledb::DateTimePicosecond => Some(arrow::Int64),
            tiledb::DateTimeFemtosecond => Some(arrow::Int64),
            tiledb::DateTimeAttosecond => Some(arrow::Int64),
            tiledb::TimeHour => Some(arrow::Int64),
            tiledb::TimeMinute => Some(arrow::Int64),
            tiledb::TimePicosecond => Some(arrow::Int64),
            tiledb::TimeFemtosecond => Some(arrow::Int64),
            tiledb::TimeAttosecond => Some(arrow::Int64),

            // Geometry types
            tiledb::GeometryWkb => Some(arrow::LargeBinary),
            tiledb::GeometryWkt => Some(arrow::LargeUtf8),

            // These are all of the types that have strict equivalents and
            // should have already been handled above.
            tiledb::Any
            | tiledb::Boolean
            | tiledb::Char
            | tiledb::Int8
            | tiledb::Int16
            | tiledb::Int32
            | tiledb::Int64
            | tiledb::UInt8
            | tiledb::UInt16
            | tiledb::UInt32
            | tiledb::UInt64
            | tiledb::Float32
            | tiledb::Float64
            | tiledb::DateTimeSecond
            | tiledb::DateTimeMillisecond
            | tiledb::DateTimeMicrosecond
            | tiledb::DateTimeNanosecond
            | tiledb::TimeSecond
            | tiledb::TimeMillisecond
            | tiledb::TimeMicrosecond
            | tiledb::TimeNanosecond
            | tiledb::StringAscii
            | tiledb::StringUtf8
            | tiledb::Blob => unreachable!("Strict conversion failed"),
        }
    }
}

pub struct FromArrowConverter {
    mode: ConversionMode,
}

impl FromArrowConverter {
    pub fn strict() -> Self {
        Self {
            mode: ConversionMode::Strict,
        }
    }

    pub fn relaxed() -> Self {
        Self {
            mode: ConversionMode::Relaxed,
        }
    }

    pub fn convert_datatype(
        &self,
        arrow_type: adt::DataType,
    ) -> Result<(Datatype, CellValNum, Option<bool>)> {
        use adt::DataType as arrow;
        use Datatype as tiledb;

        let single = CellValNum::single();
        let var = CellValNum::Var;

        match arrow_type {
            arrow::Null => Ok((tiledb::Any, single, None)),
            arrow::Boolean => Ok((tiledb::Boolean, single, None)),

            arrow::Int8 => Ok((tiledb::Int8, single, None)),
            arrow::Int16 => Ok((tiledb::Int16, single, None)),
            arrow::Int32 => Ok((tiledb::Int32, single, None)),
            arrow::Int64 => Ok((tiledb::Int64, single, None)),
            arrow::UInt8 => Ok((tiledb::UInt8, single, None)),
            arrow::UInt16 => Ok((tiledb::UInt16, single, None)),
            arrow::UInt32 => Ok((tiledb::UInt32, single, None)),
            arrow::UInt64 => Ok((tiledb::UInt64, single, None)),
            arrow::Float32 => Ok((tiledb::Float32, single, None)),
            arrow::Float64 => Ok((tiledb::Float64, single, None)),

            arrow::Timestamp(adt::TimeUnit::Second, None) => {
                Ok((tiledb::DateTimeSecond, single, None))
            }
            arrow::Timestamp(adt::TimeUnit::Millisecond, None) => {
                Ok((tiledb::DateTimeMillisecond, single, None))
            }
            arrow::Timestamp(adt::TimeUnit::Microsecond, None) => {
                Ok((tiledb::DateTimeMicrosecond, single, None))
            }
            arrow::Timestamp(adt::TimeUnit::Nanosecond, None) => {
                Ok((tiledb::DateTimeNanosecond, single, None))
            }
            arrow::Timestamp(_, Some(_)) => {
                return Err(Error::TimeZonesNotSupported);
            }

            arrow::Time64(adt::TimeUnit::Second) => {
                Ok((tiledb::TimeSecond, single, None))
            }
            arrow::Time64(adt::TimeUnit::Millisecond) => {
                Ok((tiledb::TimeMillisecond, single, None))
            }
            arrow::Time64(adt::TimeUnit::Microsecond) => {
                Ok((tiledb::TimeMicrosecond, single, None))
            }
            arrow::Time64(adt::TimeUnit::Nanosecond) => {
                Ok((tiledb::TimeNanosecond, single, None))
            }

            arrow::Utf8 => Ok((tiledb::StringUtf8, var, None)),
            arrow::LargeUtf8 => Ok((tiledb::StringUtf8, var, None)),
            arrow::Binary => Ok((tiledb::Blob, var, None)),
            arrow::FixedSizeBinary(cvn) => {
                if cvn < 1 {
                    return Err(Error::InvalidFixedSize(cvn));
                }
                let cvn = if cvn == 1 {
                    CellValNum::single()
                } else {
                    CellValNum::try_from(cvn as u32).unwrap()
                };
                Ok((tiledb::Blob, cvn, None))
            }
            arrow::LargeBinary => Ok((tiledb::Blob, var, None)),

            arrow::List(field) | arrow::LargeList(field) => {
                let dtype = field.data_type();
                if !dtype.is_primitive() {
                    return Err(Error::UnsupportedListElementType(
                        dtype.clone(),
                    ));
                }

                let (tdb_type, _, _) =
                    self.convert_datatype(dtype.clone()).map_err(|e| {
                        Error::ListElementTypeConversionFailed(Box::new(e))
                    })?;

                Ok((tdb_type, var, Some(field.is_nullable())))
            }

            arrow::FixedSizeList(field, cvn) => {
                let dtype = field.data_type();
                if !dtype.is_primitive() {
                    return Err(Error::UnsupportedListElementType(
                        dtype.clone(),
                    ));
                }

                let (tdb_type, _, _) =
                    self.convert_datatype(dtype.clone()).map_err(|e| {
                        Error::ListElementTypeConversionFailed(Box::new(e))
                    })?;

                Ok((
                    tdb_type,
                    CellValNum::try_from(cvn as u32).unwrap(),
                    Some(field.is_nullable()),
                ))
            }

            // A few relaxed conversions for accepting Arrow types that don't
            // line up directly with TileDB.
            arrow::Date32 if self.is_relaxed() => {
                Ok((tiledb::Int32, single, None))
            }

            arrow::Date64 if self.is_relaxed() => {
                Ok((tiledb::Int64, single, None))
            }

            arrow::Time32(_) if self.is_relaxed() => {
                Ok((tiledb::Int32, single, None))
            }

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
            arrow::Float16
            | arrow::Date32
            | arrow::Date64
            | arrow::Time32(_)
            | arrow::Duration(_)
            | arrow::Interval(_)
            | arrow::BinaryView
            | arrow::Utf8View
            | arrow::ListView(_)
            | arrow::LargeListView(_)
            | arrow::Struct(_)
            | arrow::Union(_, _)
            | arrow::Dictionary(_, _)
            | arrow::Decimal128(_, _)
            | arrow::Decimal256(_, _)
            | arrow::Map(_, _)
            | arrow::RunEndEncoded(_, _) => {
                return Err(Error::UnsupportedArrowDataType(arrow_type));
            }
        }
    }

    fn is_relaxed(&self) -> bool {
        matches!(self.mode, ConversionMode::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Datatype;

    /// Test that a datatype is supported as a scalar type
    /// if and only if it is also supported as a list element type
    #[test]
    fn list_unsupported_element() {
        let conv = ToArrowConverter::strict();
        for dt in Datatype::iter() {
            let single_to_arrow =
                conv.convert_datatype(&dt, &CellValNum::single(), false);
            let var_to_arrow =
                conv.convert_datatype(&dt, &CellValNum::Var, false);

            if let Err(Error::RequiresVarSized(_)) = single_to_arrow {
                assert!(var_to_arrow.is_ok());
            } else if single_to_arrow.is_err() {
                assert_eq!(single_to_arrow, var_to_arrow);
            }

            if var_to_arrow.is_err() {
                assert_eq!(var_to_arrow, single_to_arrow);
            }
        }
    }
}
