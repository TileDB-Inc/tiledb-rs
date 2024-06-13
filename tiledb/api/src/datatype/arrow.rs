use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;

use arrow::datatypes::{
    ArrowNativeTypeOp, ArrowPrimitiveType, Field, TimeUnit,
};

use crate::array::CellValNum;
use crate::Datatype;

pub trait ArrowPrimitiveTypeNative: ArrowNativeTypeOp {
    type ArrowPrimitiveType: ArrowPrimitiveType<Native = Self>;
}

impl ArrowPrimitiveTypeNative for i8 {
    type ArrowPrimitiveType = arrow::datatypes::Int8Type;
}

impl ArrowPrimitiveTypeNative for i16 {
    type ArrowPrimitiveType = arrow::datatypes::Int16Type;
}

impl ArrowPrimitiveTypeNative for i32 {
    type ArrowPrimitiveType = arrow::datatypes::Int32Type;
}

impl ArrowPrimitiveTypeNative for i64 {
    type ArrowPrimitiveType = arrow::datatypes::Int64Type;
}

impl ArrowPrimitiveTypeNative for u8 {
    type ArrowPrimitiveType = arrow::datatypes::UInt8Type;
}

impl ArrowPrimitiveTypeNative for u16 {
    type ArrowPrimitiveType = arrow::datatypes::UInt16Type;
}

impl ArrowPrimitiveTypeNative for u32 {
    type ArrowPrimitiveType = arrow::datatypes::UInt32Type;
}

impl ArrowPrimitiveTypeNative for u64 {
    type ArrowPrimitiveType = arrow::datatypes::UInt64Type;
}

impl ArrowPrimitiveTypeNative for f32 {
    type ArrowPrimitiveType = arrow::datatypes::Float32Type;
}

impl ArrowPrimitiveTypeNative for f64 {
    type ArrowPrimitiveType = arrow::datatypes::Float64Type;
}

/// Represents tiledb (`Datatype`, `CellValNum`) compatibility for an arrow `DataType`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DatatypeToArrowResult {
    /// There is an exact logical match for the tiledb `Datatype`.
    /// The individual values of the respective types have the same bit width
    /// and are meant to be interpreted the same way.
    ///
    /// In general, this means that:
    /// 1. `CellValNum::Fixed(1)` maps to an arrow primitive or date/time type.
    /// ```
    /// use tiledb::{array::CellValNum, datatype::arrow::DatatypeToArrowResult};
    /// assert_eq!(DatatypeToArrowResult::Exact(arrow::datatypes::DataType::UInt8),
    ///            tiledb::datatype::arrow::to_arrow(&tiledb::Datatype::UInt8, CellValNum::single()));
    /// ```
    /// 2. `CellValNum::Fixed(n) if n > 1` 1 maps to an arrow fixed size list.
    /// ```
    /// use arrow::datatypes::DataType as Arrow;
    /// use tiledb::{Datatype as TileDB, array::CellValNum, datatype::arrow::DatatypeToArrowResult};
    /// let arrow = tiledb::datatype::arrow::to_arrow(&TileDB::UInt8, CellValNum::try_from(8).unwrap());
    /// let DatatypeToArrowResult::Exact(Arrow::FixedSizeList(item, fixed_len)) = arrow else { unreachable!() };
    /// assert_eq!(*item.data_type(), Arrow::UInt8);
    /// assert_eq!(fixed_len, 8);
    /// ```
    /// 3. `CellValNum::Var` maps to an arrow `LargeList`.
    /// ```
    /// use arrow::datatypes::DataType as Arrow;
    /// use tiledb::{Datatype as TileDB, array::CellValNum, datatype::arrow::DatatypeToArrowResult};
    /// let arrow = tiledb::datatype::arrow::to_arrow(&TileDB::UInt8, CellValNum::Var);
    /// let DatatypeToArrowResult::Exact(Arrow::LargeList(item)) = arrow else { unreachable!() };
    /// assert_eq!(*item.data_type(), Arrow::UInt8);
    /// ```
    ///
    /// There are some exceptions, such as `(Datatype::Blob, CellValNum::Var)`
    /// mapping to `arrow::datatypes::DataType::LargeBinary`, which is always variable-length.
    ///
    /// ```
    /// use tiledb::{array::CellValNum, datatype::arrow::DatatypeToArrowResult};
    /// assert_eq!(DatatypeToArrowResult::Exact(arrow::datatypes::DataType::LargeBinary),
    ///            tiledb::datatype::arrow::to_arrow(&tiledb::Datatype::Blob, CellValNum::Var));
    /// ```
    /// When the output is any kind of list, field metadata may be used to represent the exact
    /// input datatype if the input on its own is an inexact match.
    /// ```
    /// use arrow::datatypes::DataType as Arrow;
    /// use tiledb::{Datatype as TileDB, array::CellValNum, datatype::arrow::DatatypeToArrowResult};
    /// use tiledb::datatype::arrow::{to_arrow, ARROW_FIELD_METADATA_KEY_TILEDB_TYPE_HINT};
    /// let arrow = to_arrow(&TileDB::StringAscii, CellValNum::Var);
    /// let DatatypeToArrowResult::Exact(Arrow::LargeList(item)) = arrow else { unreachable!() };
    /// assert_eq!(*item.data_type(), Arrow::UInt8);
    /// let Some(s) = item.metadata().get(ARROW_FIELD_METADATA_KEY_TILEDB_TYPE_HINT)
    /// else { unreachable!() };
    /// assert_eq!(Some(TileDB::StringAscii), TileDB::from_string(s));
    /// ```
    Exact(arrow::datatypes::DataType),
    /// There is no corresponding logical data type, but a physical data type
    /// with the same bit width can be used to represent primitive values,
    /// and there is a trivial or cheap conversion between value structural data.
    /// ```
    /// use tiledb::{array::CellValNum, datatype::arrow::DatatypeToArrowResult};
    /// assert_eq!(DatatypeToArrowResult::Inexact(arrow::datatypes::DataType::UInt8),
    ///            tiledb::datatype::arrow::to_arrow(&tiledb::Datatype::StringAscii, CellValNum::single()));
    /// ```
    Inexact(arrow::datatypes::DataType),
}

impl DatatypeToArrowResult {
    pub fn is_inexact(&self) -> bool {
        matches!(self, Self::Inexact(_))
    }

    pub fn is_exact(&self) -> bool {
        matches!(self, Self::Exact(_))
    }

    pub fn into_inner(self) -> arrow::datatypes::DataType {
        match self {
            Self::Exact(arrow) => arrow,
            Self::Inexact(arrow) => arrow,
        }
    }
}

/*
 * (Datatype::StringAscii, CellValNum::Var) does not have an exact analog in Arrow.
 * Utf8 sounds pretty good, but we can't use it because Arrow validates Utf8 and
 * tiledb does not. So we use `LargeList(UInt8)` instead.
 * However, in tiledb StringAscii has several special accommodations which
 * are not granted to UInt8. We must be able to invert back to StringAscii.
 * We can do that by storing the exact input datatype on the arrow list field metadata.
 */
/// `arrow::datatypes::Field` metadata key for the original `tiledb::Datatype` variant
/// if there is no exact mapping from `tiledb::Datatype` to `arrow::datatypes::DataType`.
pub const ARROW_FIELD_METADATA_KEY_TILEDB_TYPE_HINT: &str = "tiledb_type_hint";

pub fn to_arrow(
    datatype: &Datatype,
    cell_val_num: CellValNum,
) -> DatatypeToArrowResult {
    use arrow::datatypes::DataType as ADT;

    type Res = DatatypeToArrowResult;

    match cell_val_num {
        CellValNum::Fixed(nz) if nz.get() == 1 => {
            match datatype {
                Datatype::Int8 => Res::Exact(ADT::Int8),
                Datatype::Int16 => Res::Exact(ADT::Int16),
                Datatype::Int32 => Res::Exact(ADT::Int32),
                Datatype::Int64 => Res::Exact(ADT::Int64),
                Datatype::UInt8 => Res::Exact(ADT::UInt8),
                Datatype::UInt16 => Res::Exact(ADT::UInt16),
                Datatype::UInt32 => Res::Exact(ADT::UInt32),
                Datatype::UInt64 => Res::Exact(ADT::UInt64),
                Datatype::Float32 => Res::Exact(ADT::Float32),
                Datatype::Float64 => Res::Exact(ADT::Float64),
                Datatype::DateTimeSecond => {
                    Res::Exact(ADT::Timestamp(TimeUnit::Second, None))
                }
                Datatype::DateTimeMillisecond => {
                    Res::Exact(ADT::Timestamp(TimeUnit::Millisecond, None))
                }
                Datatype::DateTimeMicrosecond => {
                    Res::Exact(ADT::Timestamp(TimeUnit::Microsecond, None))
                }
                Datatype::DateTimeNanosecond => {
                    Res::Exact(ADT::Timestamp(TimeUnit::Nanosecond, None))
                }
                Datatype::TimeMicrosecond => {
                    Res::Exact(ADT::Time64(TimeUnit::Microsecond))
                }
                Datatype::TimeNanosecond => {
                    Res::Exact(ADT::Time64(TimeUnit::Nanosecond))
                }
                Datatype::Char => Res::Inexact(ADT::Int8),
                Datatype::StringAscii => Res::Inexact(ADT::UInt8),
                Datatype::StringUtf8 => Res::Inexact(ADT::UInt8),
                Datatype::StringUtf16 => Res::Inexact(ADT::UInt16),
                Datatype::StringUtf32 => Res::Inexact(ADT::UInt32),
                Datatype::StringUcs2 => Res::Inexact(ADT::UInt16),
                Datatype::StringUcs4 => Res::Inexact(ADT::UInt32),
                Datatype::DateTimeDay
                | Datatype::DateTimeYear
                | Datatype::DateTimeMonth
                | Datatype::DateTimeWeek
                | Datatype::DateTimeHour
                | Datatype::DateTimeMinute
                | Datatype::DateTimePicosecond
                | Datatype::DateTimeFemtosecond
                | Datatype::DateTimeAttosecond
                | Datatype::TimeHour
                | Datatype::TimeMinute
                | Datatype::TimeSecond
                | Datatype::TimeMillisecond
                | Datatype::TimePicosecond
                | Datatype::TimeFemtosecond
                | Datatype::TimeAttosecond => {
                    // these are signed 64-bit integers in tiledb,
                    // arrow datetypes with the same precision are 32 bits
                    // (or there is no equivalent time unit)
                    Res::Inexact(ADT::Int64)
                }
                Datatype::Blob
                | Datatype::Boolean
                | Datatype::GeometryWkb
                | Datatype::GeometryWkt => Res::Inexact(ADT::UInt8),
                Datatype::Any => {
                    // note that this likely is unreachable if the tiledb API is used
                    // correctly, as `Datatype::Any` requires `CellValNum::Var`
                    Res::Inexact(ADT::UInt8)
                }
            }
        }
        CellValNum::Fixed(nz) => match i32::try_from(nz.get()) {
            Ok(nz) => {
                if matches!(datatype, Datatype::Blob) {
                    Res::Exact(ADT::FixedSizeBinary(nz))
                } else {
                    match to_arrow(datatype, CellValNum::single()) {
                        Res::Exact(item) => Res::Exact(ADT::FixedSizeList(
                            Arc::new(arrow::datatypes::Field::new_list_field(
                                item, false,
                            )),
                            nz,
                        )),
                        Res::Inexact(item) => {
                            let metadata = HashMap::from_iter([(
                                ARROW_FIELD_METADATA_KEY_TILEDB_TYPE_HINT
                                    .to_string(),
                                datatype.to_string(),
                            )]);

                            let item = Arc::new(
                                Field::new_list_field(item, false)
                                    .with_metadata(metadata),
                            );
                            Res::Exact(ADT::FixedSizeList(item, nz))
                        }
                    }
                }
            }
            Err(_) => unimplemented!(),
        },
        CellValNum::Var => {
            if let Datatype::Blob = datatype {
                Res::Exact(ADT::LargeBinary)
            } else {
                /*
                 * TODO:
                 * We could, and probably ought to, treat Utf8 in a similar fashion
                 * to LargeBinary as above. However, arrow (in contrast to tiledb)
                 * actually does to a UTF-8 integrity check. Until tiledb also
                 * does that, and we update our test strategies to generate
                 * valid UTF-8 sequences, we cannot do so.
                 */
                match to_arrow(datatype, CellValNum::single()) {
                    Res::Exact(item) => {
                        let item = Arc::new(Field::new_list_field(item, false));
                        Res::Exact(ADT::LargeList(item))
                    }
                    Res::Inexact(item) => {
                        let metadata = HashMap::from_iter([(
                            ARROW_FIELD_METADATA_KEY_TILEDB_TYPE_HINT
                                .to_string(),
                            datatype.to_string(),
                        )]);
                        let item = Arc::new(
                            Field::new_list_field(item, false)
                                .with_metadata(metadata),
                        );
                        Res::Exact(ADT::LargeList(item))
                    }
                }
            }
        }
    }
}

/// Represents arrow type compatibility for a tiledb `Datatype` paired with a `CellValNum`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum DatatypeFromArrowResult {
    /// There is no reasonable matching type in tiledb.
    /// This includes, but is not limited to,
    /// types with 32-bit offsets; complex data types; view types; decimal types; and the null type.
    None,
    /// There is an exact logical match for the arrow `DataType`.
    /// The individual values of the respective types have the same bit width
    /// and are meant to be interpreted the same way.
    /// ```
    /// use arrow::datatypes::DataType as Arrow;
    /// use tiledb::{Datatype as TileDB, array::CellValNum};
    /// use tiledb::datatype::arrow::{from_arrow, DatatypeFromArrowResult};
    /// let tiledb = from_arrow(&Arrow::new_large_list(Arrow::Date32, false));
    /// assert_eq!(DatatypeFromArrowResult::Inexact(TileDB::Int32, CellValNum::Var), tiledb);
    /// ```
    Exact(Datatype, CellValNum),
    /// There is no corresponding logical data type, but a physical data type
    /// with the same bit width can be used to represent primitive values,
    /// and there is a trivial or cheap conversion between value structural data.
    /// ```
    /// use arrow::datatypes::DataType as Arrow;
    /// use tiledb::{Datatype as TileDB, array::CellValNum};
    /// use tiledb::datatype::arrow::{from_arrow, DatatypeFromArrowResult};
    /// let tiledb = from_arrow(&Arrow::Date32);
    /// assert_eq!(DatatypeFromArrowResult::Inexact(TileDB::Int32, CellValNum::single()), tiledb);
    /// ```
    Inexact(Datatype, CellValNum),
}

impl DatatypeFromArrowResult {
    pub fn is_inexact(&self) -> bool {
        matches!(self, Self::Inexact(_, _))
    }

    pub fn is_exact(&self) -> bool {
        matches!(self, Self::Exact(_, _))
    }

    pub fn ok(self) -> Option<(Datatype, CellValNum)> {
        match self {
            Self::None => None,
            Self::Exact(dt, cv) => Some((dt, cv)),
            Self::Inexact(dt, cv) => Some((dt, cv)),
        }
    }
}

pub fn from_arrow(
    value: &arrow::datatypes::DataType,
) -> DatatypeFromArrowResult {
    use arrow::datatypes::DataType as ADT;

    type Res = DatatypeFromArrowResult;

    match value {
        ADT::Null => Res::None,
        ADT::Int8 => Res::Exact(Datatype::Int8, CellValNum::single()),
        ADT::Int16 => Res::Exact(Datatype::Int16, CellValNum::single()),
        ADT::Int32 => Res::Exact(Datatype::Int32, CellValNum::single()),
        ADT::Int64 => Res::Exact(Datatype::Int64, CellValNum::single()),
        ADT::UInt8 => Res::Exact(Datatype::UInt8, CellValNum::single()),
        ADT::UInt16 => Res::Exact(Datatype::UInt16, CellValNum::single()),
        ADT::UInt32 => Res::Exact(Datatype::UInt32, CellValNum::single()),
        ADT::UInt64 => Res::Exact(Datatype::UInt64, CellValNum::single()),
        ADT::Float16 => {
            /* tiledb has no f16 type, so use u16 as a 2-byte container */
            Res::Inexact(Datatype::UInt16, CellValNum::single())
        }
        ADT::Float32 => Res::Exact(Datatype::Float32, CellValNum::single()),
        ADT::Float64 => Res::Exact(Datatype::Float64, CellValNum::single()),
        ADT::Decimal128(_, _) | ADT::Decimal256(_, _) => {
            /*
             * We could map this to fixed-length blob but probably
             * better to do a proper 128 or 256 bit thing in core
             * so we avoid making mistakes here
             */
            Res::None
        }
        ADT::Timestamp(TimeUnit::Second, _) => {
            Res::Exact(Datatype::DateTimeSecond, CellValNum::single())
        }
        ADT::Timestamp(TimeUnit::Millisecond, _) => {
            Res::Exact(Datatype::DateTimeMillisecond, CellValNum::single())
        }
        ADT::Timestamp(TimeUnit::Microsecond, _) => {
            Res::Exact(Datatype::DateTimeMicrosecond, CellValNum::single())
        }
        ADT::Timestamp(TimeUnit::Nanosecond, _) => {
            Res::Exact(Datatype::DateTimeNanosecond, CellValNum::single())
        }
        ADT::Date32 | ADT::Time32(_) => {
            Res::Inexact(Datatype::Int32, CellValNum::single())
        }
        ADT::Date64 => {
            Res::Inexact(Datatype::DateTimeMillisecond, CellValNum::single())
        }
        ADT::Time64(TimeUnit::Microsecond) => {
            Res::Exact(Datatype::TimeMicrosecond, CellValNum::single())
        }
        ADT::Time64(TimeUnit::Nanosecond) => {
            Res::Exact(Datatype::TimeNanosecond, CellValNum::single())
        }
        ADT::Time64(_) => Res::Inexact(Datatype::UInt64, CellValNum::single()),
        ADT::Boolean => {
            /* this may be bit-packed by arrow but is not by tiledb */
            Res::None
        }
        ADT::Duration(_) | ADT::Interval(_) => {
            /* these are scalars but the doc does not specify bit width */
            Res::None
        }
        ADT::LargeBinary => Res::Exact(Datatype::Blob, CellValNum::Var),
        ADT::FixedSizeBinary(len) => match u32::try_from(*len) {
            Ok(len) => match NonZeroU32::new(len) {
                None => Res::None,
                Some(nz) => Res::Exact(Datatype::Blob, CellValNum::Fixed(nz)),
            },
            Err(_) => Res::None,
        },
        ADT::FixedSizeList(ref item, ref len) => {
            let len = match u32::try_from(*len).ok().and_then(NonZeroU32::new) {
                Some(len) => len,
                None => return Res::None,
            };
            if item.is_nullable() {
                // tiledb validity applies to the entire cell, not the values within the cell.
                // there is currently no way to represent null values within a cell
                Res::None
            } else if item.data_type().primitive_width().is_none() {
                /*
                 * probably there are some cases we can handle,
                 * but let's omit for now
                 */
                Res::None
            } else if let Some(exact_datatype) = item
                .metadata()
                .get(ARROW_FIELD_METADATA_KEY_TILEDB_TYPE_HINT)
                .and_then(|s| Datatype::from_string(s))
            {
                Res::Exact(exact_datatype, CellValNum::Fixed(len))
            } else {
                match from_arrow(item.data_type()) {
                    Res::None => Res::None,
                    Res::Inexact(item, item_cell_val) => {
                        let cell_val_num = match item_cell_val {
                            CellValNum::Fixed(nz) => {
                                match nz.checked_mul(len) {
                                    None => return Res::None,
                                    Some(nz) => CellValNum::Fixed(nz),
                                }
                            }
                            CellValNum::Var => CellValNum::Var,
                        };
                        Res::Inexact(item, cell_val_num)
                    }
                    Res::Exact(item, item_cell_val) => {
                        let cell_val_num = match item_cell_val {
                            CellValNum::Fixed(nz) => {
                                match nz.checked_mul(len) {
                                    None => return Res::None,
                                    Some(nz) => CellValNum::Fixed(nz),
                                }
                            }
                            CellValNum::Var => CellValNum::Var,
                        };
                        Res::Exact(item, cell_val_num)
                    }
                }
            }
        }
        ADT::Utf8 | ADT::Utf8View | ADT::LargeUtf8 => {
            /*
             * NB: arrow checks for valid UTF-8 but tiledb does not.
             * This is not an exact conversion for that reason
             * because we cannot guarantee invertibility.
             */
            Res::Inexact(Datatype::StringUtf8, CellValNum::Var)
        }
        ADT::LargeList(ref item) => {
            if item.is_nullable() {
                // tiledb validity applies to the entire cell, not the values within the cell.
                // there is currently no way to represent null values within a cell
                Res::None
            } else if item.data_type().primitive_width().is_none() {
                /*
                 * probably there are some cases we can handle,
                 * but let's omit for now
                 */
                Res::None
            } else if let Some(exact_datatype) = item
                .metadata()
                .get(ARROW_FIELD_METADATA_KEY_TILEDB_TYPE_HINT)
                .and_then(|s| Datatype::from_string(s))
            {
                Res::Exact(exact_datatype, CellValNum::Var)
            } else {
                match from_arrow(item.data_type()) {
                    Res::None => Res::None,
                    Res::Inexact(item, CellValNum::Fixed(nz))
                        if nz.get() == 1 =>
                    {
                        Res::Inexact(item, CellValNum::Var)
                    }
                    Res::Exact(item, CellValNum::Fixed(nz))
                        if nz.get() == 1 =>
                    {
                        Res::Exact(item, CellValNum::Var)
                    }
                    _ => {
                        /*
                         * We probably *can* fill in more cases, but either:
                         * 1) we need to do work to keep the fixed cell val num around, doable but
                         *    why bother right now
                         * 2) we need to keep multiple levels of offsets, not supported right now
                         */
                        Res::None
                    }
                }
            }
        }
        ADT::Binary | ADT::List(_) => {
            /* offsets are 64 bits, these types use 32-bit offsets */
            Res::None
        }
        ADT::BinaryView | ADT::ListView(_) | ADT::LargeListView(_) => {
            /* data does not arrive from tiledb core in this format */
            Res::None
        }
        ADT::Struct(_)
        | ADT::Union(_, _)
        | ADT::Dictionary(_, _)
        | ADT::Map(_, _)
        | ADT::RunEndEncoded(_, _) => {
            /* complex types are not implemented */
            Res::None
        }
    }
}

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy {
    use std::sync::Arc;

    use proptest::prelude::*;

    #[derive(Clone, Debug)]
    pub struct FieldParameters {
        pub min_fixed_binary_len: i32,
        pub max_fixed_binary_len: i32,
        pub min_numeric_precision: u8,
        pub max_numeric_precision: u8,
        pub min_fixed_list_len: i32,
        pub max_fixed_list_len: i32,
        pub min_struct_fields: usize,
        pub max_struct_fields: usize,
        pub min_recursion_depth: u32,
        pub max_recursion_depth: u32,
    }

    impl Default for FieldParameters {
        fn default() -> Self {
            const DEFAULT_MAX_FIXED_BINARY_LEN: i32 = 1024 * 1024;
            const DEFAULT_MAX_FIXED_LIST_LEN: i32 = 2048;

            FieldParameters {
                min_fixed_binary_len: 1,
                max_fixed_binary_len: DEFAULT_MAX_FIXED_BINARY_LEN,
                min_numeric_precision: 1,
                max_numeric_precision: u8::MAX,
                min_fixed_list_len: 0,
                max_fixed_list_len: DEFAULT_MAX_FIXED_LIST_LEN,
                min_struct_fields: 0,
                max_struct_fields: 16,
                min_recursion_depth: 0,
                max_recursion_depth: 8,
            }
        }
    }

    pub fn any_datatype(
        params: FieldParameters,
    ) -> impl Strategy<Value = arrow::datatypes::DataType> {
        use arrow::datatypes::{
            DataType as ADT, Field, Fields, IntervalUnit, TimeUnit,
        };

        let leaf = prop_oneof![
            Just(ADT::Null),
            Just(ADT::Int8),
            Just(ADT::Int16),
            Just(ADT::Int32),
            Just(ADT::Int64),
            Just(ADT::UInt8),
            Just(ADT::UInt16),
            Just(ADT::UInt32),
            Just(ADT::UInt64),
            Just(ADT::Float16),
            Just(ADT::Float32),
            Just(ADT::Float64),
            Just(ADT::Timestamp(TimeUnit::Second, None)),
            Just(ADT::Timestamp(TimeUnit::Millisecond, None)),
            Just(ADT::Timestamp(TimeUnit::Microsecond, None)),
            Just(ADT::Timestamp(TimeUnit::Nanosecond, None)),
            Just(ADT::Date32),
            Just(ADT::Date64),
            Just(ADT::Time32(TimeUnit::Second)),
            Just(ADT::Time32(TimeUnit::Millisecond)),
            Just(ADT::Time64(TimeUnit::Microsecond)),
            Just(ADT::Time64(TimeUnit::Nanosecond)),
            Just(ADT::Duration(TimeUnit::Second)),
            Just(ADT::Duration(TimeUnit::Millisecond)),
            Just(ADT::Duration(TimeUnit::Nanosecond)),
            Just(ADT::Interval(IntervalUnit::YearMonth)),
            Just(ADT::Interval(IntervalUnit::DayTime)),
            Just(ADT::Interval(IntervalUnit::MonthDayNano)),
            Just(ADT::Binary),
            (params.min_fixed_binary_len..=params.max_fixed_binary_len)
                .prop_map(ADT::FixedSizeBinary),
            Just(ADT::LargeBinary),
            Just(ADT::Utf8),
            Just(ADT::LargeUtf8),
            (params.min_numeric_precision..=params.max_numeric_precision)
                .prop_flat_map(|precision| (
                    Just(precision),
                    (0..precision.clamp(0, i8::MAX as u8) as i8)
                )
                    .prop_map(|(precision, scale)| ADT::Decimal128(
                        precision, scale
                    ))),
            (params.min_numeric_precision..=params.max_numeric_precision)
                .prop_flat_map(|precision| (
                    Just(precision),
                    (0..precision.clamp(0, i8::MAX as u8) as i8)
                )
                    .prop_map(|(precision, scale)| ADT::Decimal256(
                        precision, scale
                    ))),
        ];

        leaf.prop_recursive(
            params.max_recursion_depth,
            params.max_recursion_depth * 4,
            std::cmp::max(
                2,
                (params.max_struct_fields / 4).try_into().unwrap(),
            ),
            move |strategy| {
                prop_oneof![
                    (strategy.clone(), any::<bool>())
                        .prop_map(|(s, b)| ADT::new_list(s, b)),
                    (
                        strategy.clone(),
                        params.min_fixed_list_len..=params.max_fixed_list_len,
                        any::<bool>()
                    )
                        .prop_map(|(s, l, b)| ADT::FixedSizeList(
                            Arc::new(Field::new_list_field(s, b)),
                            l
                        )),
                    (strategy.clone(), any::<bool>()).prop_map(|(s, b)| {
                        ADT::LargeList(Arc::new(Field::new_list_field(s, b)))
                    }),
                    proptest::collection::vec(
                        (
                            crate::array::attribute::strategy::prop_attribute_name(
                            ),
                            strategy.clone(),
                            any::<bool>()
                        ),
                        params.min_struct_fields..=params.max_struct_fields
                    )
                    .prop_map(|v| ADT::Struct(
                        v.into_iter()
                            .map(|(n, dt, b)| Field::new(n, dt, b))
                            .collect::<Fields>()
                    )) // union goes here
                       // dictionary goes here
                       // map goes here
                       // run-end encoded goes here
                ]
            },
        )
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use proptest::prelude::*;

    fn do_to_arrow_single(tdb_dt: Datatype) {
        let cell_val_num = CellValNum::single();
        let arrow_dt = to_arrow(&tdb_dt, cell_val_num);
        match arrow_dt {
            DatatypeToArrowResult::Inexact(arrow) => {
                assert!(arrow.is_primitive());
                let arrow_size = arrow.primitive_width().unwrap() as u64;
                assert_eq!(
                    tdb_dt.size(),
                    arrow_size,
                    "to_arrow({}, {:?}) = {}",
                    tdb_dt,
                    cell_val_num,
                    arrow
                );

                let tdb_out = from_arrow(&arrow);
                let (tdb_out, cell_val_num_out) = tdb_out.ok().unwrap();

                /* the datatype should not match exactly but it must be the same size */
                assert_ne!(tdb_dt, tdb_out);
                assert_eq!(tdb_dt.size(), tdb_out.size());
                assert_eq!(cell_val_num, cell_val_num_out);
            }
            DatatypeToArrowResult::Exact(arrow) => {
                assert!(arrow.is_primitive());
                let arrow_size = arrow.primitive_width().unwrap() as u64;
                assert_eq!(
                    tdb_dt.size(),
                    arrow_size,
                    "to_arrow({}, {:?}) = {}",
                    tdb_dt,
                    cell_val_num,
                    arrow
                );

                let tdb_out = from_arrow(&arrow);
                if let DatatypeFromArrowResult::Exact(
                    tdb_out,
                    cell_val_num_out,
                ) = tdb_out
                {
                    /* the datatype must match exactly */
                    assert_eq!(tdb_dt, tdb_out);
                    assert_eq!(cell_val_num, cell_val_num_out);
                } else {
                    unreachable!(
                        "Exact conversion did not invert, found {:?}",
                        tdb_out
                    )
                }
            }
        }
    }

    fn do_to_arrow_nonvar(tdb_dt: Datatype) {
        let fixed_len_in = 32u32;
        let cell_val_num = CellValNum::try_from(fixed_len_in).unwrap();
        let arrow_dt = to_arrow(&tdb_dt, cell_val_num);

        use arrow::datatypes::DataType as ADT;
        match arrow_dt {
            DatatypeToArrowResult::Inexact(arrow) => {
                match arrow {
                    ADT::FixedSizeList(ref item, fixed_len_out) => {
                        let item_expect =
                            to_arrow(&tdb_dt, CellValNum::single());
                        if let DatatypeToArrowResult::Inexact(item_expect) =
                            item_expect
                        {
                            assert_eq!(item_expect, *item.data_type());
                            assert_eq!(fixed_len_in, fixed_len_out as u32);
                        } else {
                            unreachable!(
                                "Expected inexact item match, found {:?}",
                                item_expect
                            )
                        }
                    }
                    arrow => unreachable!(
                        "Expected FixedSizeList for inexact match but found {}",
                        arrow
                    ),
                }

                /* invertibility */
                let tdb_out = from_arrow(&arrow);
                let (tdb_out, cell_val_num_out) = tdb_out.ok().unwrap();

                /* inexact match will not be eq, but must be the same size */
                assert_eq!(tdb_dt.size(), tdb_out.size());
                assert_eq!(cell_val_num, cell_val_num_out);
            }
            DatatypeToArrowResult::Exact(arrow) => {
                match arrow {
                    ADT::FixedSizeList(ref item, fixed_len_out) => {
                        if let Some(sub_exact) = item
                            .metadata()
                            .get(ARROW_FIELD_METADATA_KEY_TILEDB_TYPE_HINT)
                        {
                            let sub_exact =
                                Datatype::from_string(sub_exact).unwrap();
                            assert_eq!(sub_exact.size(), tdb_dt.size());

                            // item must have been inexact, else we would not have the metadata
                            let item_dt =
                                to_arrow(&tdb_dt, CellValNum::single());
                            if let DatatypeToArrowResult::Inexact(item_dt) =
                                item_dt
                            {
                                assert_eq!(*item.data_type(), item_dt);
                            } else {
                                unreachable!(
                                    "Expected inexact item match but found {:?}",
                                    item_dt
                                )
                            }
                        } else {
                            // item must be exact match
                            let item_dt =
                                to_arrow(&tdb_dt, CellValNum::single());
                            if let DatatypeToArrowResult::Exact(item_dt) =
                                item_dt
                            {
                                assert_eq!(*item.data_type(), item_dt);
                            } else {
                                unreachable!(
                                    "Expected exact item match but found {:?}",
                                    item_dt
                                )
                            }
                        }
                        assert_eq!(fixed_len_in, fixed_len_out as u32);
                    }
                    ADT::FixedSizeBinary(fixed_len_out) => {
                        assert_eq!(tdb_dt, Datatype::Blob);
                        assert_eq!(fixed_len_in, fixed_len_out as u32);
                    }
                    adt => unreachable!(
                        "to_arrow({}, {:?}) = {}",
                        tdb_dt, cell_val_num, adt
                    ),
                }

                /* invertibility */
                let tdb_out = from_arrow(&arrow);
                if let DatatypeFromArrowResult::Exact(
                    tdb_out,
                    cell_val_num_out,
                ) = tdb_out
                {
                    assert_eq!(tdb_dt, tdb_out);
                    assert_eq!(cell_val_num, cell_val_num_out);
                } else {
                    unreachable!(
                        "Arrow datatype did not invert, found {:?}",
                        tdb_out
                    )
                }
            }
        }
    }

    fn do_to_arrow_var(tdb_dt: Datatype) {
        let cell_val_num = CellValNum::Var;
        let arrow_dt = to_arrow(&tdb_dt, cell_val_num);

        use arrow::datatypes::DataType as ADT;
        match arrow_dt {
            DatatypeToArrowResult::Inexact(arrow) => {
                assert!(
                    !arrow.is_primitive(),
                    "to_arrow({}, {:?}) = {}",
                    tdb_dt,
                    cell_val_num,
                    arrow
                );

                if let ADT::LargeList(ref item) = arrow {
                    let item_expect = to_arrow(&tdb_dt, CellValNum::single());
                    if let DatatypeToArrowResult::Inexact(item_expect) =
                        item_expect
                    {
                        assert_eq!(*item.data_type(), item_expect);
                    } else {
                        unreachable!(
                            "Expected inexact item match, but found {:?}",
                            item_expect
                        )
                    }
                } else {
                    /* other possibilities should be Exact */
                    unreachable!(
                        "Expected LargeList for inexact match but found {:?}",
                        arrow
                    )
                }

                let tdb_out = from_arrow(&arrow);
                let (tdb_out, cell_val_num_out) = tdb_out.ok().unwrap();

                /* must be the same size */
                assert_eq!(tdb_dt.size(), tdb_out.size());
                assert_eq!(cell_val_num, cell_val_num_out);
            }
            DatatypeToArrowResult::Exact(arrow) => {
                assert!(
                    !arrow.is_primitive(),
                    "to_arrow({}, {:?}) = {}",
                    tdb_dt,
                    cell_val_num,
                    arrow
                );

                match arrow {
                    ADT::LargeList(ref item) => {
                        if let Some(sub_exact) = item
                            .metadata()
                            .get(ARROW_FIELD_METADATA_KEY_TILEDB_TYPE_HINT)
                        {
                            let sub_exact =
                                Datatype::from_string(sub_exact).unwrap();
                            assert_eq!(sub_exact.size(), tdb_dt.size());

                            // item must not have been exact, else we would not have the metadata
                            let item_dt =
                                to_arrow(&tdb_dt, CellValNum::single());
                            if let DatatypeToArrowResult::Inexact(item_dt) =
                                item_dt
                            {
                                assert_eq!(*item.data_type(), item_dt);
                            } else {
                                unreachable!(
                                    "Expected inexact item match but found {:?}",
                                    item_dt
                                )
                            }
                        } else {
                            let item_dt =
                                to_arrow(&tdb_dt, CellValNum::single());
                            if let DatatypeToArrowResult::Exact(item_dt) =
                                item_dt
                            {
                                assert_eq!(*item.data_type(), item_dt);
                            } else {
                                unreachable!(
                                    "Expected exact item match but found {:?}",
                                    item_dt
                                )
                            }
                        }
                    }
                    ADT::LargeUtf8 => assert!(matches!(
                        tdb_dt,
                        Datatype::StringAscii | Datatype::StringUtf8
                    )),
                    ADT::LargeBinary => {
                        assert!(matches!(tdb_dt, Datatype::Blob))
                    }
                    adt => unreachable!(
                        "to_arrow({}, {:?}) = {}",
                        tdb_dt, cell_val_num, adt
                    ),
                }

                let tdb_out = from_arrow(&arrow);
                if let DatatypeFromArrowResult::Exact(
                    tdb_out,
                    cell_val_num_out,
                ) = tdb_out
                {
                    assert_eq!(tdb_dt, tdb_out);
                    assert_eq!(cell_val_num, cell_val_num_out);
                } else {
                    unreachable!(
                        "Arrow datatype constructed from tiledb datatype must convert back")
                }
            }
        }
    }

    pub fn arrow_datatype_is_inexact_compatible(
        arrow_in: &arrow::datatypes::DataType,
        arrow_out: &arrow::datatypes::DataType,
    ) -> bool {
        if arrow_in == arrow_out {
            return true;
        }

        /* otherwise check some inexact compatibilities */
        use arrow::datatypes::DataType as ADT;
        match (arrow_in, arrow_out) {
            (
                ADT::FixedSizeList(ref item_in, len_in),
                ADT::FixedSizeList(ref item_out, len_out),
            ) => {
                len_in == len_out
                    && arrow_datatype_is_inexact_compatible(
                        item_in.data_type(),
                        item_out.data_type(),
                    )
            }
            (ADT::LargeList(ref item_in), ADT::LargeList(ref item_out)) => {
                arrow_datatype_is_inexact_compatible(
                    item_in.data_type(),
                    item_out.data_type(),
                )
            }
            (ADT::FixedSizeList(ref item_in, 1), dt_out) => {
                /*
                 * fixed size list of 1 element should have no extra data,
                 * we probably don't need to keep the FixedSizeList part
                 * for correctness, punt on it for now and see if we need
                 * to deal with it later
                 */
                arrow_datatype_is_inexact_compatible(
                    item_in.data_type(),
                    dt_out,
                )
            }
            (ADT::LargeUtf8, ADT::LargeList(ref item)) => {
                /*
                 * Arrow does checked UTF-8, tiledb does not,
                 * so we must permit this inexactness
                 */
                *item.data_type() == arrow::datatypes::DataType::UInt8
                    && !item.is_nullable()
            }
            (dt_in, dt_out) => {
                if dt_in.is_primitive() {
                    dt_in.primitive_width() == dt_out.primitive_width()
                } else {
                    false
                }
            }
        }
    }

    fn do_from_arrow(arrow_in: &arrow::datatypes::DataType) {
        match from_arrow(arrow_in) {
            DatatypeFromArrowResult::None => (),
            DatatypeFromArrowResult::Exact(datatype, cvn) => {
                let arrow_out = to_arrow(&datatype, cvn);
                if let DatatypeToArrowResult::Exact(arrow_out) = arrow_out {
                    assert_eq!(*arrow_in, arrow_out);
                } else {
                    unreachable!(
                        "Expected exact inversion, found {:?}",
                        arrow_out
                    )
                }
            }
            DatatypeFromArrowResult::Inexact(datatype, cvn) => {
                let arrow_out = to_arrow(&datatype, cvn);
                let arrow_out = arrow_out.into_inner();
                assert!(
                    arrow_datatype_is_inexact_compatible(arrow_in, &arrow_out),
                    "{:?} => {:?}",
                    arrow_in,
                    arrow_out
                );
            }
        }
    }

    proptest! {
        #[test]
        fn test_to_arrow_single(tdb_dt in any::<Datatype>()) {
            do_to_arrow_single(tdb_dt)
        }

        #[test]
        fn test_to_arrow_nonvar(tdb_dt in any::<Datatype>()) {
            do_to_arrow_nonvar(tdb_dt);
        }

        #[test]
        fn test_to_arrow_var(tdb_dt in any::<Datatype>()) {
            do_to_arrow_var(tdb_dt);
        }

        #[test]
        fn test_from_arrow(arrow in crate::array::attribute::arrow::strategy::prop_arrow_field()) {
            do_from_arrow(arrow.data_type());
        }
    }
}
