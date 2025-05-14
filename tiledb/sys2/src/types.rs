use arrow::datatypes::ArrowNativeType;

use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::hash::{Hash, Hasher};

use crate::datatype::{Datatype, DatatypeError};

/// Trait which provides statically-typed attributes for a TileDB `Datatype`
/// for use with generics.
pub trait LogicalType: crate::private::Sealed {
    const DATA_TYPE: Datatype;

    type PhysicalType: PhysicalType;
}

pub struct UInt8Type {}

impl LogicalType for UInt8Type {
    const DATA_TYPE: Datatype = Datatype::UInt8;

    type PhysicalType = u8;
}

pub struct UInt16Type {}

impl LogicalType for UInt16Type {
    const DATA_TYPE: Datatype = Datatype::UInt16;

    type PhysicalType = u16;
}

pub struct UInt32Type {}

impl LogicalType for UInt32Type {
    const DATA_TYPE: Datatype = Datatype::UInt32;

    type PhysicalType = u32;
}

pub struct UInt64Type {}

impl LogicalType for UInt64Type {
    const DATA_TYPE: Datatype = Datatype::UInt64;

    type PhysicalType = u64;
}

pub struct Int8Type {}

impl LogicalType for Int8Type {
    const DATA_TYPE: Datatype = Datatype::Int8;

    type PhysicalType = i8;
}

pub struct Int16Type {}

impl LogicalType for Int16Type {
    const DATA_TYPE: Datatype = Datatype::Int16;

    type PhysicalType = i16;
}

pub struct Int32Type {}

impl LogicalType for Int32Type {
    const DATA_TYPE: Datatype = Datatype::Int32;

    type PhysicalType = i32;
}

pub struct Int64Type {}

impl LogicalType for Int64Type {
    const DATA_TYPE: Datatype = Datatype::Int64;

    type PhysicalType = i64;
}

pub struct Float32Type {}

impl LogicalType for Float32Type {
    const DATA_TYPE: Datatype = Datatype::Float32;

    type PhysicalType = f32;
}

pub struct Float64Type {}

impl LogicalType for Float64Type {
    const DATA_TYPE: Datatype = Datatype::Float64;

    type PhysicalType = f64;
}

pub struct CharType {}

impl LogicalType for CharType {
    const DATA_TYPE: Datatype = Datatype::Char;
    type PhysicalType = std::ffi::c_char;
}

pub struct StringAsciiType {}

impl LogicalType for StringAsciiType {
    const DATA_TYPE: Datatype = Datatype::StringAscii;
    type PhysicalType = u8;
}

pub struct StringUtf8Type {}

impl LogicalType for StringUtf8Type {
    const DATA_TYPE: Datatype = Datatype::StringUtf8;
    type PhysicalType = u8;
}

pub struct StringUtf16Type {}

impl LogicalType for StringUtf16Type {
    const DATA_TYPE: Datatype = Datatype::StringUtf16;
    type PhysicalType = u16;
}
pub struct StringUtf32Type {}

impl LogicalType for StringUtf32Type {
    const DATA_TYPE: Datatype = Datatype::StringUtf32;
    type PhysicalType = u32;
}
pub struct StringUcs2Type {}

impl LogicalType for StringUcs2Type {
    const DATA_TYPE: Datatype = Datatype::StringUcs2;
    type PhysicalType = u16;
}

pub struct StringUcs4Type {}

impl LogicalType for StringUcs4Type {
    const DATA_TYPE: Datatype = Datatype::StringUcs4;
    type PhysicalType = u32;
}

macro_rules! datetime_type {
    ($($datetime:ident: $datetimetype:ident),+) => {
        $(
            pub struct $datetimetype {}

            impl crate::private::Sealed for $datetimetype {}

            impl LogicalType for $datetimetype {
                const DATA_TYPE: Datatype = Datatype::$datetime;
                type PhysicalType = i64;
            }
        )+
    }
}

datetime_type!(
    DateTimeYear: DateTimeYearType,
    DateTimeMonth: DateTimeMonthType,
    DateTimeWeek: DateTimeWeekType,
    DateTimeDay: DateTimeDayType,
    DateTimeHour: DateTimeHourType,
    DateTimeMinute: DateTimeMinuteType,
    DateTimeSecond: DateTimeSecondType,
    DateTimeMillisecond: DateTimeMillisecondType,
    DateTimeMicrosecond: DateTimeMicrosecondType,
    DateTimeNanosecond: DateTimeNanosecondType,
    DateTimePicosecond: DateTimePicosecondType,
    DateTimeFemtosecond: DateTimeFemtosecondType,
    DateTimeAttosecond: DateTimeAttosecondType,
    TimeHour: TimeHourType,
    TimeMinute: TimeMinuteType,
    TimeSecond: TimeSecondType,
    TimeMillisecond: TimeMillisecondType,
    TimeMicrosecond: TimeMicrosecondType,
    TimeNanosecond: TimeNanosecondType,
    TimePicosecond: TimePicosecondType,
    TimeFemtosecond: TimeFemtosecondType,
    TimeAttosecond: TimeAttosecondType
);

pub struct AnyType {}

impl LogicalType for AnyType {
    const DATA_TYPE: Datatype = Datatype::Any;
    type PhysicalType = u8;
}

pub struct BlobType {}

impl LogicalType for BlobType {
    const DATA_TYPE: Datatype = Datatype::Blob;
    type PhysicalType = u8;
}

pub struct BooleanType {}

impl LogicalType for BooleanType {
    const DATA_TYPE: Datatype = Datatype::Boolean;
    type PhysicalType = u8;
}

pub struct GeometryWkbType {}

impl LogicalType for GeometryWkbType {
    const DATA_TYPE: Datatype = Datatype::GeometryWkb;
    type PhysicalType = u8;
}

pub struct GeometryWktType {}

impl LogicalType for GeometryWktType {
    const DATA_TYPE: Datatype = Datatype::GeometryWkt;
    type PhysicalType = u8;
}

crate::private::sealed!(UInt8Type, UInt16Type, UInt32Type, UInt64Type);
crate::private::sealed!(Int8Type, Int16Type, Int32Type, Int64Type);
crate::private::sealed!(Float32Type, Float64Type);
crate::private::sealed!(
    CharType,
    StringAsciiType,
    StringUtf8Type,
    StringUtf16Type,
    StringUtf32Type,
    StringUcs2Type,
    StringUcs4Type
);
crate::private::sealed!(
    AnyType,
    BlobType,
    BooleanType,
    GeometryWktType,
    GeometryWkbType
);

/// Trait for comparisons based on value bits.
/// This exists to work around float `NaN` which is not equal to itself,
/// preventing float types from being `Eq` for generic operations.
pub trait BitsEq {
    /// Test if two values have the same bits.
    ///
    /// This is often the same as `PartialEq::eq`, but is not in the case
    /// of floats where `NaN != NaN`.
    fn bits_eq(&self, other: &Self) -> bool;

    fn bits_ne(&self, other: &Self) -> bool {
        !self.bits_eq(other)
    }
}

impl<T> BitsEq for &T
where
    T: BitsEq,
{
    fn bits_eq(&self, other: &Self) -> bool {
        (**self).bits_eq(*other)
    }
}

impl<T> BitsEq for [T]
where
    T: BitsEq,
{
    fn bits_eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self.iter().zip(other.iter()).all(|(l, r)| l.bits_eq(r))
    }
}

impl<T1, T2> BitsEq for (T1, T2)
where
    T1: BitsEq,
    T2: BitsEq,
{
    fn bits_eq(&self, other: &Self) -> bool {
        self.0.bits_eq(&other.0) && self.1.bits_eq(&other.1)
    }
}

impl<T> BitsEq for Vec<T>
where
    T: BitsEq,
{
    fn bits_eq(&self, other: &Self) -> bool {
        self.as_slice().bits_eq(other.as_slice())
    }
}

/// Trait for ordering based on value bits.
/// This exists to work around float `NaN` which prevents float from being
/// a total order for use with generic operations.
pub trait BitsOrd {
    /// Return the ordering between `self` and `other`.
    /// This function defines a total order for all values of `Self`.
    fn bits_cmp(&self, other: &Self) -> Ordering;

    /// Restrict a value to a certain interval, using `bits_cmp` as
    /// the ordering function. See `std::cmp::Ord::clamp`.
    fn bits_clamp(self, min: Self, max: Self) -> Self
    where
        Self: Sized,
    {
        assert_eq!(min.bits_cmp(&max), Ordering::Less);

        if matches!(self.bits_cmp(&min), Ordering::Less) {
            min
        } else if matches!(self.bits_cmp(&max), Ordering::Less) {
            self
        } else {
            max
        }
    }

    /// Returns `true` if `self` is less than `other` by `self.bits_cmp`.
    fn bits_lt(&self, other: &Self) -> bool {
        matches!(self.bits_cmp(other), Ordering::Less)
    }

    /// Returns `true` if `self` is less than or equal to `other` by `self.bits_cmp`.
    fn bits_le(&self, other: &Self) -> bool {
        matches!(self.bits_cmp(other), Ordering::Less | Ordering::Equal)
    }

    /// Returns `true` if `self` is greater than or equal to `other` by `self.bits_cmp`.
    fn bits_ge(&self, other: &Self) -> bool {
        matches!(self.bits_cmp(other), Ordering::Equal | Ordering::Greater)
    }

    /// Returns `true` if `self` is greater than `other` by `self.bits_cmp`.
    fn bits_gt(&self, other: &Self) -> bool {
        matches!(self.bits_cmp(other), Ordering::Greater)
    }
}

impl<T> BitsOrd for &T
where
    T: BitsOrd,
{
    fn bits_cmp(&self, other: &Self) -> Ordering {
        (**self).bits_cmp(*other)
    }
}

/// Implements lexicographic comparison of slices using the `BitsOrd` trait of the element.
impl<T> BitsOrd for [T]
where
    T: BitsOrd,
{
    fn bits_cmp(&self, other: &Self) -> Ordering {
        for (l, r) in self.iter().zip(other.iter()) {
            match l.bits_cmp(r) {
                Ordering::Less => return Ordering::Less,
                Ordering::Greater => return Ordering::Greater,
                Ordering::Equal => continue,
            }
        }

        self.len().cmp(&other.len())
    }
}

/// Implements lexicographic comparison of vectors using the `BitsOrd` trait of the element.
impl<T> BitsOrd for Vec<T>
where
    T: BitsOrd,
{
    fn bits_cmp(&self, other: &Self) -> Ordering {
        self.as_slice().bits_cmp(other.as_slice())
    }
}

/// Trait for hashing based on value bits.
/// This exists to work around float types, which do not implement `Hash`.
/// That makes generic programming on all physical types more challenging.
///
/// Types implementing `BitsHash` can be hashed by an instance of `Hasher`
/// using `BitsKeyAdapter` which adapts `Self::bits_hash` into an implementation
/// of the `Hash` trait.
pub trait BitsHash {
    fn bits_hash<H>(&self, state: &mut H)
    where
        H: Hasher;
}

impl<T> BitsHash for &T
where
    T: BitsHash,
{
    fn bits_hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        (**self).bits_hash(state)
    }
}

impl<T> BitsHash for [T]
where
    T: BitsHash,
{
    fn bits_hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        let adapted = self.iter().map(BitsKeyAdapter).collect::<Vec<_>>();
        adapted.hash(state)
    }
}

impl<T> BitsHash for Vec<T>
where
    T: BitsHash,
{
    fn bits_hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        let adapted = self.iter().map(BitsKeyAdapter).collect::<Vec<_>>();
        adapted.hash(state)
    }
}

/// Trait for generic operations on primitive data types.
///
/// Types which implement this trait can be used to interface with TileDB
/// at the lowest level due to having the same memory representation
/// in Rust and TileDB.
pub trait PhysicalType:
    BitsEq
    + BitsHash
    + BitsOrd
    + Copy
    + Debug
    + Default
    + PartialEq
    + PartialOrd
    + Send
    + Sync
    + ArrowNativeType
    + crate::private::Sealed
    + 'static
{
}

macro_rules! integral_type_impls {
    ($($T:ty: $datatype:expr),+) => {
        $crate::private::sealed!($($T),+);

        $(
            impl BitsEq for $T {
                fn bits_eq(&self, other: &Self) -> bool {
                    <Self as PartialEq>::eq(self, other)
                }
            }

            impl BitsOrd for $T {
                fn bits_cmp(&self, other: &Self) -> Ordering {
                    <Self as Ord>::cmp(self, other)
                }
            }

            impl BitsHash for $T {
                fn bits_hash<H>(&self, state: &mut H) where H: Hasher {
                    <Self as Hash>::hash(self, state)
                }
            }

            impl PhysicalType for $T {}
        )+
    }
}

integral_type_impls!(u8: Datatype::UInt8, u16: Datatype::UInt16, u32: Datatype::UInt32, u64: Datatype::UInt64);
integral_type_impls!(i8: Datatype::Int8, i16: Datatype::Int16, i32: Datatype::Int32, i64: Datatype::Int64);

impl crate::private::Sealed for f32 {}
impl crate::private::Sealed for f64 {}

impl BitsEq for f32 {
    fn bits_eq(&self, other: &Self) -> bool {
        self.to_bits() == other.to_bits()
    }
}

impl BitsOrd for f32 {
    fn bits_cmp(&self, other: &Self) -> Ordering {
        self.total_cmp(other)
    }
}

impl BitsHash for f32 {
    fn bits_hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.to_bits().bits_hash(state)
    }
}

impl PhysicalType for f32 {}

impl BitsEq for f64 {
    fn bits_eq(&self, other: &Self) -> bool {
        self.to_bits() == other.to_bits()
    }
}

impl BitsOrd for f64 {
    fn bits_cmp(&self, other: &Self) -> Ordering {
        self.total_cmp(other)
    }
}

impl BitsHash for f64 {
    fn bits_hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.to_bits().bits_hash(state)
    }
}
impl PhysicalType for f64 {}

/// Adapts a generic type to use as a key in `std` collections via
/// the `BitsEq`, `BitsOrd`, or `BitsHash` traits.
#[derive(Clone, Copy)]
pub struct BitsKeyAdapter<T>(pub T);

impl<T> Debug for BitsKeyAdapter<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        self.0.fmt(f)
    }
}

impl<T> Display for BitsKeyAdapter<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        self.0.fmt(f)
    }
}

impl<T> PartialEq for BitsKeyAdapter<T>
where
    T: BitsEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.0.bits_eq(&other.0)
    }
}

impl<T> Eq for BitsKeyAdapter<T> where T: BitsEq {}

impl<T> PartialEq<BitsKeyAdapter<&T>> for BitsKeyAdapter<T>
where
    T: BitsEq,
{
    fn eq(&self, other: &BitsKeyAdapter<&T>) -> bool {
        self.0.bits_eq(other.0)
    }
}

impl<T> Hash for BitsKeyAdapter<T>
where
    T: BitsHash,
{
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.0.bits_hash(state)
    }
}

impl<T> PartialOrd for BitsKeyAdapter<T>
where
    T: BitsEq + BitsOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}

impl<T> Ord for BitsKeyAdapter<T>
where
    T: BitsEq + BitsOrd,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.bits_cmp(&other.0)
    }
}

impl<T> PartialOrd<BitsKeyAdapter<&T>> for BitsKeyAdapter<T>
where
    T: BitsEq + BitsOrd,
{
    fn partial_cmp(&self, other: &BitsKeyAdapter<&T>) -> Option<Ordering> {
        Some(self.0.bits_cmp(other.0))
    }
}

/// Apply a generic expression `$then` with a static type binding in the identifier `$typename`
/// for a logical type corresponding to the dynamic `$datatype`.
///
/// This is similar to `physical_type_go!` but binds the logical type
/// instead of the physical type.
// note to developers: this is mimicking the C++ code
//      template <class Fn, class... Args>
//      inline auto apply_with_type(Fn&& f, Datatype type, Args&&... args)
//
#[macro_export]
macro_rules! logical_type_go {
    ($datatype:expr, $typename:ident, $then:expr) => {{
        type Datatype = $crate::datatype::Datatype;
        match $datatype {
            Datatype::Int8 => {
                type $typename = $crate::types::Int8Type;
                $then
            }
            Datatype::Int16 => {
                type $typename = $crate::types::Int16Type;
                $then
            }
            Datatype::Int32 => {
                type $typename = $crate::types::Int32Type;
                $then
            }
            Datatype::Int64 => {
                type $typename = $crate::types::Int64Type;
                $then
            }
            Datatype::UInt8 => {
                type $typename = $crate::types::UInt8Type;
                $then
            }
            Datatype::UInt16 => {
                type $typename = $crate::types::UInt16Type;
                $then
            }
            Datatype::UInt32 => {
                type $typename = $crate::types::UInt32Type;
                $then
            }
            Datatype::UInt64 => {
                type $typename = $crate::types::UInt64Type;
                $then
            }
            Datatype::Float32 => {
                type $typename = $crate::types::Float32Type;
                $then
            }
            Datatype::Float64 => {
                type $typename = $crate::types::Float64Type;
                $then
            }
            Datatype::Char => {
                type $typename = $crate::types::CharType;
                $then
            }
            Datatype::StringAscii => {
                type $typename = $crate::types::StringAsciiType;
                $then
            }
            Datatype::StringUtf8 => {
                type $typename = $crate::types::StringUtf8Type;
                $then
            }
            Datatype::StringUtf16 => {
                type $typename = $crate::types::StringUtf16Type;
                $then
            }
            Datatype::StringUtf32 => {
                type $typename = $crate::types::StringUtf32Type;
                $then
            }
            Datatype::StringUcs2 => {
                type $typename = $crate::types::StringUcs2Type;
                $then
            }
            Datatype::StringUcs4 => {
                type $typename = $crate::types::StringUcs4Type;
                $then
            }
            Datatype::Any => {
                type $typename = $crate::types::AnyType;
                $then
            }
            Datatype::DateTimeYear => {
                type $typename = $crate::types::DateTimeYearType;
                $then
            }
            Datatype::DateTimeMonth => {
                type $typename = $crate::types::DateTimeMonthType;
                $then
            }
            Datatype::DateTimeWeek => {
                type $typename = $crate::types::DateTimeWeekType;
                $then
            }
            Datatype::DateTimeDay => {
                type $typename = $crate::types::DateTimeDayType;
                $then
            }
            Datatype::DateTimeHour => {
                type $typename = $crate::types::DateTimeHourType;
                $then
            }
            Datatype::DateTimeMinute => {
                type $typename = $crate::types::DateTimeMinuteType;
                $then
            }
            Datatype::DateTimeSecond => {
                type $typename = $crate::types::DateTimeSecondType;
                $then
            }
            Datatype::DateTimeMillisecond => {
                type $typename = $crate::types::DateTimeMillisecondType;
                $then
            }
            Datatype::DateTimeMicrosecond => {
                type $typename = $crate::types::DateTimeMicrosecondType;
                $then
            }
            Datatype::DateTimeNanosecond => {
                type $typename = $crate::types::DateTimeNanosecondType;
                $then
            }
            Datatype::DateTimePicosecond => {
                type $typename = $crate::types::DateTimePicosecondType;
                $then
            }
            Datatype::DateTimeFemtosecond => {
                type $typename = $crate::types::DateTimeFemtosecondType;
                $then
            }
            Datatype::DateTimeAttosecond => {
                type $typename = $crate::types::DateTimeAttosecondType;
                $then
            }
            Datatype::TimeHour => {
                type $typename = $crate::types::TimeHourType;
                $then
            }
            Datatype::TimeMinute => {
                type $typename = $crate::types::TimeMinuteType;
                $then
            }
            Datatype::TimeSecond => {
                type $typename = $crate::types::TimeSecondType;
                $then
            }
            Datatype::TimeMillisecond => {
                type $typename = $crate::types::TimeMillisecondType;
                $then
            }
            Datatype::TimeMicrosecond => {
                type $typename = $crate::types::TimeMicrosecondType;
                $then
            }
            Datatype::TimeNanosecond => {
                type $typename = $crate::types::TimeNanosecondType;
                $then
            }
            Datatype::TimePicosecond => {
                type $typename = $crate::types::TimePicosecondType;
                $then
            }
            Datatype::TimeFemtosecond => {
                type $typename = $crate::types::TimeFemtosecondType;
                $then
            }
            Datatype::TimeAttosecond => {
                type $typename = $crate::types::TimeAttosecondType;
                $then
            }
            Datatype::Blob => {
                type $typename = $crate::types::BlobType;
                $then
            }
            Datatype::Boolean => {
                type $typename = $crate::types::BooleanType;
                $then
            }
            Datatype::GeometryWkb => {
                type $typename = $crate::types::GeometryWkbType;
                $then
            }
            Datatype::GeometryWkt => {
                type $typename = $crate::types::GeometryWktType;
                $then
            }
        }
    }};
}

/// Apply a generic expression `$then` with a static type binding in the identifier `$typename`
/// for a physical type corresponding to the dynamic `$datatype`.
///
/// This is similar to `logical_type_go!` but binds the physical type instead of logical
/// type which is useful for calling generic functions and methods with a `PhysicalType`
/// trait bound.
///
/// # Examples
///
/// ```
/// use tiledb_sys2::physical_type_go;
/// use tiledb_sys2::datatype::Datatype;
///
/// fn physical_type_to_str(datatype: Datatype) -> String {
///     physical_type_go!(datatype, DT, std::any::type_name::<DT>().to_owned())
/// }
///
/// assert_eq!("u8", physical_type_to_str(Datatype::UInt8));
/// assert_eq!("u8", physical_type_to_str(Datatype::StringAscii));
/// assert_eq!("u64", physical_type_to_str(Datatype::UInt64));
/// assert_eq!("i64", physical_type_to_str(Datatype::DateTimeMillisecond));
/// ```
#[macro_export]
macro_rules! physical_type_go {
    ($datatype:expr, $typename:ident, $then:expr) => {{
        $crate::logical_type_go!($datatype, PhysicalTypeGoLogicalType, {
            type $typename =
                <PhysicalTypeGoLogicalType as $crate::types::LogicalType>::PhysicalType;
            $then
        })
    }};
}

/// Represents a dynamically typed single physical value.
///
/// [PhysicalValue] holds the bits which correspond to a single value of a logical data type.
/// For a given logical data type, [PhysicalValue] will always be constructed with the
/// primitive numerical type of the same bit width, signed-ness, and precision.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum PhysicalValue {
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
}

macro_rules! physical_value_go {
    ($physical_value:expr, $DT:ident, $value:pat, $then:expr) => {{
        use $crate::types::PhysicalValue;
        match $physical_value {
            PhysicalValue::UInt8($value) => {
                type $DT = u8;
                $then
            }
            PhysicalValue::UInt16($value) => {
                type $DT = u16;
                $then
            }
            PhysicalValue::UInt32($value) => {
                type $DT = u32;
                $then
            }
            PhysicalValue::UInt64($value) => {
                type $DT = u64;
                $then
            }
            PhysicalValue::Int8($value) => {
                type $DT = i8;
                $then
            }
            PhysicalValue::Int16($value) => {
                type $DT = i16;
                $then
            }
            PhysicalValue::Int32($value) => {
                type $DT = i32;
                $then
            }
            PhysicalValue::Int64($value) => {
                type $DT = i64;
                $then
            }
            PhysicalValue::Float32($value) => {
                type $DT = f32;
                $then
            }
            PhysicalValue::Float64($value) => {
                type $DT = f64;
                $then
            }
        }
    }};
}

macro_rules! physical_value_traits {
    ($($ty:ty: $variant:ident),+) => {
        $(
            impl From<$ty> for PhysicalValue {
                fn from(val: $ty) -> Self {
                    PhysicalValue::$variant(val)
                }
            }

            impl TryFrom<PhysicalValue> for $ty {
                type Error = DatatypeError;

                fn try_from(value: PhysicalValue) -> Result<Self, Self::Error> {
                    if let PhysicalValue::$variant(val) = value {
                        Ok(val)
                    } else {
                        physical_value_go!(
                            value,
                            DT,
                            _,
                            Err(DatatypeError::physical_type_mismatch::<$ty, DT>())
                        )
                    }
                }
            }
        )+
    };
}

physical_value_traits!(u8: UInt8, u16: UInt16, u32: UInt32, u64: UInt64);
physical_value_traits!(i8: Int8, i16: Int16, i32: Int32, i64: Int64);
physical_value_traits!(f32: Float32, f64: Float64);

impl Display for PhysicalValue {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::UInt8(value) => Display::fmt(value, f),
            Self::UInt16(value) => Display::fmt(value, f),
            Self::UInt32(value) => Display::fmt(value, f),
            Self::UInt64(value) => Display::fmt(value, f),
            Self::Int8(value) => Display::fmt(value, f),
            Self::Int16(value) => Display::fmt(value, f),
            Self::Int32(value) => Display::fmt(value, f),
            Self::Int64(value) => Display::fmt(value, f),
            Self::Float32(value) => Display::fmt(value, f),
            Self::Float64(value) => Display::fmt(value, f),
        }
    }
}
