use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::hash::{Hash, Hasher};

use crate::datatype::Error;
use crate::private::sealed;

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
    + crate::private::Sealed
    + 'static
{
}

macro_rules! integral_type_impls {
    ($($T:ty: $datatype:expr),+) => {
        sealed!($($T),+);

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

/// Defines an equivalence relation for `f32`.
///
/// The difference from the `PartialEq` implementation for `f32` is that of
/// reflexivity. Specifically, if the bits match, then `self` and `other` are equal
/// in this relation, whereas this is not true of floating-point equality for `NaN` values.
///
/// Zero and negative zero are considered equal under this relation.
impl BitsEq for f32 {
    fn bits_eq(&self, other: &Self) -> bool {
        self.to_bits() == other.to_bits() ||
            // catch negative zero
            (*self == 0f32 && *other == 0f32)
    }
}

/// Defines a total order for `f32`.
///
/// This ordering intends to be a bridge between
/// `<f32 as PartialOrd>` (which is not a total order due to `NaN`)
/// and `f32::total_cmp` (which does not consider 0 and -0 to be equal).
/// Specifically, we use `f32::total_cmp` except zero and negative zero are equal.
impl BitsOrd for f32 {
    fn bits_cmp(&self, other: &Self) -> Ordering {
        if *self == 0f32 && *other == 0f32 {
            Ordering::Equal
        } else {
            self.total_cmp(other)
        }
    }
}

impl BitsHash for f32 {
    fn bits_hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        if self.to_bits() == (-0f32).to_bits() {
            0f32.bits_hash(state)
        } else {
            self.to_bits().bits_hash(state)
        }
    }
}

impl PhysicalType for f32 {}

/// Defines an equivalence relation for `f64`.
///
/// The difference from the `PartialEq` implementation for `f32` is that of
/// reflexivity. Specifically, if the bits match, then `self` and `other` are equal
/// in this relation, whereas this is not true of floating-point equality for `NaN` values.
///
/// Zero and negative zero are considered equal under this relation.
impl BitsEq for f64 {
    fn bits_eq(&self, other: &Self) -> bool {
        self.to_bits() == other.to_bits() ||
            // catch negative zero
            (*self == 0f64 && *other == 0f64)
    }
}

/// Defines a total order for `f64`.
///
/// This ordering intends to be a bridge between
/// `<f32 as PartialOrd>` (which is not a total order due to `NaN`)
/// and `f32::total_cmp` (which does not consider 0 and -0 to be equal).
/// Specifically, we use `f32::total_cmp` except zero and negative zero are equal.
impl BitsOrd for f64 {
    fn bits_cmp(&self, other: &Self) -> Ordering {
        if *self == 0f64 && *other == 0f64 {
            Ordering::Equal
        } else {
            self.total_cmp(other)
        }
    }
}

impl BitsHash for f64 {
    fn bits_hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        if self.to_bits() == (-0f64).to_bits() {
            0f64.bits_hash(state)
        } else {
            self.to_bits().bits_hash(state)
        }
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
        use $crate::datatype::physical::PhysicalValue;
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
                type Error = Error;

                fn try_from(value: PhysicalValue) -> Result<Self, Self::Error> {
                    if let PhysicalValue::$variant(val) = value {
                        Ok(val)
                    } else {
                        physical_value_go!(
                            value,
                            DT,
                            _,
                            Err(Error::physical_type_mismatch::<$ty, DT>())
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

#[cfg(feature = "proptest-strategies")]
pub mod strategy {
    use proptest::strategy::BoxedStrategy;

    pub enum PhysicalValueStrategy {
        UInt8(BoxedStrategy<u8>),
        UInt16(BoxedStrategy<u16>),
        UInt32(BoxedStrategy<u32>),
        UInt64(BoxedStrategy<u64>),
        Int8(BoxedStrategy<i8>),
        Int16(BoxedStrategy<i16>),
        Int32(BoxedStrategy<i32>),
        Int64(BoxedStrategy<i64>),
        Float32(BoxedStrategy<f32>),
        Float64(BoxedStrategy<f64>),
    }

    macro_rules! field_value_strategy {
        ($($variant:ident : $T:ty),+) => {
            $(
                impl From<BoxedStrategy<$T>> for PhysicalValueStrategy {
                    fn from(value: BoxedStrategy<$T>) -> Self {
                        Self::$variant(value)
                    }
                }

                impl TryFrom<PhysicalValueStrategy> for BoxedStrategy<$T> {
                    type Error = ();
                    fn try_from(value: PhysicalValueStrategy) -> Result<Self, Self::Error> {
                        if let PhysicalValueStrategy::$variant(b) = value {
                            Ok(b)
                        } else {
                            Err(())
                        }
                    }
                }
            )+
        }
    }

    field_value_strategy!(UInt8 : u8, UInt16 : u16, UInt32 : u32, UInt64 : u64);
    field_value_strategy!(Int8 : i8, Int16 : i16, Int32 : i32, Int64 : i64);
    field_value_strategy!(Float32 : f32, Float64 : f64);
}

#[cfg(test)]
mod tests {
    use std::hash::DefaultHasher;

    use proptest::prelude::*;

    use super::*;

    fn default_hash<T>(value: T) -> u64
    where
        T: BitsHash,
    {
        let mut hasher = DefaultHasher::new();
        value.bits_hash(&mut hasher);
        hasher.finish()
    }

    /// Returns a strategy which produces truly any possible f32 bits.
    ///
    /// This is in contrast with [Arbitrary] does not produce `NaN` or infinities.
    fn any_f32() -> impl Strategy<Value = f32> {
        any::<[u8; 4]>().prop_map(f32::from_le_bytes)
    }

    /// Returns a strategy which produces truly any possible f64 bits.
    ///
    /// This is in contrast with [Arbitrary] does not produce `NaN` or infinities.
    fn any_f64() -> impl Strategy<Value = f64> {
        any::<[u8; 8]>().prop_map(f64::from_le_bytes)
    }

    proptest! {
        #[test]
        fn bits_eq_f32_vs_eq(f1 in any_f32(), f2 in any_f32()) {
            if f1 == f2 {
                assert!(f1.bits_eq(&f2));
                assert!(f2.bits_eq(&f1));
            } else if f1.bits_eq(&f2) {
                // NaN
                assert!(f2.bits_eq(&f1));
                assert_eq!(f1.to_bits(), f2.to_bits());
            }
        }

        #[test]
        fn bits_eq_f64_vs_eq(f1 in any_f64(), f2 in any_f64()) {
            if f1 == f2 {
                assert!(f1.bits_eq(&f2));
                assert!(f2.bits_eq(&f1));
            } else if f1.bits_eq(&f2) {
                // NaN
                assert!(f2.bits_eq(&f1));
                assert_eq!(f1.to_bits(), f2.to_bits());
            }
        }

        #[test]
        fn bits_eq_f32_reflexive(f in any_f32()) {
            assert!(f.bits_eq(&f));
        }

        #[test]
        fn bits_eq_f64_reflexive(f in any_f64()) {
            assert!(f.bits_eq(&f));
        }

        #[test]
        fn bits_cmp_f32_total_order(f1 in any_f32(), f2 in any_f32()) {
            let lt = matches!(f1.bits_cmp(&f2), Ordering::Less);
            let eq = matches!(f1.bits_cmp(&f2), Ordering::Equal);
            let gt = matches!(f1.bits_cmp(&f2), Ordering::Greater);

            // exactly one of `<`, `==`, and `>` must be true for a total order
            if lt {
                assert!(!eq);
                assert!(!gt);
            } else if eq {
                assert!(!gt);
            } else {
                assert!(gt);
            }
        }

        #[test]
        fn bits_cmp_f64_total_order(f1 in any_f64(), f2 in any_f64()) {
            let lt = matches!(f1.bits_cmp(&f2), Ordering::Less);
            let eq = matches!(f1.bits_cmp(&f2), Ordering::Equal);
            let gt = matches!(f1.bits_cmp(&f2), Ordering::Greater);

            // exactly one of `<`, `==`, and `>` must be true for a total order
            if lt {
                assert!(!eq);
                assert!(!gt);
            } else if eq {
                assert!(!gt);
            } else {
                assert!(gt);
            }
        }

        #[test]
        fn bits_cmp_f32_reflexive(f in any_f32()) {
            assert!(matches!(f.bits_cmp(&f), Ordering::Equal));
        }

        #[test]
        fn bits_cmp_f64_reflexive(f in any_f64()) {
            assert!(matches!(f.bits_cmp(&f), Ordering::Equal));
        }

        #[test]
        fn bits_cmp_f32_transitive(f1 in any_f32(), f2 in any_f32(), f3 in any_f32()) {
            let f1 = BitsKeyAdapter(f1);
            let f2 = BitsKeyAdapter(f2);
            let f3 = BitsKeyAdapter(f3);

            if f1 <= f2 {
                if f2 <= f3 {
                    assert!(f1 <= f3);
                }
            } else if f1 <= f3 {
                assert!(f2 <= f3);
            }
        }

        #[test]
        fn bits_cmp_f64_transitive(f1 in any_f64(), f2 in any_f64(), f3 in any_f64()) {
            let f1 = BitsKeyAdapter(f1);
            let f2 = BitsKeyAdapter(f2);
            let f3 = BitsKeyAdapter(f3);

            if f1 <= f2 {
                if f2 <= f3 {
                    assert!(f1 <= f3);
                }
            } else if f1 <= f3 {
                assert!(f2 <= f3);
            }
        }

        #[test]
        fn bits_hash_f32(f1 in any_f32(), f2 in any_f32()) {
            if f1.bits_eq(&f2) {
                assert_eq!(default_hash(f1), default_hash(f2));
            }
        }

        #[test]
        fn bits_hash_f64(f1 in any_f64(), f2 in any_f64()) {
            if f1.bits_eq(&f2) {
                assert_eq!(default_hash(f1), default_hash(f2));
            }
        }
    }

    #[test]
    fn bits_cmp_f32() {
        // NB: no proptest since we would just use total_cmp and that's literally the
        // implementation now, but here's a spot check of an edge case.
        assert!(matches!(0f32.bits_cmp(&(-0f32)), Ordering::Equal));
        assert!(matches!((-0f32).bits_cmp(&0f32), Ordering::Equal));
    }

    #[test]
    fn bits_cmp_f64() {
        // NB: no proptest since we would just use total_cmp and that's literally the
        // implementation now, but here's a spot check of an edge case.
        assert!(matches!(0f64.bits_cmp(&(-0f64)), Ordering::Equal));
        assert!(matches!((-0f64).bits_cmp(&0f64), Ordering::Equal));
    }

    #[test]
    fn bits_hash_f32_examples() {
        assert_eq!(default_hash(0f32), default_hash(-0f32))
    }

    #[test]
    fn bits_hash_f64_examples() {
        assert_eq!(default_hash(0f64), default_hash(-0f64))
    }
}
