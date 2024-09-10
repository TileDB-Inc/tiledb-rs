use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

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
    + for<'a> Deserialize<'a>
    + PartialEq
    + PartialOrd
    + Send
    + Serialize
    + Sync
    + crate::private::Sealed
    + 'static
{
}

pub trait IntegralType: Eq + Ord + PhysicalType {}

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

            impl IntegralType for $T {}
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
pub struct BitsKeyAdapter<T>(pub T);

impl<T> PartialEq for BitsKeyAdapter<T>
where
    T: BitsEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.0.bits_eq(&other.0)
    }
}

impl<T> Eq for BitsKeyAdapter<T> where T: BitsEq {}

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

#[cfg(test)]
mod tests {
    use std::hash::DefaultHasher;

    use proptest::prelude::*;

    use super::*;

    #[test]
    fn bits_eq_hash_consistency() {
        proptest!(|(f1 in any::<f32>(), f2 in any::<f32>())| {
            if f1.bits_eq(&f2) {
                let f1_hash = {
                    let mut h = DefaultHasher::default();
                    f1.bits_hash(&mut h);
                    h.finish()
                };
                let f2_hash = {
                    let mut h = DefaultHasher::default();
                    f2.bits_hash(&mut h);
                    h.finish()
                };
                assert_eq!(f1_hash, f2_hash);
            }
        });
    }
}
