use std::fmt::Debug;

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

/// Trait for ordering based on value bits.
/// This exists to work around float `NaN` which prevents float from being
/// a total order for use with generic operations.
pub trait BitsOrd {
    /// Return the ordering between `self` and `other`.
    /// This function defines a total order for all values of `Self`.
    fn bits_cmp(&self, other: &Self) -> std::cmp::Ordering;

    /// Restrict a value to a certain interval, using `bits_cmp` as
    /// the ordering function. See `std::cmp::Ord::clamp`.
    fn bits_clamp(self, min: Self, max: Self) -> Self
    where
        Self: Sized,
    {
        use std::cmp::Ordering;

        assert_eq!(min.bits_cmp(&max), Ordering::Less);

        if matches!(self.bits_cmp(&min), Ordering::Less) {
            min
        } else if matches!(self.bits_cmp(&max), Ordering::Less) {
            self
        } else {
            max
        }
    }
}

/// Implements lexicographic comparison of slices using the `BitsOrd` trait of the element.
impl<T> BitsOrd for [T]
where
    T: BitsOrd,
{
    fn bits_cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        for (l, r) in self.iter().zip(other.iter()) {
            match l.bits_cmp(r) {
                Ordering::Less => return Ordering::Less,
                Ordering::Greater => return Ordering::Greater,
                Ordering::Equal => continue,
            }
        }

        if self.len() < other.len() {
            Ordering::Less
        } else if self.len() == other.len() {
            Ordering::Equal
        } else {
            Ordering::Greater
        }
    }
}

/// Implements lexicographic comparison of vectors using the `BitsOrd` trait of the element.
impl<T> BitsOrd for Vec<T>
where
    T: BitsOrd,
{
    fn bits_cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_slice().bits_cmp(other.as_slice())
    }
}

/// Trait for generic operations on primitive data types.
///
/// Types which implement this trait can be used to interface with TileDB
/// at the lowest level due to having the same memory representation
/// in Rust and TileDB.
pub trait PhysicalType:
    BitsEq
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

macro_rules! native_type_eq {
    ($($T:ty: $datatype:expr),+) => {
        sealed!($($T),+);

        $(
            impl BitsEq for $T {
                fn bits_eq(&self, other: &Self) -> bool {
                    <Self as PartialEq>::eq(self, other)
                }
            }

            impl BitsOrd for $T {
                fn bits_cmp(&self, other: &Self) -> std::cmp::Ordering {
                    <Self as Ord>::cmp(self, other)
                }
            }

            impl PhysicalType for $T {}
        )+
    }
}

native_type_eq!(u8: Datatype::UInt8, u16: Datatype::UInt16, u32: Datatype::UInt32, u64: Datatype::UInt64);
native_type_eq!(i8: Datatype::Int8, i16: Datatype::Int16, i32: Datatype::Int32, i64: Datatype::Int64);

impl crate::private::Sealed for f32 {}
impl crate::private::Sealed for f64 {}

impl BitsEq for f32 {
    fn bits_eq(&self, other: &Self) -> bool {
        self.to_bits() == other.to_bits()
    }
}

impl BitsOrd for f32 {
    fn bits_cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.total_cmp(other)
    }
}

impl PhysicalType for f32 {}

impl BitsEq for f64 {
    fn bits_eq(&self, other: &Self) -> bool {
        self.to_bits() == other.to_bits()
    }
}

impl BitsOrd for f64 {
    fn bits_cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.total_cmp(other)
    }
}

impl PhysicalType for f64 {}
