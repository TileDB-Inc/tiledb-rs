use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::private::sealed;

/// Trait for generic operations on primitive data types.
///
/// Types which implement this trait can be used to interface with TileDB
/// at the lowest level due to having the same memory representation
/// in Rust and TileDB.
pub trait PhysicalType:
    Copy
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
    /// Test if two values have the same bits.
    ///
    /// This is often the same as `PartialEq::eq`, but is not in the case
    /// of floats where `NaN != NaN`.
    fn bits_eq(&self, other: &Self) -> bool;

    /// Return the ordering between `self` and `other`.
    /// This function defines a total order for all values of `Self`.
    fn bits_cmp(&self, other: &Self) -> std::cmp::Ordering;

    /// Restrict a value to a certain interval, using `bits_cmp` as
    /// the ordering function. See `std::cmp::Ord::clamp`.
    fn bits_clamp(&self, min: Self, max: Self) -> Self
    where
        Self: Sized,
    {
        use std::cmp::Ordering;

        assert_eq!(min.bits_cmp(&max), Ordering::Less);

        if matches!(self.bits_cmp(&min), Ordering::Less) {
            min
        } else if matches!(self.bits_cmp(&max), Ordering::Less) {
            *self
        } else {
            max
        }
    }
}

macro_rules! native_type_eq {
    ($($T:ty: $datatype:expr),+) => {
        sealed!($($T),+);

        $(

            impl PhysicalType for $T {
                fn bits_eq(&self, other: &Self) -> bool {
                    <Self as PartialEq>::eq(self, other)
                }

                fn bits_cmp(&self, other: &Self) -> std::cmp::Ordering {
                    <Self as Ord>::cmp(self, other)
                }
            }
        )+
    }
}

native_type_eq!(u8: Datatype::UInt8, u16: Datatype::UInt16, u32: Datatype::UInt32, u64: Datatype::UInt64);
native_type_eq!(i8: Datatype::Int8, i16: Datatype::Int16, i32: Datatype::Int32, i64: Datatype::Int64);

impl crate::private::Sealed for f32 {}
impl crate::private::Sealed for f64 {}

impl PhysicalType for f32 {
    fn bits_eq(&self, other: &Self) -> bool {
        self.to_bits() == other.to_bits()
    }

    fn bits_cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.total_cmp(other)
    }
}

impl PhysicalType for f64 {
    fn bits_eq(&self, other: &Self) -> bool {
        self.to_bits() == other.to_bits()
    }

    fn bits_cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.total_cmp(other)
    }
}
