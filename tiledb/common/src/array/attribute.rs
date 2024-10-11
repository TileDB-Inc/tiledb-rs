use std::num::NonZeroU32;

use anyhow::anyhow;
use thiserror::Error;

use crate::array::CellValNum;
use crate::datatype::PhysicalType;

/// Trait for data which can be used as a fill value for an attribute.
pub trait IntoFillValue {
    type PhysicalType: PhysicalType;

    /// Get a reference to the raw fill value data.
    /// The returned slice will be copied into the tiledb core.
    fn to_raw(&self) -> &[Self::PhysicalType];
}

/// Trait for data which can be constructed from an attribute's raw fill value.
pub trait FromFillValue<'a>: IntoFillValue + Sized {
    /// Construct a value of this type from a raw fill value.
    fn from_raw(
        raw: &'a [Self::PhysicalType],
    ) -> Result<Self, FromFillValueError>;
}

#[derive(Debug, Error)]
pub enum FromFillValueError {
    #[error("Unexpected cell structure: expected {0}, found {1}")]
    UnexpectedCellStructure(CellValNum, CellValNum),
    #[error("Error constructing object: {0}")]
    Construction(anyhow::Error),
}

impl<T> IntoFillValue for T
where
    T: PhysicalType,
{
    type PhysicalType = Self;

    fn to_raw(&self) -> &[Self::PhysicalType] {
        std::slice::from_ref(self)
    }
}

impl<T> FromFillValue<'_> for T
where
    T: PhysicalType,
{
    fn from_raw(
        raw: &[Self::PhysicalType],
    ) -> Result<Self, FromFillValueError> {
        if raw.len() == 1 {
            Ok(raw[0])
        } else {
            // SAFETY: this is safe when coming from core which forbids zero-length fill values
            let found = CellValNum::try_from(raw.len() as u32).unwrap();

            Err(FromFillValueError::UnexpectedCellStructure(
                CellValNum::single(),
                found,
            ))
        }
    }
}

impl<T, const K: usize> IntoFillValue for [T; K]
where
    T: PhysicalType,
{
    type PhysicalType = T;

    fn to_raw(&self) -> &[Self::PhysicalType] {
        self
    }
}

impl<'a, T, const K: usize> FromFillValue<'a> for [T; K]
where
    T: PhysicalType,
{
    fn from_raw(
        raw: &'a [Self::PhysicalType],
    ) -> Result<Self, FromFillValueError> {
        Self::try_from(raw).map_err(|_| {
            let expected = {
                // SAFETY: there's no way to bound `0 < K < u32::MAX` for a trait impl
                // so this can panic, but in a way that's statically known
                let nz =
                    u32::try_from(K).ok().and_then(NonZeroU32::new).expect(
                        "`impl FillValue for [T; K] requires 0 < K < u32::MAX",
                    );
                CellValNum::Fixed(nz)
            };

            // SAFETY: this is safe when coming from core which forbids zero-length fill values
            let found = CellValNum::try_from(raw.len() as u32).unwrap();

            FromFillValueError::UnexpectedCellStructure(expected, found)
        })
    }
}

impl<T> IntoFillValue for &[T]
where
    T: PhysicalType,
{
    type PhysicalType = T;

    fn to_raw(&self) -> &[Self::PhysicalType] {
        self
    }
}

impl<'a, T> FromFillValue<'a> for &'a [T]
where
    T: PhysicalType,
{
    fn from_raw(
        raw: &'a [Self::PhysicalType],
    ) -> Result<Self, FromFillValueError> {
        Ok(raw)
    }
}

impl<T> IntoFillValue for Vec<T>
where
    T: PhysicalType,
{
    type PhysicalType = T;

    fn to_raw(&self) -> &[Self::PhysicalType] {
        self.as_slice()
    }
}

impl<T> FromFillValue<'_> for Vec<T>
where
    T: PhysicalType,
{
    fn from_raw(
        raw: &[Self::PhysicalType],
    ) -> Result<Self, FromFillValueError> {
        Ok(raw.to_vec())
    }
}

impl IntoFillValue for &str {
    type PhysicalType = u8;

    fn to_raw(&self) -> &[Self::PhysicalType] {
        self.as_bytes()
    }
}

impl<'a> FromFillValue<'a> for &'a str {
    fn from_raw(
        raw: &'a [Self::PhysicalType],
    ) -> Result<Self, FromFillValueError> {
        std::str::from_utf8(raw).map_err(|e| {
            FromFillValueError::Construction(anyhow!(
                "Non-UTF8 fill value: {}",
                e
            ))
        })
    }
}

impl IntoFillValue for String {
    type PhysicalType = u8;

    fn to_raw(&self) -> &[Self::PhysicalType] {
        self.as_bytes()
    }
}

impl<'a> FromFillValue<'a> for String {
    fn from_raw(
        raw: &'a [Self::PhysicalType],
    ) -> Result<Self, FromFillValueError> {
        <&'a str as FromFillValue<'a>>::from_raw(raw).map(|s| s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use proptest::collection::vec;
    use proptest::prelude::*;

    use super::*;

    fn fill_value_roundtrip<T>(value: T) -> bool
    where
        T: for<'a> FromFillValue<'a> + PartialEq,
    {
        let value_out = T::from_raw(value.to_raw());
        if let Ok(value_out) = value_out {
            value == value_out
        } else {
            false
        }
    }

    proptest! {
        #[test]
        fn fill_value_roundtrip_u64(value in any::<u64>()) {
            assert!(fill_value_roundtrip(value))
        }

        #[test]
        fn fill_value_roundtrip_array(value in any::<[u64; 32]>()) {
            assert!(fill_value_roundtrip(value));
        }

        #[test]
        fn fill_value_roundtrip_vec(value in vec(any::<u64>(), 0..=64)) {
            assert!(fill_value_roundtrip(value));
        }

        #[test]
        fn fill_value_roundtrip_str(value in any::<String>()) {
            assert!(fill_value_roundtrip(value));
        }
    }
}
