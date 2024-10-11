pub mod attribute;
pub mod dimension;

use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::num::NonZeroU32;

use thiserror::Error;

#[cfg(feature = "option-subset")]
use tiledb_utils::option::OptionSubset;

#[cfg(feature = "proptest-strategies")]
use proptest::prelude::*;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Mode {
    Read,
    Write,
    Delete,
    Update,
    ModifyExclusive,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ModeError {
    #[error("Invalid discriminant for {}: {0}", std::any::type_name::<Mode>())]
    InvalidDiscriminant(u64),
}

impl From<Mode> for ffi::tiledb_query_type_t {
    fn from(value: Mode) -> Self {
        match value {
            Mode::Read => ffi::tiledb_query_type_t_TILEDB_READ,
            Mode::Write => ffi::tiledb_query_type_t_TILEDB_WRITE,
            Mode::Delete => ffi::tiledb_query_type_t_TILEDB_DELETE,
            Mode::Update => ffi::tiledb_query_type_t_TILEDB_UPDATE,
            Mode::ModifyExclusive => {
                ffi::tiledb_query_type_t_TILEDB_MODIFY_EXCLUSIVE
            }
        }
    }
}

impl TryFrom<ffi::tiledb_query_type_t> for Mode {
    type Error = ModeError;

    fn try_from(value: ffi::tiledb_query_type_t) -> Result<Self, Self::Error> {
        Ok(match value {
            ffi::tiledb_query_type_t_TILEDB_READ => Mode::Read,
            ffi::tiledb_query_type_t_TILEDB_WRITE => Mode::Write,
            ffi::tiledb_query_type_t_TILEDB_DELETE => Mode::Delete,
            ffi::tiledb_query_type_t_TILEDB_UPDATE => Mode::Update,
            ffi::tiledb_query_type_t_TILEDB_MODIFY_EXCLUSIVE => {
                Mode::ModifyExclusive
            }
            _ => return Err(ModeError::InvalidDiscriminant(value as u64)),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum TileOrder {
    RowMajor,
    ColumnMajor,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum TileOrderError {
    #[error("Invalid discriminant for {}: {0}", std::any::type_name::<TileOrder>())]
    InvalidDiscriminant(u64),
}

impl From<TileOrder> for ffi::tiledb_layout_t {
    fn from(value: TileOrder) -> Self {
        match value {
            TileOrder::RowMajor => ffi::tiledb_layout_t_TILEDB_ROW_MAJOR,
            TileOrder::ColumnMajor => ffi::tiledb_layout_t_TILEDB_COL_MAJOR,
        }
    }
}

impl TryFrom<ffi::tiledb_layout_t> for TileOrder {
    type Error = TileOrderError;
    fn try_from(value: ffi::tiledb_layout_t) -> Result<Self, Self::Error> {
        match value {
            ffi::tiledb_layout_t_TILEDB_ROW_MAJOR => Ok(TileOrder::RowMajor),
            ffi::tiledb_layout_t_TILEDB_COL_MAJOR => Ok(TileOrder::ColumnMajor),
            _ => Err(TileOrderError::InvalidDiscriminant(value as u64)),
        }
    }
}

#[cfg(feature = "proptest-strategies")]
impl Arbitrary for TileOrder {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![Just(TileOrder::RowMajor), Just(TileOrder::ColumnMajor)]
            .boxed()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum CellOrder {
    Unordered,
    RowMajor,
    ColumnMajor,
    Global,
    Hilbert,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum CellOrderError {
    #[error("Invalid discriminant for {}: {0}", std::any::type_name::<CellOrder>())]
    InvalidDiscriminant(u64),
}

impl From<CellOrder> for ffi::tiledb_layout_t {
    fn from(value: CellOrder) -> Self {
        match value {
            CellOrder::Unordered => ffi::tiledb_layout_t_TILEDB_UNORDERED,
            CellOrder::RowMajor => ffi::tiledb_layout_t_TILEDB_ROW_MAJOR,
            CellOrder::ColumnMajor => ffi::tiledb_layout_t_TILEDB_COL_MAJOR,
            CellOrder::Global => ffi::tiledb_layout_t_TILEDB_GLOBAL_ORDER,
            CellOrder::Hilbert => ffi::tiledb_layout_t_TILEDB_HILBERT,
        }
    }
}

impl TryFrom<ffi::tiledb_layout_t> for CellOrder {
    type Error = CellOrderError;
    fn try_from(value: ffi::tiledb_layout_t) -> Result<Self, Self::Error> {
        match value {
            ffi::tiledb_layout_t_TILEDB_UNORDERED => Ok(CellOrder::Unordered),
            ffi::tiledb_layout_t_TILEDB_ROW_MAJOR => Ok(CellOrder::RowMajor),
            ffi::tiledb_layout_t_TILEDB_COL_MAJOR => Ok(CellOrder::ColumnMajor),
            ffi::tiledb_layout_t_TILEDB_GLOBAL_ORDER => Ok(CellOrder::Global),
            ffi::tiledb_layout_t_TILEDB_HILBERT => Ok(CellOrder::Hilbert),
            _ => Err(CellOrderError::InvalidDiscriminant(value as u64)),
        }
    }
}

#[cfg(feature = "proptest-strategies")]
impl Arbitrary for CellOrder {
    type Strategy = BoxedStrategy<CellOrder>;
    type Parameters = Option<ArrayType>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        match args {
            None => prop_oneof![
                Just(CellOrder::Unordered),
                Just(CellOrder::RowMajor),
                Just(CellOrder::ColumnMajor),
                Just(CellOrder::Hilbert),
            ]
            .boxed(),
            Some(ArrayType::Sparse) => prop_oneof![
                Just(CellOrder::RowMajor),
                Just(CellOrder::ColumnMajor),
                Just(CellOrder::Hilbert),
            ]
            .boxed(),
            Some(ArrayType::Dense) => prop_oneof![
                Just(CellOrder::RowMajor),
                Just(CellOrder::ColumnMajor),
            ]
            .boxed(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum ArrayType {
    #[default]
    Dense,
    Sparse,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ArrayTypeError {
    #[error("Invalid discriminant for {}: {0}", std::any::type_name::<ArrayType>())]
    InvalidDiscriminant(u64),
}

impl From<ArrayType> for ffi::tiledb_array_type_t {
    fn from(value: ArrayType) -> Self {
        match value {
            ArrayType::Dense => ffi::tiledb_array_type_t_TILEDB_DENSE,
            ArrayType::Sparse => ffi::tiledb_array_type_t_TILEDB_SPARSE,
        }
    }
}

impl TryFrom<ffi::tiledb_array_type_t> for ArrayType {
    type Error = ArrayTypeError;
    fn try_from(value: ffi::tiledb_array_type_t) -> Result<Self, Self::Error> {
        match value {
            ffi::tiledb_array_type_t_TILEDB_DENSE => Ok(ArrayType::Dense),
            ffi::tiledb_array_type_t_TILEDB_SPARSE => Ok(ArrayType::Sparse),
            _ => Err(ArrayTypeError::InvalidDiscriminant(value as u64)),
        }
    }
}

#[cfg(feature = "proptest-strategies")]
impl Arbitrary for ArrayType {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![Just(ArrayType::Dense), Just(ArrayType::Sparse)].boxed()
    }
}

/// Represents the number of values carried within a single cell of an attribute or dimension.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum CellValNum {
    /// The number of values per cell is a specific fixed number.
    Fixed(std::num::NonZeroU32),
    /// The number of values per cell varies.
    /// When this option is used for a dimension or attribute, queries must allocate additional
    /// space to hold structural information about each cell. The values will be concatenated
    /// together in a single buffer, and the structural data buffer contains the offset
    /// of each record into the values buffer.
    Var,
}

impl CellValNum {
    pub fn single() -> Self {
        CellValNum::Fixed(NonZeroU32::new(1).unwrap())
    }

    pub fn is_var_sized(&self) -> bool {
        matches!(self, CellValNum::Var)
    }

    pub fn is_single_valued(&self) -> bool {
        matches!(self, CellValNum::Fixed(nz) if nz.get() == 1)
    }

    /// Return the fixed number of values per cell, if not variable.
    pub fn fixed(&self) -> Option<NonZeroU32> {
        if let CellValNum::Fixed(nz) = self {
            Some(*nz)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum CellValNumError {
    #[error("{} cannot be zero", std::any::type_name::<CellValNum>())]
    CannotBeZero,
}

impl Default for CellValNum {
    fn default() -> Self {
        Self::single()
    }
}

impl PartialEq<u32> for CellValNum {
    fn eq(&self, other: &u32) -> bool {
        match self {
            CellValNum::Fixed(val) => val.get() == *other,
            CellValNum::Var => *other == u32::MAX,
        }
    }
}

impl Display for CellValNum {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        Debug::fmt(self, f)
    }
}

impl TryFrom<u32> for CellValNum {
    type Error = CellValNumError;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Err(CellValNumError::CannotBeZero),
            u32::MAX => Ok(CellValNum::Var),
            v => Ok(CellValNum::Fixed(NonZeroU32::new(v).unwrap())),
        }
    }
}

impl From<CellValNum> for u32 {
    fn from(value: CellValNum) -> Self {
        match value {
            CellValNum::Fixed(nz) => nz.get(),
            CellValNum::Var => u32::MAX,
        }
    }
}

#[cfg(feature = "proptest-strategies")]
impl Arbitrary for CellValNum {
    type Strategy = BoxedStrategy<CellValNum>;
    type Parameters = Option<std::ops::Range<NonZeroU32>>;

    fn arbitrary_with(r: Self::Parameters) -> Self::Strategy {
        if let Some(range) = r {
            (range.start.get()..range.end.get())
                .prop_map(|nz| CellValNum::try_from(nz).unwrap())
                .boxed()
        } else {
            prop_oneof![
                30 => Just(CellValNum::single()),
                30 => Just(CellValNum::Var),
                25 => (2u32..=8).prop_map(|nz| CellValNum::try_from(nz).unwrap()),
                10 => (9u32..=16).prop_map(|nz| CellValNum::try_from(nz).unwrap()),
                3 => (17u32..=32).prop_map(|nz| CellValNum::try_from(nz).unwrap()),
                2 => (33u32..=64).prop_map(|nz| CellValNum::try_from(nz).unwrap()),
                // NB: large fixed CellValNums don't really reflect production use cases
                // and are not well tested, and are known to cause problems
            ].boxed()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ffi_mode() {
        for m in vec![Mode::Read, Mode::Write, Mode::Delete, Mode::Update] {
            assert_eq!(
                m,
                Mode::try_from(ffi::tiledb_query_type_t::from(m)).unwrap()
            );
        }
    }

    #[test]
    fn ffi_tile_order() {
        for t in vec![TileOrder::RowMajor, TileOrder::ColumnMajor] {
            assert_eq!(
                t,
                TileOrder::try_from(ffi::tiledb_layout_t::from(t)).unwrap()
            );
        }
    }

    #[test]
    fn ffi_cell_order() {
        for c in vec![
            CellOrder::Unordered,
            CellOrder::RowMajor,
            CellOrder::ColumnMajor,
            CellOrder::Global,
            CellOrder::Hilbert,
        ] {
            assert_eq!(
                c,
                CellOrder::try_from(ffi::tiledb_layout_t::from(c)).unwrap()
            );
        }
    }

    #[test]
    fn ffi_array_type() {
        for a in vec![ArrayType::Dense, ArrayType::Sparse] {
            assert_eq!(
                a,
                ArrayType::try_from(ffi::tiledb_array_type_t::from(a)).unwrap()
            );
        }
    }
}
