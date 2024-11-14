#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;

#[cfg(feature = "option-subset")]
use tiledb_utils::option::OptionSubset;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use tiledb_common::range::{NonEmptyDomain, Range};

use crate::array::dimension::DimensionData;

/// Encapsulation of data needed to construct a Domain
#[derive(Clone, Default, Debug, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct DomainData {
    pub dimension: Vec<DimensionData>,
}

impl DomainData {
    /// Returns the total number of cells spanned by all dimensions,
    /// or `None` if:
    /// - any dimension is not constrained into a domain; or
    /// - the total number of cells exceeds `usize::MAX`.
    pub fn num_cells(&self) -> Option<usize> {
        let mut total = 1u128;
        for d in self.dimension.iter() {
            total = total.checked_mul(d.constraints.num_cells()?)?;
        }
        usize::try_from(total).ok()
    }

    /// Returns the number of cells in each tile, or `None` if:
    /// - any dimension does not have a tile extent specified (e.g. for a sparse array); or
    /// - the number of cells in a tile exceeds `usize::MAX`.
    pub fn num_cells_per_tile(&self) -> Option<usize> {
        let mut total = 1usize;
        for d in self.dimension.iter() {
            total = total.checked_mul(d.constraints.num_cells_per_tile()?)?;
        }
        Some(total)
    }

    /// Returns the domains of each dimension as a `NonEmptyDomain`,
    /// or `None` if any dimension is not constrained into a domain
    pub fn domains(&self) -> Option<NonEmptyDomain> {
        self.dimension
            .iter()
            .map(|d| d.constraints.domain().map(Range::Single))
            .collect::<Option<NonEmptyDomain>>()
    }
}
