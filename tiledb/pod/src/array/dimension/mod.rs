#[cfg(feature = "option-subset")]
use tiledb_utils::option::OptionSubset;

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use tiledb_common::array::CellValNum;
use tiledb_common::array::dimension::DimensionConstraints;
use tiledb_common::datatype::Datatype;
use tiledb_common::filter::FilterData;

/// Encapsulation of data needed to construct a Dimension
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct DimensionData {
    pub name: String,
    pub datatype: Datatype,
    pub constraints: DimensionConstraints,

    /// Optional filters to apply to the dimension. If empty,
    /// then filters will be inherited from the schema's `coordinate_filters`
    /// field when the array is constructed.
    pub filters: Vec<FilterData>,
}

impl DimensionData {
    pub fn new<T>(
        name: &str,
        domain_low: T,
        domain_high: T,
        extent: Option<T>,
    ) -> Self
    where
        DimensionConstraints: From<([T; 2], Option<T>)>,
    {
        let constraints =
            DimensionConstraints::from(([domain_low, domain_high], extent));
        Self {
            name: name.to_string(),
            datatype: constraints.physical_datatype(),
            constraints: constraints,
            filters: vec![],
        }
    }

    pub fn new_ascii_string(name: &str) -> Self {
        Self {
            name: name.to_string(),
            datatype: Datatype::StringAscii,
            constraints: DimensionConstraints::StringAscii,
            filters: vec![],
        }
    }

    pub fn with_datatype(self, datatype: Datatype) -> Self {
        Self { datatype, ..self }
    }

    pub fn with_filters(self, filters: Vec<FilterData>) -> Self {
        Self { filters, ..self }
    }

    pub fn cell_val_num(&self) -> CellValNum {
        self.constraints.cell_val_num()
    }
}
