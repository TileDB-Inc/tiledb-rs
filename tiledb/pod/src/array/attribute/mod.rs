#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;

#[cfg(feature = "option-subset")]
use tiledb_utils::option::OptionSubset;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use tiledb_common::array::CellValNum;
use tiledb_common::datatype::Datatype;
use tiledb_common::filter::FilterData;
use tiledb_common::metadata::Value as MetadataValue;

use super::EnumerationData;

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct AttributeData {
    pub name: String,
    pub datatype: Datatype,
    pub nullability: bool,
    pub cell_val_num: CellValNum,
    pub fill: Option<FillData>,
    pub filters: Vec<FilterData>,
    pub enumeration: Option<String>,
}

impl AttributeData {
    pub fn new(name: String, datatype: Datatype) -> Self {
        Self {
            name,
            datatype,
            nullability: false,
            cell_val_num: CellValNum::default(),
            fill: None,
            filters: Vec::new(),
            enumeration: None,
        }
    }

    pub fn with_nullability(self, nullability: bool) -> Self {
        Self {
            nullability,
            ..self
        }
    }

    pub fn with_cell_val_num(self, cell_val_num: CellValNum) -> Self {
        Self {
            cell_val_num,
            ..self
        }
    }

    pub fn with_fill(self, fill: FillData) -> Self {
        Self {
            fill: Some(fill),
            ..self
        }
    }

    pub fn without_fill(self) -> Self {
        Self { fill: None, ..self }
    }

    pub fn with_filters(self, filters: Vec<FilterData>) -> Self {
        Self { filters, ..self }
    }

    /// Sets an enumeration on this attribute if possible
    /// and returns true if it was.
    pub fn try_set_enumeration(
        &mut self,
        enumeration: &EnumerationData,
    ) -> bool {
        let Some(max_variants) = self.datatype.max_enumeration_variants()
        else {
            return false;
        };
        if !self.cell_val_num.is_single_valued()
            || max_variants < enumeration.num_variants()
        {
            false
        } else {
            self.enumeration = Some(enumeration.name.clone());
            true
        }
    }
}

/// Encapsulation of data needed to construct an Attribute's fill value
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct FillData {
    pub data: MetadataValue,
    pub nullability: Option<bool>,
}
