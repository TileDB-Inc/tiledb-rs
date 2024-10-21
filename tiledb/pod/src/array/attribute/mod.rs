#[cfg(feature = "option-subset")]
use tiledb_utils::option::OptionSubset;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use tiledb_common::array::CellValNum;
use tiledb_common::datatype::Datatype;
use tiledb_common::filter::FilterData;
use tiledb_common::metadata::Value as MetadataValue;

#[derive(Clone, Default, Debug, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct AttributeData {
    pub name: String,
    pub datatype: Datatype,
    pub nullability: Option<bool>,
    pub cell_val_num: Option<CellValNum>,
    pub fill: Option<FillData>,
    pub filters: Vec<FilterData>,
}

/// Encapsulation of data needed to construct an Attribute's fill value
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct FillData {
    pub data: MetadataValue,
    pub nullability: Option<bool>,
}

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;
