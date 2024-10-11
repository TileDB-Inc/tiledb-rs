#[cfg(feature = "option-subset")]
use tiledb_utils::option::OptionSubset;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use tiledb_common::datatype::Datatype;

/// Encapsulation of data needed to construct an Enumeration
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct EnumerationData {
    pub name: String,
    pub datatype: Datatype,
    pub cell_val_num: Option<u32>,
    pub ordered: Option<bool>,
    pub data: Box<[u8]>,
    pub offsets: Option<Box<[u64]>>,
}

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;
