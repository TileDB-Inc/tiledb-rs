#[cfg(feature = "option-subset")]
use tiledb_utils::option::OptionSubset;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use tiledb_common::array::CellValNum;
use tiledb_common::datatype::Datatype;
use tiledb_common::filter::FilterData;
use tiledb_common::metadata::Value as MetadataValue;

#[cfg(any(test, feature = "proptest-strategies"))]
use crate::array::schema::strategy::FieldValueStrategy;

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
impl AttributeData {
    /// Returns a strategy for generating values of this attribute's type.
    pub fn value_strategy(&self) -> FieldValueStrategy {
        use proptest::prelude::*;
        use tiledb_common::filter::{
            CompressionData, CompressionType, FilterData,
        };
        use tiledb_common::physical_type_go;

        let has_double_delta = self.filters.iter().any(|f| {
            matches!(
                f,
                FilterData::Compression(CompressionData {
                    kind: CompressionType::DoubleDelta { .. },
                    ..
                })
            )
        });

        physical_type_go!(self.datatype, DT, {
            if has_double_delta {
                if std::any::TypeId::of::<DT>() == std::any::TypeId::of::<u64>()
                {
                    // see core `DoubleDelta::compute_bitsize`
                    let min = 0u64;
                    let max = u64::MAX >> 1;
                    return FieldValueStrategy::from((min..=max).boxed());
                } else if std::any::TypeId::of::<DT>()
                    == std::any::TypeId::of::<i64>()
                {
                    let min = i64::MIN >> 2;
                    let max = i64::MAX >> 2;
                    return FieldValueStrategy::from((min..=max).boxed());
                }
            }
            FieldValueStrategy::from(any::<DT>().boxed())
        })
    }
}

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;
