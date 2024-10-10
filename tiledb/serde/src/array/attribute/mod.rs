#[derive(
    Clone, Default, Debug, Deserialize, OptionSubset, Serialize, PartialEq,
)]
pub struct AttributeData {
    pub name: String,
    pub datatype: Datatype,
    pub nullability: Option<bool>,
    pub cell_val_num: Option<CellValNum>,
    pub fill: Option<FillData>,
    pub filters: FilterListData,
}

/// Encapsulation of data needed to construct an Attribute's fill value
#[derive(Clone, Debug, Deserialize, OptionSubset, PartialEq, Serialize)]
pub struct FillData {
    pub data: crate::metadata::Value,
    pub nullability: Option<bool>,
}

#[cfg(any(test, feature = "proptest-strategies"))]
impl AttributeData {
    /// Returns a strategy for generating values of this attribute's type.
    pub fn value_strategy(&self) -> crate::query::strategy::FieldValueStrategy {
        use crate::query::strategy::FieldValueStrategy;
        use proptest::prelude::*;

        use crate::filter::{CompressionData, CompressionType, FilterData};
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

#[cfg(feature = "api-conversions")]
mod conversions;

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;
