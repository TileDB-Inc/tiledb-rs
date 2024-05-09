use std::rc::Rc;

use proptest::prelude::*;
use serde_json::json;

use crate::array::{
    attribute::FillData, ArrayType, AttributeData, CellValNum, DomainData,
};
use crate::datatype::LogicalType;
use crate::filter::list::FilterListData;
use crate::{fn_typed, Datatype};

#[derive(Clone)]
pub enum StrategyContext {
    /// This attribute is being generated for an array schema
    Schema(ArrayType, Rc<DomainData>),
}

#[derive(Clone, Default)]
pub struct Requirements {
    pub name: Option<String>,
    pub datatype: Option<Datatype>,
    pub nullability: Option<bool>,
    pub context: Option<StrategyContext>,
}

pub fn prop_attribute_name() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[a-zA-Z0-9_]*")
        .expect("Error creating attribute name strategy")
        .prop_filter(
            "Attribute names may not begin with reserved prefix",
            |name| !name.starts_with("__"),
        )
}

fn prop_fill<T: Arbitrary>(
    cell_val_num: CellValNum,
) -> impl Strategy<Value = Vec<T>> {
    match cell_val_num {
        CellValNum::Fixed(nz) => {
            proptest::collection::vec(any::<T>(), nz.get() as usize)
        }
        CellValNum::Var => {
            proptest::collection::vec(any::<T>(), 1..16) /* TODO: does 16 make sense? */
        }
    }
}

/// Returns a strategy to generate a filter pipeline for an attribute with the given
/// datatype and other user requirements
fn prop_filters(
    datatype: Datatype,
    cell_val_num: CellValNum,
    requirements: Rc<Requirements>,
) -> impl Strategy<Value = FilterListData> {
    use crate::filter::strategy::{
        Requirements as FilterRequirements, StrategyContext as FilterContext,
    };

    let pipeline_requirements = FilterRequirements {
        context: Some(
            if let Some(StrategyContext::Schema(array_type, domain)) =
                requirements.context.as_ref()
            {
                FilterContext::SchemaAttribute(
                    datatype,
                    cell_val_num,
                    *array_type,
                    domain.clone(),
                )
            } else {
                FilterContext::Attribute(datatype, cell_val_num)
            },
        ),
        input_datatype: Some(datatype),
        ..Default::default()
    };

    any_with::<FilterListData>(Rc::new(pipeline_requirements))
}

/// Returns a strategy for generating an arbitrary Attribute of the given datatype
/// that satisfies the other user requirements
fn prop_attribute_for_datatype(
    datatype: Datatype,
    requirements: Rc<Requirements>,
) -> impl Strategy<Value = AttributeData> {
    fn_typed!(
        datatype,
        LT,
        {
            type DT = <LT as LogicalType>::PhysicalType;
            let name = requirements
                .name
                .as_ref()
                .map(|n| Just(n.clone()).boxed())
                .unwrap_or(prop_attribute_name().boxed());
            let nullable = requirements
                .nullability
                .as_ref()
                .map(|n| Just(*n).boxed())
                .unwrap_or(any::<bool>().boxed());
            let cell_val_num = if datatype == Datatype::Any {
                Just(CellValNum::Var).boxed()
            } else {
                any::<CellValNum>()
            };
            let fill_nullable = any::<bool>();
            (name, nullable, cell_val_num, fill_nullable).prop_flat_map(
                move |(name, nullable, cell_val_num, fill_nullable)| {
                    (
                        prop_fill::<DT>(cell_val_num),
                        prop_filters(
                            datatype,
                            cell_val_num,
                            requirements.clone(),
                        ),
                    )
                        .prop_map(
                            move |(fill, filters)| AttributeData {
                                name: name.clone(),
                                datatype,
                                nullability: Some(nullable),
                                cell_val_num: Some(cell_val_num),
                                fill: Some(FillData {
                                    data: json!(fill),
                                    nullability: Some(
                                        nullable && fill_nullable,
                                    ),
                                }),
                                filters,
                            },
                        )
                },
            )
        }
        .boxed()
    )
}

pub fn prop_attribute(
    requirements: Rc<Requirements>,
) -> impl Strategy<Value = AttributeData> {
    let datatype = requirements
        .datatype
        .map(|d| Just(d).boxed())
        .unwrap_or(any::<Datatype>());

    datatype.prop_flat_map(move |datatype| {
        prop_attribute_for_datatype(datatype, requirements.clone())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Context, Factory};
    use util::assert_option_subset;
    use util::option::OptionSubset;

    /// Test that the arbitrary attribute construction always succeeds
    #[test]
    fn attribute_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(attr in prop_attribute(Default::default()))| {
            attr.create(&ctx).expect("Error constructing arbitrary attribute");
        });
    }

    #[test]
    fn attribute_eq_reflexivity() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(attr in prop_attribute(Default::default()))| {
            assert_eq!(attr, attr);
            assert_option_subset!(attr, attr);

            let attr = attr.create(&ctx)
                .expect("Error constructing arbitrary attribute");
            assert_eq!(attr, attr);
        });
    }
}
