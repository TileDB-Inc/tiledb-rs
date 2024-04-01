use std::collections::HashSet;
use std::rc::Rc;

use proptest::prelude::*;
use proptest::strategy::ValueTree;

use crate::array::attribute::strategy::{
    prop_attribute, Requirements as AttributeRequirements,
    StrategyContext as AttributeContext,
};
use crate::array::domain::strategy::{Requirements as DomainRequirements, *};
use crate::array::{ArrayType, DomainData, Layout, SchemaData};
use crate::filter::list::FilterListData;
use crate::filter::strategy::{
    Requirements as FilterRequirements, StrategyContext as FilterContext, *,
};

#[derive(Clone, Default)]
pub struct Requirements {
    array_type: Option<ArrayType>,
}

pub fn prop_array_type() -> impl Strategy<Value = ArrayType> {
    prop_oneof![Just(ArrayType::Dense), Just(ArrayType::Sparse),]
}

pub fn prop_cell_order(array_type: ArrayType) -> impl Strategy<Value = Layout> {
    match array_type {
        ArrayType::Sparse => prop_oneof![
            Just(Layout::Unordered),
            Just(Layout::RowMajor),
            Just(Layout::ColumnMajor),
            Just(Layout::Hilbert),
        ]
        .boxed(),
        ArrayType::Dense => prop_oneof![
            Just(Layout::Unordered),
            Just(Layout::RowMajor),
            Just(Layout::ColumnMajor),
        ]
        .boxed(),
    }
}

pub fn prop_tile_order() -> impl Strategy<Value = Layout> {
    prop_oneof![
        Just(Layout::Unordered),
        Just(Layout::RowMajor),
        Just(Layout::ColumnMajor),
    ]
}

pub fn prop_coordinate_filters(
    domain: &DomainData,
) -> impl Strategy<Value = FilterListData> {
    let req = FilterRequirements {
        context: Some(FilterContext::SchemaCoordinates(Rc::new(
            domain.clone(),
        ))),
        ..Default::default()
    };
    prop_filter_pipeline(Rc::new(req))
}

fn prop_schema_for_domain(
    array_type: ArrayType,
    domain: Rc<DomainData>,
) -> impl Strategy<Value = SchemaData> {
    const MIN_ATTRS: usize = 1;
    const MAX_ATTRS: usize = 32;

    let allow_duplicates = match array_type {
        ArrayType::Dense => Just(false).boxed(),
        ArrayType::Sparse => any::<bool>().boxed(),
    };

    let capacity = match array_type {
        ArrayType::Dense => 0..=std::u64::MAX,
        ArrayType::Sparse => 1..=std::u64::MAX,
    };

    let attr_requirements = AttributeRequirements {
        context: Some(AttributeContext::Schema(array_type, Rc::clone(&domain))),
        ..Default::default()
    };

    (
        capacity,
        prop_cell_order(array_type),
        prop_tile_order(),
        allow_duplicates,
        proptest::collection::vec(
            prop_attribute(Rc::new(attr_requirements)),
            MIN_ATTRS..=MAX_ATTRS,
        ),
        prop_coordinate_filters(&domain),
        prop_filter_pipeline(Default::default()),
        prop_filter_pipeline(Default::default()),
    )
        .prop_map(
            move |(
                capacity,
                cell_order,
                tile_order,
                allow_duplicates,
                attributes,
                coordinate_filters,
                offsets_filters,
                nullity_filters,
            )| {
                /*
                 * Update the set of dimension/attribute names to be unique.
                 * This probably ought to be threaded into the domain/attribute strategies
                 * so that they have unique names in all scenarios, but this is way
                 * easier as long as we only care about the Schema in the end.
                 */
                let mut domain = (*domain).clone();
                let mut attributes = attributes;

                {
                    let mut runner =
                        proptest::test_runner::TestRunner::new(Default::default());
                    let mut names = HashSet::new();

                    {
                        let dimgen = crate::array::dimension::strategy::prop_dimension_name();
                        for dim in domain.dimension.iter_mut() {
                            while !names.insert(dim.name.clone()) {
                                dim.name = dimgen
                                    .new_tree(&mut runner)
                                    .unwrap()
                                    .current();
                            }
                        }
                    }
                    {
                        let attgen = crate::array::attribute::strategy::prop_attribute_name();
                        for attr in attributes.iter_mut() {
                            while !names.insert(attr.name.clone()) {
                                attr.name = attgen
                                    .new_tree(&mut runner)
                                    .unwrap()
                                    .current();
                            }
                        }
                    }
                }

                SchemaData {
                    array_type,
                    domain,
                    capacity: Some(capacity),
                    cell_order: Some(cell_order),
                    tile_order: Some(tile_order),
                    allow_duplicates: Some(allow_duplicates),
                    attributes,
                    coordinate_filters,
                    offsets_filters,
                    nullity_filters,
                }
            },
        )
}

pub fn prop_schema(
    requirements: Rc<Requirements>,
) -> impl Strategy<Value = SchemaData> {
    let array_type = requirements
        .array_type
        .map(|at| Just(at).boxed())
        .unwrap_or(prop_array_type().boxed());

    array_type.prop_flat_map(|array_type| {
        prop_domain(Rc::new(DomainRequirements {
            array_type: Some(array_type),
        }))
        .prop_flat_map(move |domain| {
            prop_schema_for_domain(array_type, Rc::new(domain))
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Context, Factory};

    /// Test that the arbitrary schema construction always succeeds
    #[test]
    fn schema_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_schema in prop_schema(Default::default()))| {
            maybe_schema.create(&ctx)
                .expect("Error constructing arbitrary schema");
        });
    }

    #[test]
    fn schema_eq_reflexivity() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_schema in prop_schema(Default::default()))| {
            let schema = maybe_schema.create(&ctx)
                .expect("Error constructing arbitrary schema");
            assert_eq!(schema, schema);
        });
    }
}
