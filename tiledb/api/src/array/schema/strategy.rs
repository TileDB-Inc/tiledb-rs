use std::collections::HashSet;
use std::num::NonZeroU32;
use std::rc::Rc;

use proptest::prelude::*;
use proptest::strategy::ValueTree;

use crate::array::attribute::strategy::{
    prop_attribute, Requirements as AttributeRequirements,
    StrategyContext as AttributeContext,
};
use crate::array::domain::strategy::Requirements as DomainRequirements;
use crate::array::{
    ArrayType, CellOrder, CellValNum, DomainData, SchemaData, TileOrder,
};
use crate::filter::list::FilterListData;
use crate::filter::strategy::{
    Requirements as FilterRequirements, StrategyContext as FilterContext,
};

#[derive(Clone, Default)]
pub struct Requirements {
    pub array_type: Option<ArrayType>,
}

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
                20 => (2u32..=8).prop_map(|nz| CellValNum::try_from(nz).unwrap()),
                10 => (9u32..=16).prop_map(|nz| CellValNum::try_from(nz).unwrap()),
                5 => (17u32..=32).prop_map(|nz| CellValNum::try_from(nz).unwrap()),
                3 => (33u32..=64).prop_map(|nz| CellValNum::try_from(nz).unwrap()),
                2 => (65u32..=2048).prop_map(|nz| CellValNum::try_from(nz).unwrap()),
            ].boxed()
        }
    }
}

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

pub fn prop_coordinate_filters(
    domain: &DomainData,
) -> impl Strategy<Value = FilterListData> {
    let req = FilterRequirements {
        context: Some(FilterContext::SchemaCoordinates(Rc::new(
            domain.clone(),
        ))),
        ..Default::default()
    };
    any_with::<FilterListData>(Rc::new(req))
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
        ArrayType::Dense => 0..=u64::MAX,
        ArrayType::Sparse => 1..=u64::MAX,
    };

    let attr_requirements = AttributeRequirements {
        context: Some(AttributeContext::Schema(array_type, Rc::clone(&domain))),
        ..Default::default()
    };

    (
        capacity,
        any_with::<CellOrder>(Some(array_type)),
        any::<TileOrder>(),
        allow_duplicates,
        proptest::collection::vec(
            prop_attribute(Rc::new(attr_requirements)),
            MIN_ATTRS..=MAX_ATTRS,
        ),
        prop_coordinate_filters(&domain),
        any::<FilterListData>(),
        any::<FilterListData>()
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

fn prop_schema(
    requirements: Rc<Requirements>,
) -> impl Strategy<Value = SchemaData> {
    let array_type = requirements
        .array_type
        .map(|at| Just(at).boxed())
        .unwrap_or(any::<ArrayType>().boxed());

    array_type.prop_flat_map(|array_type| {
        any_with::<DomainData>(Rc::new(DomainRequirements {
            array_type: Some(array_type),
        }))
        .prop_flat_map(move |domain| {
            prop_schema_for_domain(array_type, Rc::new(domain))
        })
    })
}

impl Arbitrary for SchemaData {
    type Parameters = Rc<Requirements>;
    type Strategy = BoxedStrategy<SchemaData>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        prop_schema(Rc::clone(&args)).boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Context, Factory};
    use util::option::OptionSubset;

    /// Test that the arbitrary schema construction always succeeds
    #[test]
    fn schema_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_schema in any::<SchemaData>())| {
            maybe_schema.create(&ctx)
                .expect("Error constructing arbitrary schema");
        });
    }

    #[test]
    fn schema_eq_reflexivity() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(schema in any::<SchemaData>())| {
            assert_eq!(schema, schema);
            assert!(schema.option_subset(&schema));

            let schema = schema.create(&ctx)
                .expect("Error constructing arbitrary schema");
            assert_eq!(schema, schema);
        });
    }
}
