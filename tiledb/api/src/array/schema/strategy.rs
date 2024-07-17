use std::collections::HashSet;
use std::num::NonZeroU32;
use std::rc::Rc;

use proptest::prelude::*;
use proptest::sample::select;
use proptest::strategy::ValueTree;

use crate::array::attribute::strategy::{
    prop_attribute, Requirements as AttributeRequirements,
    StrategyContext as AttributeContext,
};
use crate::array::domain::strategy::Requirements as DomainRequirements;
use crate::array::{
    schema::FieldData, ArrayType, AttributeData, CellOrder, CellValNum,
    DimensionData, DomainData, SchemaData, TileOrder,
};
use crate::filter::list::FilterListData;
use crate::filter::strategy::{
    Requirements as FilterRequirements, StrategyContext as FilterContext,
};

#[derive(Clone)]
pub struct Requirements {
    pub domain: Option<Rc<DomainRequirements>>,
    pub num_attributes: std::ops::RangeInclusive<usize>,
    pub attribute_filters: Option<Rc<FilterRequirements>>,
    pub coordinates_filters: Option<Rc<FilterRequirements>>,
    pub offsets_filters: Option<Rc<FilterRequirements>>,
    pub validity_filters: Option<Rc<FilterRequirements>>,
    pub sparse_tile_capacity: std::ops::RangeInclusive<u64>,
}

impl Requirements {
    pub fn min_attributes_default() -> usize {
        const DEFAULT_MIN_ATTRIBUTES: usize = 1;

        let env = "TILEDB_STRATEGY_SCHEMA_PARAMETERS_ATTRIBUTES_MIN";
        crate::tests::env::<usize>(env).unwrap_or(DEFAULT_MIN_ATTRIBUTES)
    }

    pub fn max_attributes_default() -> usize {
        const DEFAULT_MAX_ATTRIBUTES: usize = 8;

        let env = "TILEDB_STRATEGY_SCHEMA_PARAMETERS_ATTRIBUTES_MAX";
        crate::tests::env::<usize>(env).unwrap_or(DEFAULT_MAX_ATTRIBUTES)
    }

    pub fn min_sparse_tile_capacity_default() -> u64 {
        pub const DEFAULT_MIN_SPARSE_TILE_CAPACITY: u64 = 1;

        let env = "TILEDB_STRATEGY_SCHEMA_PARAMETERS_SPARSE_TILE_CAPACITY_MIN";
        crate::tests::env::<u64>(env)
            .unwrap_or(DEFAULT_MIN_SPARSE_TILE_CAPACITY)
    }

    pub fn max_sparse_tile_capacity_default() -> u64 {
        let env = "TILEDB_STRATEGY_SCHEMA_PARAMETERS_SPARSE_TILE_CAPACITY_MIN";
        crate::tests::env::<u64>(env)
            .unwrap_or(DomainRequirements::cells_per_tile_limit_default() as u64)
    }
}

impl Default for Requirements {
    fn default() -> Self {
        Requirements {
            domain: None,
            num_attributes: Self::min_attributes_default()
                ..=Self::max_attributes_default(),
            attribute_filters: None,
            coordinates_filters: None,
            offsets_filters: None,
            validity_filters: None,
            sparse_tile_capacity: Self::min_sparse_tile_capacity_default()
                ..=Self::max_sparse_tile_capacity_default(),
        }
    }
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
                25 => (2u32..=8).prop_map(|nz| CellValNum::try_from(nz).unwrap()),
                10 => (9u32..=16).prop_map(|nz| CellValNum::try_from(nz).unwrap()),
                3 => (17u32..=32).prop_map(|nz| CellValNum::try_from(nz).unwrap()),
                2 => (33u32..=64).prop_map(|nz| CellValNum::try_from(nz).unwrap()),
                // NB: large fixed CellValNums don't really reflect production use cases
                // and are not well tested, and are known to cause problems
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
    params: &Requirements,
) -> impl Strategy<Value = FilterListData> {
    let req = FilterRequirements {
        context: Some(FilterContext::SchemaCoordinates(Rc::new(
            domain.clone(),
        ))),
        ..params
            .coordinates_filters
            .as_ref()
            .map(|rc| rc.as_ref().clone())
            .unwrap_or_default()
    };
    any_with::<FilterListData>(Rc::new(req))
}

fn prop_schema_for_domain(
    array_type: ArrayType,
    domain: Rc<DomainData>,
    params: Rc<Requirements>,
) -> impl Strategy<Value = SchemaData> {
    let allow_duplicates = match array_type {
        ArrayType::Dense => Just(false).boxed(),
        ArrayType::Sparse => any::<bool>().boxed(),
    };

    let capacity = match array_type {
        ArrayType::Dense => any::<u64>().boxed(), // unused?
        ArrayType::Sparse => {
            /* this is the tile capacity for sparse writes, memory usage scales with it */
            params.sparse_tile_capacity.clone().boxed()
        }
    };

    let attr_requirements = AttributeRequirements {
        context: Some(AttributeContext::Schema(array_type, Rc::clone(&domain))),
        filters: params.attribute_filters.clone(),
        ..Default::default()
    };

    let offsets_filters_requirements = params
        .offsets_filters
        .clone()
        .unwrap_or(Rc::new(FilterRequirements {
            ..Default::default()
        }));

    let validity_filters_requirements = params
        .validity_filters
        .clone()
        .unwrap_or(Rc::new(FilterRequirements {
            allow_scale_float: false,
            allow_positive_delta: false,
            ..Default::default()
        }));

    (
        capacity,
        any_with::<CellOrder>(Some(array_type)),
        any::<TileOrder>(),
        allow_duplicates,
        proptest::collection::vec(
            prop_attribute(Rc::new(attr_requirements)),
            params.num_attributes.clone()
        ),
        prop_coordinate_filters(&domain, params.as_ref()),
        any_with::<FilterListData>(offsets_filters_requirements),
        any_with::<FilterListData>(validity_filters_requirements)
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
    let domain_requirements = requirements.domain.clone().unwrap_or_default();

    if let Some(array_type) = domain_requirements.array_type {
        any_with::<DomainData>(Rc::clone(&domain_requirements))
            .prop_flat_map(move |domain| {
                prop_schema_for_domain(
                    array_type,
                    Rc::new(domain),
                    requirements.clone(),
                )
            })
            .boxed()
    } else {
        any::<ArrayType>()
            .prop_flat_map(move |array_type| {
                let domain_requirements = Rc::new(DomainRequirements {
                    array_type: Some(array_type),
                    ..domain_requirements.as_ref().clone()
                });
                let schema_requirements = Rc::clone(&requirements);
                (
                    Just(array_type),
                    any_with::<DomainData>(domain_requirements),
                )
                    .prop_flat_map(
                        move |(array_type, domain)| {
                            prop_schema_for_domain(
                                array_type,
                                Rc::new(domain),
                                Rc::clone(&schema_requirements),
                            )
                        },
                    )
            })
            .boxed()
    }
}

impl Arbitrary for SchemaData {
    type Parameters = Rc<Requirements>;
    type Strategy = BoxedStrategy<SchemaData>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        prop_schema(Rc::clone(&args)).boxed()
    }
}

impl SchemaData {
    /// Returns a strategy which chooses any dimension from `self`.
    pub fn strat_dimension(&self) -> impl Strategy<Value = DimensionData> {
        self.domain.strat_dimension()
    }

    /// Returns a strategy which chooses any attribute from `self`.
    pub fn strat_attribute(&self) -> impl Strategy<Value = AttributeData> {
        select(self.attributes.clone())
    }

    /// Returns a strategy which chooses any dimension or attribute from `self`.
    pub fn strat_field(&self) -> impl Strategy<Value = FieldData> {
        select(
            self.domain
                .dimension
                .clone()
                .into_iter()
                .map(FieldData::Dimension)
                .chain(
                    self.attributes
                        .clone()
                        .into_iter()
                        .map(FieldData::Attribute),
                )
                .collect::<Vec<FieldData>>(),
        )
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
