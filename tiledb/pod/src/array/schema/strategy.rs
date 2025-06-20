use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use itertools::Itertools;
use proptest::option::Probability;
use proptest::prelude::*;
use proptest::sample::select;
use proptest::strategy::ValueTree;
use strategy_ext::StrategyExt;
use strategy_ext::records::RecordsValueTree;
use tiledb_common::array::{ArrayType, CellOrder, TileOrder};
use tiledb_common::filter::FilterData;

use crate::array::attribute::strategy::{
    AttributeValueTree, Requirements as AttributeRequirements,
    StrategyContext as AttributeContext, prop_attribute,
};
use crate::array::domain::strategy::{
    DomainValueTree, Requirements as DomainRequirements,
};
use crate::array::enumeration::strategy::EnumerationValueTree;
use crate::array::schema::{FieldData, SchemaData};
use crate::array::{AttributeData, DimensionData, DomainData, EnumerationData};
use crate::filter::strategy::{
    FilterPipelineStrategy, FilterPipelineValueTree,
    Requirements as FilterRequirements, StrategyContext as FilterContext,
};

#[derive(Clone)]
pub struct Requirements {
    pub domain: Option<Rc<DomainRequirements>>,
    pub num_attributes: std::ops::RangeInclusive<usize>,
    pub attributes: Option<AttributeRequirements>,
    pub coordinates_filters: Option<Rc<FilterRequirements>>,
    pub offsets_filters: Option<Rc<FilterRequirements>>,
    pub validity_filters: Option<Rc<FilterRequirements>>,
    pub sparse_tile_capacity: std::ops::RangeInclusive<u64>,
    pub attribute_enumeration_likelihood: f64,
}

impl Requirements {
    pub fn min_attributes_default() -> usize {
        **tiledb_proptest_config::TILEDB_STRATEGY_SCHEMA_PARAMETERS_ATTRIBUTES_MIN
    }

    pub fn max_attributes_default() -> usize {
        **tiledb_proptest_config::TILEDB_STRATEGY_SCHEMA_PARAMETERS_ATTRIBUTES_MAX
    }

    pub fn min_sparse_tile_capacity_default() -> u64 {
        **tiledb_proptest_config::TILEDB_STRATEGY_SCHEMA_PARAMETERS_SPARSE_TILE_CAPACITY_MIN
    }

    pub fn max_sparse_tile_capacity_default() -> u64 {
        **tiledb_proptest_config::TILEDB_STRATEGY_SCHEMA_PARAMETERS_SPARSE_TILE_CAPACITY_MIN
    }

    pub fn attribute_enumeration_likelihood_default() -> f64 {
        **tiledb_proptest_config::TILEDB_STRATEGY_SCHEMA_PARAMETERS_ATTRIBUTE_ENUMERATION_LIKELIHOOD
    }
}

impl Default for Requirements {
    fn default() -> Self {
        Requirements {
            domain: None,
            num_attributes: Self::min_attributes_default()
                ..=Self::max_attributes_default(),
            attributes: None,
            coordinates_filters: None,
            offsets_filters: None,
            validity_filters: None,
            sparse_tile_capacity: Self::min_sparse_tile_capacity_default()
                ..=Self::max_sparse_tile_capacity_default(),
            attribute_enumeration_likelihood:
                Self::attribute_enumeration_likelihood_default(),
        }
    }
}

pub fn prop_coordinate_filters(
    domain: Rc<DomainData>,
    params: Rc<Requirements>,
) -> impl Strategy<Value = Vec<FilterData>> {
    let req = FilterRequirements {
        context: Some(FilterContext::SchemaCoordinates(domain.clone())),
        ..params
            .coordinates_filters
            .as_ref()
            .map(|rc| rc.as_ref().clone())
            .unwrap_or_default()
    };
    FilterPipelineStrategy::new(Rc::new(req))
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
        ..params.attributes.clone().unwrap_or_default()
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

    // Generate attributes and enumerations together.
    // Choose attributes, then candidate enumerations,
    // then assign enumeration names to attributes
    // and return the used attributes
    let enumeration_likelihood = params.attribute_enumeration_likelihood;
    let strat_attributes_enumerations = proptest::collection::vec(
        prop_attribute(Rc::new(attr_requirements)),
        params.num_attributes.clone(),
    )
    .prop_flat_map(|attributes| {
        // we have the attributes, now make some candidate enumerations
        let num_attributes = attributes.len();
        (
            Just(attributes),
            proptest::collection::vec(
                any::<EnumerationData>(),
                0..=num_attributes,
            ),
        )
    })
    .prop_flat_map(move |(attributes, enumerations)| {
        // we have attributes and candidate enumerations,
        // select optional enumerations per attribute
        let num_enumerations = enumerations.len();
        let attribute_mapping = attributes
            .iter()
            .map(|_| {
                // NB: `weighted` is not friendly to 0.0 and 1.0
                if num_enumerations == 0 || enumeration_likelihood <= 0.0 {
                    Just(None).boxed()
                } else if enumeration_likelihood >= 1.0 {
                    (0..num_enumerations).prop_map(Some).boxed()
                } else {
                    proptest::option::weighted(
                        Probability::new(enumeration_likelihood),
                        0..num_enumerations,
                    )
                    .boxed()
                }
            })
            .collect::<Vec<_>>();
        (Just(attributes), Just(enumerations), attribute_mapping)
    })
    .prop_map(|(mut attributes, enumerations, attribute_mapping)| {
        // set enumeration names and return the attributes and used enumerations
        attribute_mapping.iter().copied().enumerate().for_each(
            |(aidx, eidx)| {
                if let Some(eidx) = eidx {
                    attributes[aidx].try_set_enumeration(&enumerations[eidx]);
                }
            },
        );
        (
            attributes,
            attribute_mapping
                .into_iter()
                .flatten()
                .unique()
                .map(|eidx| enumerations[eidx].clone())
                .collect::<Vec<EnumerationData>>(),
        )
    });

    (
        capacity,
        any_with::<CellOrder>(Some(array_type)),
        any::<TileOrder>(),
        allow_duplicates,
        strat_attributes_enumerations,
        prop_coordinate_filters(domain.clone(), params.clone()),
        FilterPipelineStrategy::new(offsets_filters_requirements),
        FilterPipelineStrategy::new(validity_filters_requirements)
    )
        .prop_map(
            move |(
                capacity,
                cell_order,
                tile_order,
                allow_duplicates,
                (attributes, enumerations),
                coordinate_filters,
                offsets_filters,
                validity_filters,
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
                    enumerations,
                    coordinate_filters,
                    offsets_filters,
                    validity_filters,
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
    .value_tree_map(|vt| SchemaValueTree::new(vt.current()))
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

#[derive(Clone, Debug)]
pub struct SchemaValueTree {
    array_type: ArrayType,
    domain: DomainValueTree,
    capacity: Just<Option<u64>>, // TODO: make shrinkable
    cell_order: Just<Option<CellOrder>>, // TODO: make shrinkable
    tile_order: Just<Option<TileOrder>>, // TODO: make shrinkable
    allow_duplicates: Just<Option<bool>>, // TODO: make shrinkable
    all_attributes: Vec<AttributeValueTree>,
    all_enumerations: HashMap<String, EnumerationValueTree>,
    selected_attributes: RecordsValueTree<Vec<usize>>,
    coordinate_filters: FilterPipelineValueTree,
    offsets_filters: FilterPipelineValueTree,
    validity_filters: FilterPipelineValueTree,
}

impl SchemaValueTree {
    pub fn new(schema: SchemaData) -> Self {
        let num_attributes = schema.attributes.len();

        Self {
            array_type: schema.array_type,
            domain: DomainValueTree::new(schema.domain),
            capacity: Just(schema.capacity),
            cell_order: Just(schema.cell_order),
            tile_order: Just(schema.tile_order),
            allow_duplicates: Just(schema.allow_duplicates),
            all_attributes: schema
                .attributes
                .into_iter()
                .map(AttributeValueTree::new)
                .collect::<Vec<_>>(),
            all_enumerations: schema
                .enumerations
                .into_iter()
                .map(|e| {
                    (
                        e.name.clone(),
                        EnumerationValueTree::new(EnumerationData::clone(&e)),
                    )
                })
                .collect::<HashMap<String, EnumerationValueTree>>(),
            selected_attributes: RecordsValueTree::new(
                1,
                (0..num_attributes).collect::<Vec<_>>(),
            ),
            coordinate_filters: FilterPipelineValueTree::new(
                schema.coordinate_filters,
            ),
            offsets_filters: FilterPipelineValueTree::new(
                schema.offsets_filters,
            ),
            validity_filters: FilterPipelineValueTree::new(
                schema.validity_filters,
            ),
        }
    }
}

impl ValueTree for SchemaValueTree {
    type Value = SchemaData;

    fn current(&self) -> Self::Value {
        let attributes = self
            .selected_attributes
            .current()
            .into_iter()
            .map(|a| self.all_attributes[a].current())
            .collect::<Vec<_>>();

        let enumerations = attributes
            .iter()
            .filter_map(|a| a.enumeration.as_ref())
            .unique()
            .map(|e| self.all_enumerations.get(e).unwrap().current())
            .collect::<Vec<_>>();

        SchemaData {
            array_type: self.array_type,
            domain: self.domain.current(),
            capacity: self.capacity.current(),
            cell_order: self.cell_order.current(),
            tile_order: self.tile_order.current(),
            allow_duplicates: self.allow_duplicates.current(),
            attributes,
            enumerations,
            coordinate_filters: self.coordinate_filters.current(),
            offsets_filters: self.offsets_filters.current(),
            validity_filters: self.validity_filters.current(),
        }
    }

    fn simplify(&mut self) -> bool {
        self.selected_attributes.simplify()
            || self.domain.simplify()
            || self
                .selected_attributes
                .current()
                .into_iter()
                .any(|a| self.all_attributes[a].simplify())
            || self.all_enumerations.values_mut().any(|e| e.simplify())
            || self.cell_order.simplify()
            || self.tile_order.simplify()
            || self.coordinate_filters.simplify()
            || self.offsets_filters.simplify()
            || self.validity_filters.simplify()
    }

    fn complicate(&mut self) -> bool {
        self.selected_attributes.complicate()
            || self.domain.complicate()
            || self
                .selected_attributes
                .current()
                .into_iter()
                .any(|a| self.all_attributes[a].complicate())
            || self.all_enumerations.values_mut().any(|e| e.complicate())
            || self.cell_order.complicate()
            || self.tile_order.complicate()
            || self.coordinate_filters.complicate()
            || self.offsets_filters.complicate()
            || self.validity_filters.complicate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs one instance of [schema_value_tree]
    fn test_schema_value_tree(mut vt: SchemaValueTree) {
        let base = vt.current();
        assert!(!base.attributes.is_empty());

        // first shrink should reduce the number of attributes if possible
        if base.attributes.len() > 1 {
            assert!(vt.simplify());
            assert!(vt.current().attributes.len() < base.attributes.len());
        }

        // if we continue shrinking after finding the minimal attribute set
        // we should not thrash the attribute set
        while vt.selected_attributes.simplify() {
            let current = vt.current();
            assert!(!current.attributes.is_empty());
            assert!(!current.domain.dimension.is_empty());
        }

        // (this may not be generally true but it is true for RecordsStrategy)
        assert!(!vt.selected_attributes.complicate());

        while vt.simplify() {}

        let minimal = vt.current();
        assert_eq!(1, minimal.attributes.len());
        assert_eq!(1, minimal.domain.dimension.len());

        // check contract of ValueTree
        assert!(!vt.complicate());
    }

    proptest! {
        /// Test that [SchemaValueTree] shrinks in the expected way
        #[test]
        fn schema_value_tree(schema in any::<SchemaData>()) {
            let vt = SchemaValueTree::new(schema);
            test_schema_value_tree(vt)
        }
    }
}
