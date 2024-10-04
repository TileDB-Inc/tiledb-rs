use std::collections::HashSet;
use std::num::NonZeroU32;
use std::rc::Rc;

use proptest::prelude::*;
use proptest::sample::select;
use proptest::strategy::ValueTree;
use tiledb_test_utils::strategy::records::RecordsValueTree;
use tiledb_test_utils::strategy::StrategyExt;

use crate::array::attribute::strategy::{
    prop_attribute, AttributeValueTree, Requirements as AttributeRequirements,
    StrategyContext as AttributeContext,
};
use crate::array::domain::strategy::{
    DomainValueTree, Requirements as DomainRequirements,
};
use crate::array::{
    schema::FieldData, ArrayType, AttributeData, CellOrder, CellValNum,
    DimensionData, DomainData, SchemaData, TileOrder,
};
use crate::filter::list::FilterListData;
use crate::filter::strategy::{
    FilterPipelineValueTree, Requirements as FilterRequirements,
    StrategyContext as FilterContext,
};

impl Arbitrary for ArrayType {
    type Parameters = ();
    type Strategy = BoxedStrategy<ArrayType>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        prop_oneof![Just(ArrayType::Dense), Just(ArrayType::Sparse),].boxed()
    }
}

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
        **crate::strategy::config::TILEDB_STRATEGY_SCHEMA_PARAMETERS_ATTRIBUTES_MIN
    }

    pub fn max_attributes_default() -> usize {
        **crate::strategy::config::TILEDB_STRATEGY_SCHEMA_PARAMETERS_ATTRIBUTES_MAX
    }

    pub fn min_sparse_tile_capacity_default() -> u64 {
        **crate::strategy::config::TILEDB_STRATEGY_SCHEMA_PARAMETERS_SPARSE_TILE_CAPACITY_MIN
    }

    pub fn max_sparse_tile_capacity_default() -> u64 {
        **crate::strategy::config::TILEDB_STRATEGY_SCHEMA_PARAMETERS_SPARSE_TILE_CAPACITY_MIN
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
    selected_attributes: RecordsValueTree<Vec<usize>>,
    coordinate_filters: FilterPipelineValueTree,
    offsets_filters: FilterPipelineValueTree,
    nullity_filters: FilterPipelineValueTree,
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
            nullity_filters: FilterPipelineValueTree::new(
                schema.nullity_filters,
            ),
        }
    }
}

impl ValueTree for SchemaValueTree {
    type Value = SchemaData;

    fn current(&self) -> Self::Value {
        SchemaData {
            array_type: self.array_type,
            domain: self.domain.current(),
            capacity: self.capacity.current(),
            cell_order: self.cell_order.current(),
            tile_order: self.tile_order.current(),
            allow_duplicates: self.allow_duplicates.current(),
            attributes: self
                .selected_attributes
                .current()
                .into_iter()
                .map(|a| self.all_attributes[a].current())
                .collect::<Vec<_>>(),
            coordinate_filters: self.coordinate_filters.current(),
            offsets_filters: self.offsets_filters.current(),
            nullity_filters: self.nullity_filters.current(),
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
            || self.cell_order.simplify()
            || self.tile_order.simplify()
            || self.coordinate_filters.simplify()
            || self.offsets_filters.simplify()
            || self.nullity_filters.simplify()
    }

    fn complicate(&mut self) -> bool {
        self.selected_attributes.complicate()
            || self.domain.complicate()
            || self
                .selected_attributes
                .current()
                .into_iter()
                .any(|a| self.all_attributes[a].complicate())
            || self.cell_order.complicate()
            || self.tile_order.complicate()
            || self.coordinate_filters.complicate()
            || self.offsets_filters.complicate()
            || self.nullity_filters.complicate()
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
        while vt.selected_attributes.simplify() {}
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
