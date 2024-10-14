use std::rc::Rc;

use proptest::prelude::*;
use proptest::sample::select;
use proptest::strategy::ValueTree;
use tiledb_common::array::ArrayType;
use tiledb_common::datatype::strategy::*;
use tiledb_common::datatype::Datatype;
use tiledb_test_utils::strategy::records::RecordsValueTree;
use tiledb_test_utils::strategy::StrategyExt;

use crate::array::dimension::strategy::{
    DimensionValueTree, Requirements as DimensionRequirements,
};
use crate::array::dimension::DimensionData;
use crate::array::domain::DomainData;

#[derive(Clone)]
pub struct Requirements {
    pub array_type: Option<ArrayType>,
    pub num_dimensions: std::ops::RangeInclusive<usize>,
    pub cells_per_tile_limit: usize,
    pub dimension: Option<DimensionRequirements>,
}

impl Requirements {
    pub fn env_max_dimensions() -> Option<usize> {
        crate::strategy::config::TILEDB_STRATEGY_DOMAIN_PARAMETERS_DIMENSIONS_MAX.environmental()
    }

    pub fn min_dimensions_default() -> usize {
        **crate::strategy::config::TILEDB_STRATEGY_DOMAIN_PARAMETERS_DIMENSIONS_MIN
    }

    pub fn max_dimensions_default() -> usize {
        **crate::strategy::config::TILEDB_STRATEGY_DOMAIN_PARAMETERS_DIMENSIONS_MAX
    }

    pub fn cells_per_tile_limit_default() -> usize {
        **crate::strategy::config::TILEDB_STRATEGY_DOMAIN_PARAMETERS_CELLS_PER_TILE_LIMIT
    }
}

impl Default for Requirements {
    fn default() -> Self {
        Requirements {
            array_type: None,
            num_dimensions: Self::min_dimensions_default()
                ..=Self::max_dimensions_default(),
            cells_per_tile_limit: Self::cells_per_tile_limit_default(),
            dimension: None,
        }
    }
}

fn prop_domain_for_array_type(
    array_type: ArrayType,
    params: &Requirements,
) -> impl Strategy<Value = DomainData> {
    let dimension_params = params.dimension.clone().unwrap_or_default();

    match array_type {
        ArrayType::Dense => {
            let cells_per_tile_limit = params.cells_per_tile_limit;

            /*
             * The number of cells per tile is the product of the extents of all
             * dimensions, we have to be careful if there are many dimensions.
             * If we have D dimensions and the desired bound on the number of
             * cells per tile is T, then we want to bound each extent on
             * the Dth root of T
             */
            (
                any_with::<Datatype>(DatatypeContext::DenseDimension),
                params.num_dimensions.clone(),
            )
                .prop_flat_map(
                    move |(dimension_type, actual_num_dimensions)| {
                        let dimension_params = DimensionRequirements {
                            datatype: Some(dimension_type),
                            extent_limit: {
                                // Dth root of T is the same as T^(1/D)
                                f64::powf(
                                    cells_per_tile_limit as f64,
                                    1.0f64 / (actual_num_dimensions as f64),
                                ) as usize
                                    + 1 // round up, probably won't hurt, might prevent problems
                            },
                            ..dimension_params.clone()
                        };
                        proptest::collection::vec(
                            any_with::<DimensionData>(dimension_params),
                            actual_num_dimensions,
                        )
                    },
                )
                .boxed()
        }
        ArrayType::Sparse => {
            let dimension_params = DimensionRequirements {
                array_type: Some(ArrayType::Sparse),
                ..dimension_params
            };
            proptest::collection::vec(
                any_with::<DimensionData>(dimension_params),
                params.num_dimensions.clone(),
            )
            .boxed()
        }
    }
    .prop_map(|dimension| DomainData { dimension })
}

fn prop_domain(
    requirements: Rc<Requirements>,
) -> impl Strategy<Value = DomainData> {
    if let Some(array_type) = requirements.array_type {
        prop_domain_for_array_type(array_type, requirements.as_ref()).boxed()
    } else {
        prop_oneof![Just(ArrayType::Dense), Just(ArrayType::Sparse)]
            .prop_flat_map(move |a| {
                prop_domain_for_array_type(a, requirements.as_ref())
            })
            .boxed()
    }
    .value_tree_map(|vt| DomainValueTree::new(vt.current()))
}

impl Arbitrary for DomainData {
    type Parameters = Rc<Requirements>;
    type Strategy = BoxedStrategy<DomainData>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        prop_domain(args.clone()).boxed()
    }
}

impl DomainData {
    /// Returns a strategy which chooses any dimension from `self.`
    pub fn strat_dimension(&self) -> impl Strategy<Value = DimensionData> {
        select(self.dimension.clone())
    }
}

#[derive(Clone, Debug)]
pub struct DomainValueTree {
    all_dimensions: Vec<DimensionValueTree>,
    selected_dimensions: RecordsValueTree<Vec<usize>>,
}

impl DomainValueTree {
    pub fn new(domain: DomainData) -> Self {
        let num_dimension = domain.dimension.len();

        Self {
            all_dimensions: domain
                .dimension
                .into_iter()
                .map(DimensionValueTree::new)
                .collect::<Vec<_>>(),
            selected_dimensions: RecordsValueTree::new(
                1,
                (0..num_dimension).collect::<Vec<_>>(),
            ),
        }
    }
}

impl ValueTree for DomainValueTree {
    type Value = DomainData;

    fn current(&self) -> Self::Value {
        DomainData {
            dimension: self
                .selected_dimensions
                .current()
                .into_iter()
                .map(|i| self.all_dimensions[i].current())
                .collect::<Vec<DimensionData>>(),
        }
    }

    fn simplify(&mut self) -> bool {
        self.selected_dimensions.simplify()
            || self.all_dimensions.iter_mut().any(|d| d.simplify())
    }

    fn complicate(&mut self) -> bool {
        self.selected_dimensions.complicate()
            || self.all_dimensions.iter_mut().any(|d| d.complicate())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::strategy::ValueTree;

    #[test]
    fn domain_shrinking() {
        let strat = any::<DomainData>();

        let mut runner =
            proptest::test_runner::TestRunner::new(Default::default());

        let mut value = loop {
            let value = strat.new_tree(&mut runner).unwrap();
            if value.current().dimension.len() > 4 {
                break value;
            }
        };

        let init = value.current();
        for _ in 0..runner.config().max_shrink_iters {
            if value.simplify() {
                assert_ne!(init, value.current());
            } else {
                break;
            }
        }
        let last = value.current();
        assert_ne!(init, last);
        assert_eq!(1, last.dimension.len());
    }
}
