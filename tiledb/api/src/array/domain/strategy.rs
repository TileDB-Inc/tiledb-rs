use std::rc::Rc;

use proptest::prelude::*;

use crate::array::dimension::strategy::Requirements as DimensionRequirements;
use crate::array::{ArrayType, DimensionData, DomainData};
use crate::datatype::strategy::*;
use crate::Datatype;

#[derive(Clone)]
pub struct Requirements {
    pub array_type: Option<ArrayType>,
    pub num_dimensions: std::ops::RangeInclusive<usize>,
    pub cells_per_tile_limit: usize,
    pub dimension: Option<DimensionRequirements>,
}

impl Requirements {
    pub const DEFAULT_MIN_DIMENSIONS: usize = 1;
    pub const DEFAULT_MAX_DIMENSIONS: usize = 8;

    pub const DEFAULT_CELLS_PER_TILE_LIMIT: usize = 1024 * 32;
}

impl Default for Requirements {
    fn default() -> Self {
        Requirements {
            array_type: None,
            num_dimensions: Self::DEFAULT_MIN_DIMENSIONS
                ..=Self::DEFAULT_MAX_DIMENSIONS,
            cells_per_tile_limit: Self::DEFAULT_CELLS_PER_TILE_LIMIT,
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
}

impl Arbitrary for DomainData {
    type Parameters = Rc<Requirements>;
    type Strategy = BoxedStrategy<DomainData>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        prop_domain(args.clone()).boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Context, Factory};
    use proptest::strategy::ValueTree;
    use util::option::OptionSubset;

    /// Test that the arbitrary domain construction always succeeds
    #[test]
    fn domain_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_domain in any::<DomainData>())| {
            maybe_domain.create(&ctx)
                .expect("Error constructing arbitrary domain");
        });
    }

    #[test]
    fn domain_eq_reflexivity() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(domain in any::<DomainData>())| {
            assert_eq!(domain, domain);
            assert!(domain.option_subset(&domain));

            let domain = domain.create(&ctx)
                .expect("Error constructing arbitrary domain");
            assert_eq!(domain, domain);
        });
    }

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
        assert_eq!(Requirements::DEFAULT_MIN_DIMENSIONS, last.dimension.len());
    }
}
