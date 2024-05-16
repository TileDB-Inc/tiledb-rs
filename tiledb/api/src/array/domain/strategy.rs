use std::rc::Rc;

use proptest::prelude::*;

use crate::array::dimension::strategy::Requirements as DimensionRequirements;
use crate::array::{ArrayType, DimensionData, DomainData};
use crate::datatype::strategy::*;
use crate::Datatype;

const MIN_DIMENSIONS: usize = 1;
const MAX_DIMENSIONS: usize = 8;

#[derive(Clone, Default)]
pub struct Requirements {
    pub array_type: Option<ArrayType>,
}

fn prop_domain_for_array_type(
    array_type: ArrayType,
) -> impl Strategy<Value = DomainData> {
    match array_type {
        ArrayType::Dense => {
            /*
             * The number of cells per tile is the product of the extents of all dimensions, we
             * have to be careful if there are many dimensions.
             * If we have D dimensions and the desired bound on the number of cells per tile is T, then we want to bound each extent on the Dth root of T
             */
            const CELLS_PER_TILE_LIMIT: usize = 1024 * 1024;

            (
                any_with::<Datatype>(DatatypeContext::DenseDimension),
                MIN_DIMENSIONS..=MAX_DIMENSIONS,
            )
                .prop_flat_map(|(dimension_type, num_dimensions)| {
                    let params = DimensionRequirements {
                        datatype: Some(dimension_type),
                        extent_limit: f64::powf(
                            CELLS_PER_TILE_LIMIT as f64,
                            1.0f64 / (num_dimensions as f64),
                        ) as usize
                            + 1, // round up, probably won't hurt, might prevent problems
                        ..Default::default()
                    };
                    proptest::collection::vec(
                        any_with::<DimensionData>(params),
                        MIN_DIMENSIONS..=MAX_DIMENSIONS,
                    )
                })
                .boxed()
        }
        ArrayType::Sparse => {
            let params = DimensionRequirements {
                array_type: Some(ArrayType::Sparse),
                ..Default::default()
            };
            proptest::collection::vec(
                any_with::<DimensionData>(params),
                MIN_DIMENSIONS..=MAX_DIMENSIONS,
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
        prop_domain_for_array_type(array_type).boxed()
    } else {
        prop_oneof![Just(ArrayType::Dense), Just(ArrayType::Sparse)]
            .prop_flat_map(prop_domain_for_array_type)
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
        assert_eq!(MIN_DIMENSIONS, last.dimension.len());
    }
}
