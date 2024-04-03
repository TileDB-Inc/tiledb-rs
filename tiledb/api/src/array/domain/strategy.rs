use std::rc::Rc;

use proptest::prelude::*;

use crate::array::dimension::strategy::*;
use crate::array::{ArrayType, DomainData};
use crate::datatype::strategy::*;

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
        ArrayType::Dense => prop_datatype_for_dense_dimension()
            .prop_flat_map(|dimension_type| {
                proptest::collection::vec(
                    prop_dimension_for_datatype(dimension_type),
                    MIN_DIMENSIONS..=MAX_DIMENSIONS,
                )
            })
            .boxed(),
        ArrayType::Sparse => proptest::collection::vec(
            prop_dimension_for_array_type(array_type),
            MIN_DIMENSIONS..=MAX_DIMENSIONS,
        )
        .boxed(),
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
