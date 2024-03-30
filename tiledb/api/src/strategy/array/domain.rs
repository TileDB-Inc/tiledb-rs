use proptest::prelude::*;

use crate::array::dimension::strategy::*;
use crate::array::{ArrayType, DomainData};
use crate::strategy::datatype::*;

pub fn prop_domain_for_array_type(
    array_type: ArrayType,
) -> impl Strategy<Value = DomainData> {
    const MIN_DIMENSIONS: usize = 1;
    const MAX_DIMENSIONS: usize = 8;

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

pub fn prop_domain() -> impl Strategy<Value = DomainData> {
    prop_oneof![Just(ArrayType::Dense), Just(ArrayType::Sparse)]
        .prop_flat_map(prop_domain_for_array_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Context, Factory};

    /// Test that the arbitrary domain construction always succeeds
    #[test]
    fn domain_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_domain in prop_domain())| {
            maybe_domain.create(&ctx)
                .expect("Error constructing arbitrary domain");
        });
    }

    #[test]
    fn domain_eq_reflexivity() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_domain in prop_domain())| {
            let domain = maybe_domain.create(&ctx)
                .expect("Error constructing arbitrary domain");
            assert_eq!(domain, domain);
        });
    }
}
