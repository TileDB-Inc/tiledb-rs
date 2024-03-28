use proptest::prelude::*;
use tiledb::array::{ArrayType, DomainData};

use crate::*;

pub fn arbitrary_for_array_type(
    array_type: ArrayType,
) -> impl Strategy<Value = DomainData> {
    const MIN_DIMENSIONS: usize = 1;
    const MAX_DIMENSIONS: usize = 8;

    match array_type {
        ArrayType::Dense => datatype::prop_datatype_for_dense_dimension()
            .prop_flat_map(|dimension_type| {
                proptest::collection::vec(
                    dimension::prop_dimension_for_datatype(dimension_type),
                    MIN_DIMENSIONS..=MAX_DIMENSIONS,
                )
            })
            .boxed(),
        ArrayType::Sparse => proptest::collection::vec(
            dimension::prop_dimension_for_array_type(array_type),
            MIN_DIMENSIONS..=MAX_DIMENSIONS,
        )
        .boxed(),
    }
    .prop_map(|dimension| DomainData { dimension })
}

pub fn arbitrary() -> impl Strategy<Value = DomainData> {
    prop_oneof![Just(ArrayType::Dense), Just(ArrayType::Sparse)]
        .prop_flat_map(arbitrary_for_array_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tiledb::{Context, Factory};

    /// Test that the arbitrary domain construction always succeeds
    #[test]
    fn domain_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_domain in arbitrary())| {
            maybe_domain.create(&ctx)
                .expect("Error constructing arbitrary domain");
        });
    }

    #[test]
    fn domain_eq_reflexivity() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_domain in arbitrary())| {
            let domain = maybe_domain.create(&ctx)
                .expect("Error constructing arbitrary domain");
            assert_eq!(domain, domain);
        });
    }
}
