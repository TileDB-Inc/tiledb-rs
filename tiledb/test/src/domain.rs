use proptest::prelude::*;
use tiledb::array::{ArrayType, Domain, DomainBuilder};
use tiledb::context::Context;
use tiledb::Result as TileDBResult;

use crate::strategy::LifetimeBoundStrategy;

pub fn arbitrary_for_array_type(
    context: &Context,
    array_type: ArrayType,
) -> impl Strategy<Value = TileDBResult<Domain>> {
    const MIN_DIMENSIONS: usize = 1;
    const MAX_DIMENSIONS: usize = 8;

    match array_type {
        ArrayType::Dense => crate::datatype::arbitrary_for_dense_dimension()
            .prop_flat_map(|dimension_type| {
                proptest::collection::vec(
                    crate::dimension::arbitrary_for_type(
                        context,
                        dimension_type,
                    ),
                    MIN_DIMENSIONS..=MAX_DIMENSIONS,
                )
            })
            .bind(),
        ArrayType::Sparse => proptest::collection::vec(
            crate::dimension::arbitrary_for_array_type(context, array_type),
            MIN_DIMENSIONS..=MAX_DIMENSIONS,
        )
        .bind(),
    }
    .prop_map(|dimensions| {
        let mut d = DomainBuilder::new(context)?;
        for dim in dimensions {
            d = d.add_dimension(dim?)?;
        }
        Ok(d.build())
    })
}

pub fn arbitrary(
    context: &Context,
) -> impl Strategy<Value = TileDBResult<Domain>> {
    prop_oneof![Just(ArrayType::Dense), Just(ArrayType::Sparse)]
        .prop_flat_map(|atype| arbitrary_for_array_type(context, atype))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that the arbitrary domain construction always succeeds
    #[test]
    fn domain_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_domain in arbitrary(&ctx))| {
            maybe_domain.expect("Error constructing arbitrary domain");
        });
    }

    #[test]
    fn domain_eq_reflexivity() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_domain in arbitrary(&ctx))| {
            let domain = maybe_domain.expect("Error constructing arbitrary domain");
            assert_eq!(domain, domain);
        });
    }
}
