use proptest::prelude::*;
use tiledb::array::{ArrayType, Domain, DomainBuilder};
use tiledb::context::Context;
use tiledb::Result as TileDBResult;

use crate::strategy::LifetimeBoundStrategy;

pub fn arbitrary(
    context: &Context,
    array_type: ArrayType,
) -> impl Strategy<Value = TileDBResult<Domain>> {
    const MIN_DIMENSIONS: usize = 1;
    const MAX_DIMENSIONS: usize = 8;

    match array_type {
        ArrayType::Dense => unimplemented!(),
        ArrayType::Sparse => proptest::collection::vec(
            crate::dimension::arbitrary(context),
            MIN_DIMENSIONS..=MAX_DIMENSIONS,
        )
        .prop_map(|dimensions| {
            let mut d = DomainBuilder::new(context)?;
            for dim in dimensions {
                d = d.add_dimension(dim?)?;
            }
            Ok(d.build())
        })
        .bind(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that the arbitrary domain construction always succeeds
    #[test]
    fn domain_arbitrary_sparse() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_domain in arbitrary(&ctx, ArrayType::Sparse))| {
            maybe_domain.expect("Error constructing arbitrary domain");
        });
    }
}