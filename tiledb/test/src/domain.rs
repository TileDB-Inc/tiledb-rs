use proptest::prelude::*;
use tiledb::array::{Domain, DomainBuilder};
use tiledb::context::Context;
use tiledb::Result as TileDBResult;

pub fn arbitrary(
    context: &Context,
) -> impl Strategy<Value = TileDBResult<Domain>> {
    const MIN_DIMENSIONS: usize = 1;
    const MAX_DIMENSIONS: usize = 8;

    proptest::collection::vec(
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
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that the arbitrary domain construction always succeeds
    #[test]
    fn domain_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_domain in arbitrary(&ctx))| {
            maybe_domain.expect("Error constructing arbitrary dimension");
        });
    }
}
