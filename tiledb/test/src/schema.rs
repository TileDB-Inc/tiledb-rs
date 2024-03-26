use proptest::prelude::*;
use tiledb::array::{ArrayType, Schema, SchemaBuilder};
use tiledb::context::Context;
use tiledb::Result as TileDBResult;

pub fn arbitrary_array_type() -> impl Strategy<Value = ArrayType> {
    prop_oneof![Just(ArrayType::Dense), Just(ArrayType::Sparse),]
}

pub fn arbitrary(
    context: &Context,
) -> impl Strategy<Value = TileDBResult<Schema>> {
    const MIN_ATTRS: usize = 1;
    const MAX_ATTRS: usize = 128;

    arbitrary_array_type()
        .prop_flat_map(move |array_type|
            (
                Just(array_type),
                crate::domain::arbitrary_for_array_type(context, array_type),
                proptest::collection::vec(crate::attribute::arbitrary(context), MIN_ATTRS..=MAX_ATTRS)
            ))
        .prop_map(|(array_type, domain, attrs)| {
            /* TODO: cell order, tile order, capacity, duplicates */
            let mut b = SchemaBuilder::new(context, array_type, domain?)?;
            for attr in attrs {
                /* TODO: how to ensure no duplicate names, assuming that matters? */
                b = b.add_attribute(attr?)?
            }

            Ok(b.build())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that the arbitrary schema construction always succeeds
    #[test]
    fn schema_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_schema in arbitrary(&ctx))| {
            maybe_schema.expect("Error constructing arbitrary schema");
        });
    }
}
