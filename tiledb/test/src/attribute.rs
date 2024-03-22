use proptest::prelude::*;
use tiledb::array::{Attribute, AttributeBuilder};
use tiledb::context::Context;
use tiledb::Result as TileDBResult;

pub fn arbitrary_name() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[a-zA-Z0-9_]*")
        .expect("Error creating attribute name strategy")
        .prop_filter(
            "Attribute names may not begin with reserved prefix",
            |name| !name.starts_with("__"),
        )
}

pub fn arbitrary(
    context: &Context,
) -> impl Strategy<Value = TileDBResult<Attribute>> {
    (arbitrary_name(), crate::datatype::arbitrary_implemented()).prop_flat_map(
        |(name, dt)| {
            (
                Just(name),
                Just(dt),
                crate::filter::arbitrary_list_for_datatype(context, dt),
            )
                .prop_map(|(name, dt, filters)| {
                    Ok(AttributeBuilder::new(context, name.as_ref(), dt)?
                        .filter_list(filters?)?
                        .build())
                })
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that the arbitrary attribute construction always succeeds
    #[test]
    fn attribute_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(attr in arbitrary(&ctx))| {
            attr.expect("Error constructing arbitrary attribute");
        });
    }

    #[test]
    fn attribute_eq_reflexivity() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(attr in arbitrary(&ctx))| {
            let attr = attr.expect("Error constructing arbitrary attribute");
            assert_eq!(attr, attr);
        });
    }
}
