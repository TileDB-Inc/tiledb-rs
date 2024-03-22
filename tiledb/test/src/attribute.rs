use proptest::prelude::*;
use tiledb::array::{Attribute, AttributeBuilder};
use tiledb::context::Context;

pub fn arbitrary_name() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[a-zA-Z0-9_]*")
        .expect("Error creating attribute name strategy")
        .prop_filter(
            "Attribute names may not begin with reserved prefix",
            |name| !name.starts_with("__"),
        )
}

pub fn arbitrary(context: &Context) -> impl Strategy<Value = Attribute> {
    (arbitrary_name(), crate::datatype::arbitrary_implemented()).prop_map(
        |(name, dt)| {
            AttributeBuilder::new(context, name.as_ref(), dt)
                .expect("Error building attribute")
                .build()
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

        proptest!(|(_ in arbitrary(&ctx))| {});
    }
}
