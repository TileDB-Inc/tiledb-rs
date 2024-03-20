use proptest::prelude::*;
use tiledb::array::{Attribute, AttributeBuilder};
use tiledb::context::Context;

pub fn arbitrary_name() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[a-zA-Z0-9_]*")
        .expect("Error creating attribute name strategy")
}

pub fn arbitrary(context: &Context) -> impl Strategy<Value = Attribute> {
    (arbitrary_name(), crate::datatype::arbitrary()).prop_map(|(name, dt)| {
        AttributeBuilder::new(context, name.as_ref(), dt)
            .expect("Error building attribute")
            .build()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attribute_alloc() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(_ in arbitrary(&ctx))| {});
    }
}
