use proptest::prelude::*;

pub fn prop_field_name() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[a-zA-Z0-9_]*")
        .expect("Error creating field name strategy")
        .prop_filter("Field names may not begin with reserved prefix", |name| {
            !name.starts_with("__")
        })
}
