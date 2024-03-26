use proptest::prelude::*;
use tiledb::array::{Attribute, AttributeBuilder};
use tiledb::context::Context;
use tiledb::{fn_typed, Datatype, Result as TileDBResult};

use crate::strategy::LifetimeBoundStrategy;

pub fn arbitrary_name() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[a-zA-Z0-9_]*")
        .expect("Error creating attribute name strategy")
        .prop_filter(
            "Attribute names may not begin with reserved prefix",
            |name| !name.starts_with("__"),
        )
}

fn arbitrary_fill_value<T>() -> impl Strategy<Value = T>
where
    T: Arbitrary,
{
    proptest::prelude::any::<T>()
}

pub fn arbitrary_for_datatype(
    context: &Context,
    datatype: Datatype,
) -> impl Strategy<Value = TileDBResult<Attribute<'_>>> {
    proptest::prelude::any::<bool>()
        .prop_flat_map(move |nullable| {
            (Just(nullable),
            if nullable { proptest::prelude::any::<bool>().bind() } else { Just(false).bind() })
                .prop_flat_map(move |(nullable, fill_nullable)| {
                    fn_typed!(arbitrary_fill_value, datatype => {
                        (arbitrary_name(), Just(nullable), crate::filter::arbitrary_list_for_datatype(context, datatype), arbitrary_fill_value, Just(fill_nullable))
                            .prop_map(move |(name, nullable, filters, fill, fill_nullable)| {
                                Ok(AttributeBuilder::new(context, name.as_ref(), datatype)?
                                    .nullability(nullable)?
                                    .fill_value_nullability(fill, fill_nullable)?
                                    .filter_list(&filters?)?
                                    .build())
                            }).bind()
                    })
            })
        })
}

pub fn arbitrary(
    context: &Context,
) -> impl Strategy<Value = TileDBResult<Attribute>> {
    crate::datatype::arbitrary_implemented()
        .prop_flat_map(|dt| arbitrary_for_datatype(context, dt))
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
