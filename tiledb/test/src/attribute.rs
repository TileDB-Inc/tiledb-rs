use proptest::prelude::*;
use serde_json::json;
use tiledb::array::{attribute::FillData, AttributeData};

use tiledb::{fn_typed, Datatype};

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
    datatype: Datatype,
) -> impl Strategy<Value = AttributeData> {
    proptest::prelude::any::<bool>().prop_flat_map(move |nullable| {
        (
            Just(nullable),
            if nullable {
                proptest::prelude::any::<bool>().bind()
            } else {
                Just(false).bind()
            },
        )
            .prop_flat_map(move |(nullable, fill_nullable)| {
                fn_typed!(datatype, DT, {
                    (
                        arbitrary_name(),
                        Just(nullable),
                        crate::filter::arbitrary_list_for_datatype(datatype),
                        arbitrary_fill_value::<DT>(),
                        Just(fill_nullable),
                    )
                        .prop_map(
                            move |(
                                name,
                                nullable,
                                filters,
                                fill,
                                fill_nullable,
                            )| {
                                AttributeData {
                                    name,
                                    datatype,
                                    nullability: Some(nullable),
                                    cell_val_num: None,
                                    fill: Some(FillData {
                                        data: json!(fill),
                                        nullability: Some(fill_nullable),
                                    }),
                                    filters,
                                }
                            },
                        )
                        .bind()
                })
            })
    })
}

pub fn arbitrary() -> impl Strategy<Value = AttributeData> {
    crate::datatype::arbitrary_implemented()
        .prop_flat_map(arbitrary_for_datatype)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tiledb::{Context, Factory};

    /// Test that the arbitrary attribute construction always succeeds
    #[test]
    fn attribute_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(attr in arbitrary())| {
            attr.create(&ctx).expect("Error constructing arbitrary attribute");
        });
    }

    #[test]
    fn attribute_eq_reflexivity() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(attr in arbitrary())| {
            let attr = attr.create(&ctx).expect("Error constructing arbitrary attribute");
            assert_eq!(attr, attr);
        });
    }
}
