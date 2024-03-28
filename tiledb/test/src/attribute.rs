use proptest::prelude::*;
use serde_json::json;

use tiledb::array::{attribute::FillData, AttributeData};
use tiledb::filter_list::FilterListData;
use tiledb::{fn_typed, Datatype};

use crate::*;

fn prop_nullable() -> impl Strategy<Value = bool> {
    any::<bool>()
}

fn prop_cell_val_num() -> impl Strategy<Value = Option<u32>> {
    Just(None)
}

fn prop_fill<T: Arbitrary>() -> impl Strategy<Value = T> {
    any::<T>()
}

fn prop_filters(datatype: Datatype) -> impl Strategy<Value = FilterListData> {
    prop_filter_pipeline_for_datatype(datatype)
}

pub fn prop_attribute_for_datatype(
    datatype: Datatype,
) -> impl Strategy<Value = AttributeData> {
    fn_typed!(datatype, DT, {
        let name = prop_field_name();
        let nullable = prop_nullable();
        let cell_val_num = prop_cell_val_num();
        let fill = prop_fill::<DT>();
        let fill_nullable = any::<bool>();
        let filters = prop_filters(datatype);
        (name, nullable, cell_val_num, fill, fill_nullable, filters)
            .prop_map(
                move |(
                    name,
                    nullable,
                    cell_val_num,
                    fill,
                    fill_nullable,
                    filters,
                )| {
                    AttributeData {
                        name,
                        datatype,
                        nullability: Some(nullable),
                        cell_val_num,
                        fill: Some(FillData {
                            data: json!(fill),
                            nullability: Some(nullable && fill_nullable),
                        }),
                        filters,
                    }
                },
            )
            .boxed()
    })
}

pub fn prop_attribute() -> impl Strategy<Value = AttributeData> {
    prop_datatype_implemented().prop_flat_map(prop_attribute_for_datatype)
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
            let attr = attr.create(&ctx)
                .expect("Error constructing arbitrary attribute");
            assert_eq!(attr, attr);
        });
    }
}
