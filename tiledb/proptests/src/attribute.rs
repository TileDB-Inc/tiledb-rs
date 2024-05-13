use proptest::collection::vec;
use proptest::prelude::*;
use serde_json::json;

use tiledb::array::attribute::{AttributeData, FillData};
use tiledb::array::schema::{CellValNum, SchemaData};
use tiledb::datatype::{Datatype, LogicalType};
use tiledb::fn_typed;

use crate::datatype as pt_datatype;
use crate::filter::list as pt_list;

pub fn prop_attribute_name() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[a-zA-Z0-9_]*")
        .expect("Error creating attribute name property")
        .prop_flat_map(|name| {
            if name.starts_with("__") {
                Just("a".to_string() + &name)
            } else {
                Just(name)
            }
        })
}

fn add_nullability(
    attr: AttributeData,
) -> impl Strategy<Value = AttributeData> {
    (Just(attr), any::<bool>()).prop_flat_map(|(mut attr, nullability)| {
        attr.nullability = Some(nullability);
        Just(attr)
    })
}

fn add_cell_val_num(
    attr: AttributeData,
) -> impl Strategy<Value = AttributeData> {
    let cvn = prop_oneof![
        2 => Just(CellValNum::single()),
        1 => (2u32..6).prop_flat_map(|cvn| {
            let cvn = CellValNum::try_from(cvn).expect("Error creating cell_val_num for attribute");
            Just(cvn)
        }),
        2 => Just(CellValNum::Var)
    ];

    (Just(attr), cvn).prop_flat_map(|(mut attr, cvn)| {
        if matches!(attr.datatype, Datatype::Any) {
            attr.cell_val_num = None;
        } else {
            attr.cell_val_num = Some(cvn);
        }
        Just(attr)
    })
}

fn prop_fill_data(
    datatype: Datatype,
    cell_val_num: CellValNum,
    attr_nullability: bool,
) -> impl Strategy<Value = FillData> {
    fn_typed!(datatype, LT, {
        type DT = <LT as LogicalType>::PhysicalType;
        let cvn = u32::from(cell_val_num) as usize;
        let cvn_range = if cvn == u32::MAX as usize {
            1..=16
        } else {
            cvn..=cvn
        };
        let data = vec(any::<DT>(), cvn_range);
        let nullability = any::<bool>();
        (data, nullability)
            .prop_map(move |(data, nullability)| {
                let nullability = if attr_nullability {
                    Some(nullability)
                } else {
                    None
                };
                FillData {
                    data: json!(data),
                    nullability,
                }
            })
            .boxed()
    })
}

fn add_fill(attr: AttributeData) -> impl Strategy<Value = AttributeData> {
    let prop = prop_fill_data(
        attr.datatype,
        attr.cell_val_num.unwrap_or(CellValNum::Var),
        attr.nullability.unwrap(),
    );
    (Just(attr), prop).prop_flat_map(|(mut attr, fill_data)| {
        attr.fill = Some(fill_data);
        Just(attr)
    })
}

fn add_filters(attr: AttributeData) -> impl Strategy<Value = AttributeData> {
    let filters = pt_list::prop_filter_list(
        attr.datatype,
        attr.cell_val_num.unwrap_or(CellValNum::Var),
        6,
    );
    (Just(attr), filters).prop_flat_map(|(mut attr, filters)| {
        attr.filters = filters;
        Just(attr)
    })
}

pub fn prop_attribute_for(
    _schema: &SchemaData,
) -> impl Strategy<Value = AttributeData> {
    let name = prop_attribute_name();
    let datatype = pt_datatype::prop_all_datatypes();
    (name, datatype).prop_flat_map(|(name, datatype)| {
        let attr = AttributeData {
            name,
            datatype,
            ..Default::default()
        };

        add_nullability(attr)
            .prop_flat_map(add_cell_val_num)
            .prop_flat_map(add_fill)
            .prop_flat_map(add_filters)
    })
}
