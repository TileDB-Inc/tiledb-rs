use proptest::prelude::*;
use proptest::test_runner::TestRng;
use serde_json::json;

use tiledb::array::attribute::{AttributeData, FillData};
use tiledb::array::schema::{CellValNum, SchemaData};
use tiledb::datatype::LogicalType;
use tiledb::filter::list::FilterListData;
use tiledb::fn_typed;

use crate::datatype;
use crate::filter_list;
use crate::util;

fn gen_nullability(rng: &mut TestRng) -> Option<bool> {
    if rng.gen_bool(0.5) {
        Some(rng.gen_bool(0.5))
    } else {
        None
    }
}

fn gen_cell_val_num(rng: &mut TestRng) -> Option<CellValNum> {
    let which = rng.gen_range(1..=4);
    if which == 1 {
        Some(CellValNum::single())
    } else if which == 2 {
        let cvn = rng.gen_range(2u32..6);
        Some(CellValNum::try_from(cvn).expect("Error creating cell val num."))
    } else if which == 3 {
        Some(CellValNum::Var)
    } else {
        None
    }
}

fn gen_fill_data(rng: &mut TestRng, attr: &AttributeData) -> Option<FillData> {
    if rng.gen_bool(0.5) {
        return None;
    }

    fn_typed!(attr.datatype, LT, {
        type DT = <LT as LogicalType>::PhysicalType;
        let cvn = u32::from(attr.cell_val_num.unwrap()) as usize;
        let count = if cvn == u32::MAX as usize {
            rng.gen_range(1..=16)
        } else {
            cvn
        };
        let mut data = Vec::new();
        for _ in 0..count {
            data.push(rng.gen::<DT>())
        }
        let nullability = if attr.nullability.unwrap() {
            Some(rng.gen_bool(0.5))
        } else {
            None
        };
        Some(FillData {
            data: json!(data),
            nullability,
        })
    })
}

// pub struct AttributeData {
//     pub name: String,
//     pub datatype: Datatype,
//     pub nullability: Option<bool>,
//     pub cell_val_num: Option<CellValNum>,
//     pub fill: Option<FillData>,
//     pub filters: FilterListData,
// }

pub fn generate(rng: &mut TestRng, _schema: &SchemaData) -> AttributeData {
    let name = util::gen_name(rng);
    let datatype = util::choose(rng, &datatype::all_datatypes_vec());
    let mut attr = AttributeData {
        name,
        datatype,
        nullability: gen_nullability(rng),
        cell_val_num: gen_cell_val_num(rng),
        fill: None,
        filters: FilterListData::default(),
    };

    attr.fill = gen_fill_data(rng, &attr);
    attr.filters = filter_list::gen_for_attribute(rng, attr);

    attr
}
