use std::collections::HashSet;

use proptest::prelude::*;
use proptest::test_runner::TestRng;
use serde_json::json;

use tiledb::array::attribute::{AttributeData, FillData};
use tiledb::array::schema::{CellValNum, SchemaData};
use tiledb::datatype::{Datatype, LogicalType};
use tiledb::filter::list::FilterListData;
use tiledb::{fn_typed, Result as TileDBResult};

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

fn gen_cell_val_num(
    rng: &mut TestRng,
    datatype: Datatype,
) -> Option<CellValNum> {
    if matches!(datatype, Datatype::Any) {
        return None;
    }

    let which = rng.gen_range(1..=3);
    if which == 1 {
        Some(CellValNum::single())
    } else if which == 2 {
        let cvn = rng.gen_range(2u32..6);
        Some(CellValNum::try_from(cvn).expect("Error creating cell val num."))
    } else {
        assert!(which == 3);
        Some(CellValNum::Var)
    }
}

fn gen_fill_data(rng: &mut TestRng, attr: &AttributeData) -> Option<FillData> {
    if rng.gen_bool(0.5) {
        return None;
    }

    let cell_val_num = if matches!(attr.datatype, Datatype::Any) {
        CellValNum::Var
    } else {
        attr.cell_val_num.unwrap()
    };

    fn_typed!(attr.datatype, LT, {
        type DT = <LT as LogicalType>::PhysicalType;
        let cvn = u32::from(cell_val_num) as usize;
        let count = if cvn == u32::MAX as usize {
            rng.gen_range(1..=16)
        } else {
            cvn
        };
        let mut data = Vec::new();
        for _ in 0..count {
            data.push(rng.gen::<DT>())
        }
        let nullability = if attr.nullability.unwrap_or(false) {
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

pub fn generate(
    rng: &mut TestRng,
    schema: &SchemaData,
    field_names: &mut HashSet<String>,
) -> TileDBResult<AttributeData> {
    let name = util::gen_name(rng, field_names);
    let datatype = util::choose(rng, &datatype::all_datatypes_vec());
    let mut attr = AttributeData {
        name,
        datatype,
        nullability: gen_nullability(rng),
        cell_val_num: gen_cell_val_num(rng, datatype),
        fill: None,
        filters: FilterListData::default(),
    };

    attr.fill = gen_fill_data(rng, &attr);
    attr.filters = filter_list::gen_for_attribute(rng, schema, &attr)?;

    Ok(attr)
}
