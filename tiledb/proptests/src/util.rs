use proptest::prelude::*;

use tiledb::array::schema::CellValNum;

pub fn prop_cell_val_num() -> impl Strategy<Value = CellValNum> {
    let fixed = (2u32..4).prop_map(|cvn| CellValNum::try_from(cvn).unwrap());
    prop_oneof![
        Just(CellValNum::try_from(1).unwrap()),
        fixed,
        Just(CellValNum::Var)
    ]
}
