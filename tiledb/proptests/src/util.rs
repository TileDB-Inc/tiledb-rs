use proptest::prelude::*;
use proptest::test_runner::TestRng;

use tiledb::array::schema::CellValNum;

pub fn gen_name(rng: &mut TestRng) -> String {
    let choices = "abcdefghijklmnopqrstuvwxyz\
                    ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                    0123456789_";
    let name_len = rng.gen_range(0u32..16);
    let mut data = Vec::new();
    for _ in 0..name_len {
        data.push(choose(rng, choices.as_bytes()))
    }
    String::from_utf8(data).unwrap()
}

pub fn prop_cell_val_num() -> impl Strategy<Value = CellValNum> {
    let fixed = (2u32..4).prop_map(|cvn| CellValNum::try_from(cvn).unwrap());
    prop_oneof![
        Just(CellValNum::try_from(1).unwrap()),
        fixed,
        Just(CellValNum::Var)
    ]
}

pub fn choose<T: Copy>(rng: &mut TestRng, choices: &[T]) -> T {
    assert!(choices.len() > 0);
    let idx = rng.gen_range(0..choices.len());
    choices[idx]
}
