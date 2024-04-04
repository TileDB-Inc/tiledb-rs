use std::cmp::Ordering;

use proptest::collection::vec;
use proptest::prelude::*;
use util::numbers::AnyNumCmp;

use crate::array::EnumerationData;
use crate::datatype::strategy::*;
use crate::{fn_typed, Datatype};

const MIN_ENUMERATION_VALUES: usize = 1;
const MAX_ENUMERATION_VALUES: usize = 1024;

pub fn prop_enumeration_name() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[a-zA-Z0-9_]+")
        .expect("Error creating enumeration name strategy")
}

fn prop_cell_val_num() -> impl Strategy<Value = Option<u32>> {
    Just(None)
}

fn prop_ordered() -> impl Strategy<Value = bool> {
    any::<bool>()
}

fn do_cmp<T: AnyNumCmp>(a: &T, b: &T) -> Ordering {
    a.cmp(b)
}

fn prop_enumeration_values(datatype: Datatype) -> BoxedStrategy<Box<[u8]>> {
    fn_typed!(datatype, DT, {
        vec(any::<DT>(), MIN_ENUMERATION_VALUES..=MAX_ENUMERATION_VALUES)
            .prop_map(|data| {
                let mut data = data;
                data.sort_unstable_by(do_cmp);
                data.dedup();
                let data = unsafe {
                    std::slice::from_raw_parts(
                        data.as_ptr() as *const std::ffi::c_void as *const u8,
                        std::mem::size_of_val(&data[..]),
                    )
                };
                Box::from(data)
            })
            .boxed()
    })
}

pub fn prop_enumeration_for_datatype(
    datatype: Datatype,
) -> impl Strategy<Value = EnumerationData> {
    let name = prop_enumeration_name();
    let ordered = prop_ordered();
    let cell_val_num = prop_cell_val_num();
    let data = prop_enumeration_values(datatype);
    (name, ordered, cell_val_num, data)
        .prop_map(move |(name, ordered, cell_val_num, data)| EnumerationData {
            name,
            datatype,
            ordered: Some(ordered),
            cell_val_num,
            data,
            offsets: None,
        })
        .boxed()
}

pub fn prop_enumeration() -> impl Strategy<Value = EnumerationData> {
    prop_datatype_implemented().prop_flat_map(prop_enumeration_for_datatype)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Context, Factory};

    /// Test that the arbitrary enumeration construction always succeeds
    #[test]
    fn enumeration_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(enmr in prop_enumeration())| {
            enmr.create(&ctx).expect("Error constructing arbitrary enumeration");
        });
    }

    #[test]
    fn enumeration_eq_reflexivity() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(enmr in prop_enumeration())| {
            let enmr = enmr.create(&ctx)
                .expect("Error constructing arbitrary enumeration");
            assert_eq!(enmr, enmr);
        });
    }
}
