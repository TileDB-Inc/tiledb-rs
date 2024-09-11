use std::cmp::Ordering;

use proptest::collection::vec;
use proptest::prelude::*;

use crate::array::EnumerationData;
use crate::datatype::PhysicalType;
use crate::{physical_type_go, Datatype};

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

fn do_cmp<T: PhysicalType>(a: &T, b: &T) -> Ordering {
    a.bits_cmp(b)
}

fn prop_enumeration_values(
    datatype: Datatype,
    min_variants: usize,
    max_variants: usize,
) -> BoxedStrategy<Box<[u8]>> {
    physical_type_go!(datatype, DT, {
        vec(any::<DT>(), min_variants..=max_variants)
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
    min_variants: usize,
    max_variants: usize,
) -> impl Strategy<Value = EnumerationData> {
    let name = prop_enumeration_name();
    let ordered = prop_ordered();
    let cell_val_num = prop_cell_val_num();
    let data = prop_enumeration_values(datatype, min_variants, max_variants);
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

pub struct Parameters {
    datatype: BoxedStrategy<Datatype>,
    min_variants: usize,
    max_variants: usize,
}

impl Parameters {
    fn min_variants_default() -> usize {
        const DEFAULT_MIN_ENUMERATION_VALUES: usize = 1;

        let env = "TILEDB_STRATEGY_ENUMERATION_PARAMETERS_NUM_VARIANTS_MIN";
        crate::env::parse::<usize>(env)
            .unwrap_or(DEFAULT_MIN_ENUMERATION_VALUES)
    }

    fn max_variants_default() -> usize {
        const DEFAULT_MAX_ENUMERATION_VALUES: usize = 1024;

        let env = "TILEDB_STRATEGY_ENUMERATION_PARAMETERS_NUM_VARIANTS_MAX";
        crate::env::parse::<usize>(env)
            .unwrap_or(DEFAULT_MAX_ENUMERATION_VALUES)
    }
}

impl Default for Parameters {
    fn default() -> Self {
        Parameters {
            datatype: any::<Datatype>().boxed(),
            min_variants: Self::min_variants_default(),
            max_variants: Self::max_variants_default(),
        }
    }
}

impl Arbitrary for EnumerationData {
    type Parameters = Parameters;
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        params
            .datatype
            .prop_flat_map(move |dt| {
                prop_enumeration_for_datatype(
                    dt,
                    params.min_variants,
                    params.max_variants,
                )
            })
            .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Context, Factory};

    /// Test that the arbitrary enumeration construction always succeeds
    #[test]
    fn enumeration_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(enmr in any::<EnumerationData>())| {
            enmr.create(&ctx).expect("Error constructing arbitrary enumeration");
        });
    }

    #[test]
    fn enumeration_eq_reflexivity() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(enmr in any::<EnumerationData>())| {
            let enmr = enmr.create(&ctx)
                .expect("Error constructing arbitrary enumeration");
            assert_eq!(enmr, enmr);
        });
    }
}
