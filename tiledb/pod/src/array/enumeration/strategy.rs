use std::cmp::Ordering;

use proptest::collection::vec;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use strategy_ext::StrategyExt;
use strategy_ext::records::RecordsValueTree;
use tiledb_common::array::CellValNum;
use tiledb_common::datatype::physical::{BitsEq, BitsOrd};
use tiledb_common::{Datatype, physical_type_go};

use crate::array::enumeration::EnumerationData;

impl EnumerationData {
    /// Returns a strategy which produces a valid
    /// key datatype for this enumeration.
    pub fn key_datatype_strategy(
        &self,
    ) -> impl Strategy<Value = Datatype> + use<> {
        let nv = self.num_variants();
        let mut candidates = Vec::new();

        for dt in Datatype::iter() {
            let Some(max_variants) = dt.max_enumeration_variants() else {
                continue;
            };
            if nv <= max_variants {
                candidates.push(dt)
            }
        }
        proptest::sample::select(candidates)
    }
}

pub fn prop_enumeration_name() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[a-zA-Z0-9_]+")
        .expect("Error creating enumeration name strategy")
}

fn prop_ordered() -> impl Strategy<Value = bool> {
    any::<bool>()
}

fn do_cmp<T: BitsOrd>(a: &T, b: &T) -> Ordering {
    a.bits_cmp(b)
}

fn do_dedup<T: BitsEq>(a: &mut T, b: &mut T) -> bool {
    a.bits_eq(b)
}

type EnumerationBytes = (Box<[u8]>, Option<Box<[u64]>>);

fn prop_enumeration_values(
    datatype: Datatype,
    cell_val_num: CellValNum,
    params: &Parameters,
) -> BoxedStrategy<EnumerationBytes> {
    fn to_enumeration_values<T>(data: Vec<T>) -> Vec<T>
    where
        T: BitsEq + BitsOrd,
    {
        let mut data = data;
        data.sort_unstable_by(do_cmp);
        data.dedup_by(do_dedup);
        data
    }

    fn to_raw<T>(vec: Vec<T>) -> Box<[u8]> {
        let data = unsafe {
            std::slice::from_raw_parts(
                vec.as_ptr() as *const std::ffi::c_void as *const u8,
                std::mem::size_of_val(&vec[..]),
            )
        };
        Box::from(data)
    }

    physical_type_go!(
        datatype,
        DT,
        match cell_val_num {
            CellValNum::Fixed(nz) if nz.get() == 1 => {
                vec(any::<DT>(), params.min_variants..=params.max_variants)
                    .prop_map(|v| (to_raw(to_enumeration_values(v)), None))
                    .boxed()
            }
            CellValNum::Fixed(nz) => {
                vec(
                    vec(any::<DT>(), nz.get() as usize),
                    params.min_variants..=params.max_variants,
                )
                .prop_map(|v| {
                    (
                        to_raw(
                            to_enumeration_values(v)
                                .into_iter()
                                .flatten()
                                .collect::<Vec<DT>>(),
                        ),
                        None,
                    )
                })
                .boxed()
            }
            CellValNum::Var => {
                vec(
                    vec(
                        any::<DT>(),
                        params.var_variant_min_values
                            ..=params.var_variant_max_values,
                    ),
                    params.min_variants..=params.max_variants,
                )
                .prop_map(|v| {
                    let variants = to_enumeration_values(v);
                    let mut offsets = vec![0];

                    // NB: the final variant length is inferred from total data length,
                    // so we skip pushing it onto offsets
                    variants.iter().take(variants.len() - 1).for_each(
                        |value| {
                            offsets.push(offsets.last().unwrap() + value.len())
                        },
                    );

                    let data = to_raw(
                        variants.into_iter().flatten().collect::<Vec<DT>>(),
                    );
                    let offsets = offsets
                        .into_iter()
                        .map(|o| (o * std::mem::size_of::<DT>()) as u64)
                        .collect::<Vec<u64>>();

                    (data, Some(offsets.into_boxed_slice()))
                })
                .boxed()
            }
        }
    )
}

pub fn prop_enumeration_for_datatype(
    datatype: Datatype,
    cell_val_num: CellValNum,
    params: Parameters,
) -> impl Strategy<Value = EnumerationData> {
    let name = prop_enumeration_name();
    let ordered = prop_ordered();
    let data = prop_enumeration_values(datatype, cell_val_num, &params);
    (name, ordered, data)
        .prop_map(move |(name, ordered, (data, offsets))| EnumerationData {
            name,
            datatype,
            ordered: Some(ordered),
            cell_val_num: Some(cell_val_num),
            data,
            offsets,
        })
        .boxed()
}

#[derive(Clone)]
pub struct Parameters {
    pub datatype: BoxedStrategy<Datatype>,
    pub cell_val_num: BoxedStrategy<CellValNum>,
    pub min_variants: usize,
    pub max_variants: usize,
    pub var_variant_min_values: usize,
    pub var_variant_max_values: usize,
}

impl Parameters {
    fn min_variants_default() -> usize {
        **tiledb_proptest_config::TILEDB_STRATEGY_ENUMERATION_PARAMETERS_NUM_VARIANTS_MIN
    }

    fn max_variants_default() -> usize {
        **tiledb_proptest_config::TILEDB_STRATEGY_ENUMERATION_PARAMETERS_NUM_VARIANTS_MAX
    }

    fn var_variant_min_values_default() -> usize {
        **tiledb_proptest_config::TILEDB_STRATEGY_ENUMERATION_PARAMETERS_VAR_VARIANT_NUM_VALUES_MIN
    }

    fn var_variant_max_values_default() -> usize {
        **tiledb_proptest_config::TILEDB_STRATEGY_ENUMERATION_PARAMETERS_VAR_VARIANT_NUM_VALUES_MAX
    }
}

impl Default for Parameters {
    fn default() -> Self {
        Parameters {
            datatype: any::<Datatype>().boxed(),
            cell_val_num: prop_oneof![
                Just(CellValNum::single()),
                Just(CellValNum::Var)
            ]
            .boxed(),
            min_variants: Self::min_variants_default(),
            max_variants: Self::max_variants_default(),
            var_variant_min_values: Self::var_variant_min_values_default(),
            var_variant_max_values: Self::var_variant_max_values_default(),
        }
    }
}

type EnumerationStrategy = strategy_ext::meta::MapValueTree<
    BoxedStrategy<EnumerationData>,
    EnumerationValueTree,
>;

impl Arbitrary for EnumerationData {
    type Parameters = Parameters;
    type Strategy = EnumerationStrategy;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        (params.datatype.clone(), params.cell_val_num.clone())
            .prop_flat_map(move |(dt, cvn)| {
                prop_enumeration_for_datatype(dt, cvn, params.clone())
            })
            .boxed()
            .value_tree_map(|vt| EnumerationValueTree::new(vt.current()))
    }
}

#[derive(Clone, Debug)]
pub struct EnumerationValueTree {
    name: String,
    datatype: Datatype,
    cell_val_num: CellValNum,
    ordered: Option<bool>,
    variants: RecordsValueTree<Vec<Vec<u8>>>,
}

impl EnumerationValueTree {
    pub fn new(enumeration: EnumerationData) -> Self {
        let variants = RecordsValueTree::new(1, enumeration.records());

        EnumerationValueTree {
            name: enumeration.name,
            datatype: enumeration.datatype,
            cell_val_num: enumeration
                .cell_val_num
                .unwrap_or(CellValNum::single()),
            ordered: enumeration.ordered,
            variants,
        }
    }
}

impl ValueTree for EnumerationValueTree {
    type Value = EnumerationData;

    fn current(&self) -> Self::Value {
        let variants = self.variants.current();
        let (data, offsets) =
            super::variants_from_records(self.cell_val_num, variants);

        EnumerationData {
            name: self.name.clone(),
            datatype: self.datatype,
            cell_val_num: Some(self.cell_val_num),
            ordered: self.ordered,
            data,
            offsets,
        }
    }

    fn simplify(&mut self) -> bool {
        self.variants.simplify()
    }

    fn complicate(&mut self) -> bool {
        self.variants.complicate()
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use strategy_ext::meta::{ShrinkAction, ShrinkSequenceStrategy};

    use super::*;

    fn do_search_integrity(
        mut vt: EnumerationValueTree,
        search: Vec<ShrinkAction>,
    ) {
        for action in search {
            if !action.apply(&mut vt) {
                break;
            }
        }
    }

    proptest! {
        #[test]
        fn search_integrity(vt in any::<EnumerationData>().prop_indirect(), search in ShrinkSequenceStrategy::default()) {
            do_search_integrity(vt, search)
        }
    }
}
