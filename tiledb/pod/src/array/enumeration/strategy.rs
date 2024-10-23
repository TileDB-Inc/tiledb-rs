use std::cmp::Ordering;

use proptest::collection::vec;
use proptest::prelude::*;
use tiledb_common::array::CellValNum;
use tiledb_common::datatype::physical::{BitsEq, BitsOrd};
use tiledb_common::{physical_type_go, Datatype};

use crate::array::enumeration::EnumerationData;

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

fn prop_enumeration_values(
    datatype: Datatype,
    cell_val_num: CellValNum,
    min_variants: usize,
    max_variants: usize,
) -> BoxedStrategy<(Box<[u8]>, Option<Box<[u64]>>)> {
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
                vec(any::<DT>(), min_variants..=max_variants)
                    .prop_map(|v| (to_raw(to_enumeration_values(v)), None))
                    .boxed()
            }
            CellValNum::Fixed(nz) => {
                vec(
                    vec(any::<DT>(), nz.get() as usize),
                    min_variants..=max_variants,
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
                vec(vec(any::<DT>(), 0..=64), min_variants..=max_variants)
                    .prop_map(|v| {
                        let variants = to_enumeration_values(v);
                        let mut offsets = vec![0];

                        // NB: the final variant length is inferred from total data length,
                        // so we skip pushing it onto offsets
                        variants.iter().take(variants.len() - 1).for_each(
                            |value| {
                                offsets
                                    .push(offsets.last().unwrap() + value.len())
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
    min_variants: usize,
    max_variants: usize,
) -> impl Strategy<Value = EnumerationData> {
    let name = prop_enumeration_name();
    let ordered = prop_ordered();
    let data = prop_enumeration_values(
        datatype,
        cell_val_num,
        min_variants,
        max_variants,
    );
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

pub struct Parameters {
    pub datatype: BoxedStrategy<Datatype>,
    pub cell_val_num: BoxedStrategy<CellValNum>,
    pub min_variants: usize,
    pub max_variants: usize,
}

impl Parameters {
    fn min_variants_default() -> usize {
        **tiledb_proptest_config::TILEDB_STRATEGY_ENUMERATION_PARAMETERS_NUM_VARIANTS_MIN
    }

    fn max_variants_default() -> usize {
        **tiledb_proptest_config::TILEDB_STRATEGY_ENUMERATION_PARAMETERS_NUM_VARIANTS_MAX
    }
}

impl Default for Parameters {
    fn default() -> Self {
        Parameters {
            datatype: any::<Datatype>().boxed(),
            cell_val_num: any::<CellValNum>().boxed(),
            min_variants: Self::min_variants_default(),
            max_variants: Self::max_variants_default(),
        }
    }
}

impl Arbitrary for EnumerationData {
    type Parameters = Parameters;
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        (params.datatype, params.cell_val_num)
            .prop_flat_map(move |(dt, cvn)| {
                prop_enumeration_for_datatype(
                    dt,
                    cvn,
                    params.min_variants,
                    params.max_variants,
                )
            })
            .boxed()
    }
}
