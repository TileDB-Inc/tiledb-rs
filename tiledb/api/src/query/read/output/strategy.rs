use std::fmt::Debug;
use std::num::NonZeroU32;

use proptest::prelude::*;

use crate::array::CellValNum;
use crate::query::buffer::{CellStructure, QueryBuffers};
use crate::query::read::output::{RawReadOutput, TypedRawReadOutput};
use crate::{fn_typed, Datatype};

#[derive(Clone, Debug)]
pub struct RawReadOutputParameters {
    pub cell_val_num: Option<CellValNum>,
    pub is_nullable: Option<bool>,
    pub min_values_capacity: usize,
    pub max_values_capacity: usize,
    pub min_offset_capacity: usize,
    pub max_offset_capacity: usize,
    pub min_validity_capacity: usize,
    pub max_validity_capacity: usize,
}

impl Default for RawReadOutputParameters {
    fn default() -> Self {
        const MIN_VALUES_CAPACITY: usize = 0;
        const MAX_VALUES_CAPACITY: usize = 1024;
        const MIN_OFFSET_CAPACITY: usize = 0;
        const MAX_OFFSET_CAPACITY: usize = 128;
        const MIN_VALIDITY_CAPACITY: usize = 0;
        const MAX_VALIDITY_CAPACITY: usize = 128;

        RawReadOutputParameters {
            cell_val_num: None,
            is_nullable: None,
            min_values_capacity: MIN_VALUES_CAPACITY,
            max_values_capacity: MAX_VALUES_CAPACITY,
            min_offset_capacity: MIN_OFFSET_CAPACITY,
            max_offset_capacity: MAX_OFFSET_CAPACITY,
            min_validity_capacity: MIN_VALIDITY_CAPACITY,
            max_validity_capacity: MAX_VALIDITY_CAPACITY,
        }
    }
}

/// Produces an arbitrary raw read output.
/// Buffer capacities are not correlated with each other,
/// but `nvalues` and `nbytes` are valid for each of the buffers.
/// Unused capacity for all buffers is essentially random.
/// Cell offsets are sorted and valid indices into the `data` buffer.
/// Validity is random - null values have no guaranteed representation
/// (or length, in the case of cell offsets).
impl<'data, C> Arbitrary for RawReadOutput<'data, C>
where
    C: Arbitrary + Clone + Debug + 'static,
{
    type Parameters = RawReadOutputParameters;
    type Strategy = BoxedStrategy<RawReadOutput<'data, C>>;

    fn arbitrary_with(p: Self::Parameters) -> Self::Strategy {
        /* strategy to generate initial data buffer */
        let strategy_data_buffer = proptest::collection::vec(
            any::<C>(),
            p.min_values_capacity..=p.max_values_capacity,
        );

        /* strategy to choose cell offsets (the offsets themselves will be generated later) */
        let strategy_cell_offsets_capacity = {
            let strategy_capacity =
                (p.min_offset_capacity..=p.max_offset_capacity).prop_map(Ok);
            let strategy_fixed_cvn = (1..i32::MAX)
                .prop_map(|nz| Err(NonZeroU32::new(nz as u32).unwrap()));

            if let Some(CellValNum::Var) = p.cell_val_num {
                strategy_capacity.boxed()
            } else if let Some(CellValNum::Fixed(nz)) = p.cell_val_num {
                Just(Err(nz)).boxed()
            } else {
                prop_oneof![
                    strategy_fixed_cvn.boxed(),
                    strategy_capacity.boxed()
                ]
                .boxed()
            }
        };

        /* strategy to choose validity buffer (the validity values themselves will be set later) */
        let strategy_validity_buffer = {
            let strategy_buffer = proptest::collection::vec(
                prop_oneof![Just(0), any::<u8>()],
                p.min_validity_capacity..=p.max_validity_capacity,
            )
            .prop_map(Some);

            if let Some(n) = p.is_nullable {
                if n {
                    strategy_buffer.boxed()
                } else {
                    Just(None).boxed()
                }
            } else {
                prop_oneof![Just(None).boxed(), strategy_buffer.boxed()].boxed()
            }
        };

        (
            strategy_data_buffer,
            strategy_cell_offsets_capacity,
            strategy_validity_buffer,
        )
            .prop_flat_map(|(data, offsets_capacity, validity)| {
                prop_raw_read_output_for(data, offsets_capacity, validity)
            })
            .boxed()
    }
}

impl<'data> Arbitrary for TypedRawReadOutput<'data> {
    type Parameters = Option<(Datatype, RawReadOutputParameters)>;
    type Strategy = BoxedStrategy<TypedRawReadOutput<'data>>;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        let strategy_datatype =
            if let Some(datatype) = params.as_ref().map(|p| p.0) {
                Just(datatype).boxed()
            } else {
                any::<Datatype>().boxed()
            };

        strategy_datatype
            .prop_flat_map(move |datatype| {
                fn_typed!(
                    datatype,
                    DT,
                    any_with::<RawReadOutput<DT>>(
                        params
                            .as_ref()
                            .cloned()
                            .map(|p| p.1)
                            .unwrap_or(Default::default())
                    )
                    .prop_map(move |rr| TypedRawReadOutput::new(datatype, rr))
                    .boxed()
                )
            })
            .boxed()
    }
}

fn prop_raw_read_output_for<'data, C>(
    data: Vec<C>,
    offsets_capacity: Result<usize, NonZeroU32>,
    validity: Option<Vec<u8>>,
) -> BoxedStrategy<RawReadOutput<'data, C>>
where
    C: Clone + Debug + 'static,
{
    match offsets_capacity {
        Ok(o) => prop_raw_read_output_with_cell_offsets::<'data, C>(
            data, o, validity,
        ),
        Err(nz) => prop_raw_read_output_without_cell_offsets::<'data, C>(
            data, validity, nz,
        ),
    }
}

fn prop_raw_read_output_with_cell_offsets<'data, C>(
    data: Vec<C>,
    offsets_capacity: usize,
    validity: Option<Vec<u8>>,
) -> BoxedStrategy<RawReadOutput<'data, C>>
where
    C: Clone + Debug + 'static,
{
    let max_values = data.len();
    let max_offsets = if let Some(v) = validity.as_ref() {
        std::cmp::min(offsets_capacity, v.len())
    } else {
        offsets_capacity
    };
    (
        0..=max_values,
        Just(max_offsets),
        Just(data),
        Just(validity),
    )
        .prop_flat_map(|(nvalues, max_offsets, data, validity)| {
            (
                Just(data),
                proptest::collection::vec(
                    0u64..=(nvalues as u64),
                    0..=max_offsets,
                ),
                Just(validity),
            )
                .prop_map(|(data, mut offsets, mut validity)| {
                    validity.iter_mut().for_each(|v| {
                        v.iter_mut().take(offsets.len()).for_each(
                            |v: &mut u8| {
                                if *v != 0 {
                                    *v = 1
                                }
                            },
                        )
                    });

                    offsets = offsets
                        .into_iter()
                        .map(|o| o * std::mem::size_of::<C>() as u64)
                        .collect::<Vec<u64>>();
                    offsets.sort();
                    RawReadOutput {
                        nvalues: offsets.len(),
                        nbytes: data.len() * std::mem::size_of::<C>(),
                        input: QueryBuffers {
                            data: data.into(),
                            cell_structure: CellStructure::Var(offsets.into()),
                            validity: validity.map(|v| v.into()),
                        },
                    }
                })
        })
        .boxed()
}

fn prop_raw_read_output_without_cell_offsets<'data, C>(
    data: Vec<C>,
    validity: Option<Vec<u8>>,
    cell_val_num: NonZeroU32,
) -> BoxedStrategy<RawReadOutput<'data, C>>
where
    C: Clone + Debug + 'static,
{
    let max_cells = {
        let data_bound = data.len() / cell_val_num.get() as usize;
        if let Some(v) = validity.as_ref() {
            let validity_bound = v.len();
            std::cmp::min(data_bound, validity_bound)
        } else {
            data_bound
        }
    };

    (0..=max_cells, Just(data), Just(validity))
        .prop_map(move |(ncells, mut data, mut validity)| {
            data.truncate(ncells * ncells);

            validity.iter_mut().for_each(|v| {
                v.iter_mut().take(ncells).for_each(|v: &mut u8| {
                    if *v != 0 {
                        *v = 1
                    }
                })
            });

            let nvalues = ncells * cell_val_num.get() as usize;
            let nbytes = nvalues * std::mem::size_of::<C>();
            RawReadOutput {
                nvalues,
                nbytes,
                input: QueryBuffers {
                    data: data.into(),
                    cell_structure: CellStructure::Fixed(cell_val_num),
                    validity: validity.map(|v| v.into()),
                },
            }
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::proptest;

    fn arbitrary_raw_read_handle<C>(rr: RawReadOutput<C>) {
        match rr.input.cell_structure {
            CellStructure::Var(offsets) => {
                assert!(
                    rr.nbytes <= std::mem::size_of_val(rr.input.data.as_ref()),
                    "nbytes = {}, data.bytelen() = {}",
                    rr.nbytes,
                    std::mem::size_of_val(rr.input.data.as_ref())
                );

                let offsets = offsets.as_ref().to_vec();
                assert!(
                    rr.nvalues <= offsets.len(),
                    "nvalues = {}, offsets.len() = {}",
                    rr.nvalues,
                    offsets.len()
                );

                // offsets are in bytes and are sorted
                for w in offsets.windows(2) {
                    assert!(w[0] <= w[1]);

                    let delta = w[1] - w[0];
                    assert_eq!(0, delta % std::mem::size_of::<C>() as u64);
                }

                // offsets must be valid into `data`
                if rr.nvalues > 0 {
                    let last_offset = offsets[rr.nvalues - 1];
                    let byte_bound =
                        std::mem::size_of_val(rr.input.data.as_ref()) as u64;
                    assert!(
                        last_offset <= rr.nbytes as u64,
                        "last_offset = {}, nbytes = {}",
                        last_offset,
                        rr.nbytes
                    );
                    assert!(
                        last_offset <= byte_bound,
                        "last_offset = {}, byte_bound = {}",
                        last_offset,
                        byte_bound
                    );
                }
            }
            CellStructure::Fixed(nz) => {
                assert!(
                    rr.nvalues <= rr.input.data.len(),
                    "nvalues = {}, data.len() = {}",
                    rr.nvalues,
                    rr.input.data.len()
                );

                let cvn = nz.get() as usize;

                assert_eq!(0, rr.nvalues % cvn);

                if let Some(validity) = rr.input.validity {
                    let ncells = rr.nvalues / cvn;
                    /* TODO: this is why we want rr.ncells instead of nvalues, multiplication
                     * is much more comfortable than division */

                    let validity = validity.as_ref().to_vec();
                    assert!(
                        ncells <= validity.len(),
                        "nvalues = {}, validity.len() = {}",
                        rr.nvalues,
                        validity.len()
                    );

                    assert_eq!(0, rr.nvalues % nz.get() as usize);

                    for v in validity[0..ncells].iter() {
                        assert!(*v == 0 || *v == 1);
                    }
                }
            }
        }
    }

    proptest! {
        #[test]
        fn arbitrary_raw_read_handle_u8(rr in any::<RawReadOutput<u8>>()) {
            arbitrary_raw_read_handle::<u8>(rr);
        }

        #[test]
        fn arbitrary_raw_read_handle_u16(rr in any::<RawReadOutput<u16>>()) {
            arbitrary_raw_read_handle::<u16>(rr);
        }

        #[test]
        fn arbitrary_raw_read_handle_u32(rr in any::<RawReadOutput<u32>>()) {
            arbitrary_raw_read_handle::<u32>(rr);
        }

        #[test]
        fn arbitrary_raw_read_handle_u64(rr in any::<RawReadOutput<u64>>()) {
            arbitrary_raw_read_handle::<u64>(rr);
        }

        #[test]
        fn arbitrary_raw_read_handle_f32(rr in any::<RawReadOutput<f32>>()) {
            arbitrary_raw_read_handle::<f32>(rr);
        }

        #[test]
        fn arbitrary_raw_read_handle_f64(rr in any::<RawReadOutput<f64>>()) {
            arbitrary_raw_read_handle::<f64>(rr);
        }
    }
}
