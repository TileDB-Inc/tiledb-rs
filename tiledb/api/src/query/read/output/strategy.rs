use std::fmt::Debug;
use std::num::NonZeroU32;

use proptest::prelude::*;

use crate::array::CellValNum;
use crate::datatype::LogicalType;
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
/// but `ncells` is valid for each of the buffers.
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
                    LT,
                    any_with::<RawReadOutput<<LT as LogicalType>::PhysicalType>>(
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
                .prop_map(
                    move |(data, mut offsets, mut validity)| {
                        /*
                         * What does "empty" look like for arrow-shaped offsets?
                         * Is it an empty offset buffer?
                         * Or is it a single offset `[0]`?
                         * The arrow `OffsetBuffer` doc suggests the latter, but is the tiledb core
                         * implementation compliant with that?
                         * A glance at the source code suggests it is not, i.e. if the number
                         * of records is zero then the offsets will be empty.
                         * So we must be able to generate empty offsets here too, and
                         * upstream code must be ready.
                         */
                        let ncells = std::cmp::max(1, offsets.len()) - 1;

                        validity.iter_mut().for_each(|v| {
                            v.iter_mut().take(ncells).for_each(|v: &mut u8| {
                                if *v != 0 {
                                    *v = 1
                                }
                            })
                        });

                        offsets = offsets
                            .into_iter()
                            .take(ncells + 1)
                            .collect::<Vec<u64>>();
                        if let Some(first) = offsets.first_mut() {
                            *first = 0u64;
                        }
                        if let Some(last) = offsets.last_mut() {
                            *last = nvalues as u64;
                        }
                        offsets.sort();

                        RawReadOutput {
                            ncells,
                            input: QueryBuffers {
                                data: data.into(),
                                cell_structure: CellStructure::Var(
                                    offsets.into(),
                                ),
                                validity: validity.map(|v| v.into()),
                            },
                        }
                    },
                )
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
            data.truncate(ncells * cell_val_num.get() as usize);

            validity.iter_mut().for_each(|v| {
                v.iter_mut().take(ncells).for_each(|v: &mut u8| {
                    if *v != 0 {
                        *v = 1
                    }
                })
            });

            RawReadOutput {
                ncells,
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
                if rr.ncells == 0 {
                    // nothing to check really
                    return;
                }
                assert!(
                    rr.ncells < offsets.len(),
                    "ncells = {}, offsets.len() = {}",
                    rr.ncells,
                    offsets.len()
                );

                let value_bound = rr.input.data.len() as u64;

                // offset unit is elements, and offsets are sorted
                for w in offsets.windows(2).take(rr.ncells) {
                    assert!(w[0] <= w[1], "w[0] = {}, w[1] = {}", w[0], w[1]);
                    assert!(
                        w[1] <= value_bound,
                        "offset = {}, value_bound = {}",
                        w[1],
                        value_bound
                    );
                }

                // should be covered by the loop, but paranoia check
                let last_offset = offsets[rr.ncells];
                assert!(last_offset <= value_bound);

                if let Some(validity) = rr.input.validity {
                    for v in validity[0..rr.ncells].iter() {
                        assert!(*v == 0 || *v == 1);
                    }
                }
            }
            CellStructure::Fixed(nz) => {
                assert!(
                    rr.nvalues() <= rr.input.data.len(),
                    "ncells = {}, cell_val_num = {}, data.len() = {}",
                    rr.ncells,
                    nz.get(),
                    rr.input.data.len()
                );

                if let Some(validity) = rr.input.validity {
                    let validity = validity.as_ref().to_vec();
                    assert!(
                        rr.ncells <= validity.len(),
                        "ncells = {}, validity.len() = {}",
                        rr.ncells,
                        validity.len()
                    );

                    for v in validity[0..rr.ncells].iter() {
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
