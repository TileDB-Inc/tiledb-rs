use arrow::array::AsArray;
use proptest::prelude::*;
use tiledb_common::array::CellValNum;
use tiledb_common::Datatype;

use super::*;
use crate::buffers::alloc_array;

impl Arbitrary for Capacity {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        let min_memory_limit = std::mem::size_of::<u64>();
        prop_oneof![
            (1usize..=1024).prop_map(Capacity::Cells),
            (1usize..=(1024 * 16)).prop_map(Capacity::Values),
            (min_memory_limit..=(10 * 1024 * 1024)).prop_map(Capacity::Memory)
        ]
        .boxed()
    }
}

/// Instance of a capacity limits test.
fn instance_capacity_limits(
    capacity: Capacity,
    target_type: adt::DataType,
    nullable: bool,
) -> anyhow::Result<()> {
    let a1 = alloc_array(target_type.clone(), nullable, capacity)?;
    assert_eq!(nullable, a1.nulls().is_some());

    // NB: boolean requires special handling because it is bit-packed in arrow
    // but not in tiledb, so there is an extra buffer allocated to translate
    match target_type {
        adt::DataType::Boolean => {
            return instance_capacity_limits_boolean(capacity, nullable, a1)
        }
        adt::DataType::FixedSizeList(ref elt, fl)
            if matches!(elt.data_type(), adt::DataType::Boolean) =>
        {
            return instance_capacity_limits_boolean_fl(
                capacity,
                nullable,
                a1,
                fl as usize,
            )
        }
        adt::DataType::LargeList(ref elt)
            if matches!(elt.data_type(), adt::DataType::Boolean) =>
        {
            return instance_capacity_limits_boolean_vl(capacity, nullable, a1)
        }
        _ => {
            // not a supported boolean thing, fall through
        }
    }

    let (actual_num_cells, actual_num_values, cell_width, value_width) = {
        use arrow::array::Array;
        use arrow::datatypes as arrow_schema;

        let a1 = a1.as_ref();
        aa::downcast_primitive_array!(
            a1 => {
                let value_width = a1.data_type().primitive_width().unwrap();
                (a1.len(), a1.len(), Some(value_width), value_width)
            },
            adt::DataType::FixedSizeBinary(fl) => {
                (a1.len(), a1.as_fixed_size_binary().values().len(), Some(*fl as usize), 1)
            },
            adt::DataType::FixedSizeList(_, fl) => {
                let elt = a1.as_fixed_size_list().values();
                let value_size = elt.data_type().primitive_width().unwrap();
                (a1.len(), elt.len(), Some(*fl as usize * value_size), value_size)
            },
            adt::DataType::LargeUtf8 => {
                (a1.len(), a1.as_string::<i64>().values().len(), None, 1)
            },
            adt::DataType::LargeBinary => {
                (a1.len(), a1.as_binary::<i64>().values().len(), None, 1)
            },
            adt::DataType::LargeList(_) => {
                let elt = a1.as_list::<i64>().values();
                (a1.len(), elt.len(), None, elt.data_type().primitive_width().unwrap())
            },
            dt => unreachable!("Unexpected data type {} in downcast", dt)
        )
    };

    match capacity {
        Capacity::Cells(num_cells) => {
            assert_eq!(num_cells, actual_num_cells);

            // adding another cell should increase the number of values
            let num_values = capacity.num_values(&target_type, nullable)?;
            let next_values = Capacity::Cells(num_cells + 1)
                .num_values(&target_type, nullable)?;
            assert!(
                next_values > num_values,
                "num_cells = {:?}, num_values = {:?}, next_values = {:?}",
                num_cells,
                num_values,
                next_values
            );
        }
        Capacity::Values(num_values) => {
            let a1 = a1.as_ref();

            if let Some(cell_width) = cell_width {
                // we should hold the largest amount of integral cells
                assert_eq!(0, cell_width % value_width);
                let num_values_per_cell = cell_width / value_width;
                assert_eq!(num_values_per_cell * a1.len(), actual_num_values);
                assert!(num_values_per_cell * a1.len() <= num_values);
                assert!(num_values_per_cell * (a1.len() + 1) > num_values);
            } else {
                // whichever buffer holds the values must be big enough
                assert_eq!(num_values, actual_num_values);
            }

            let num_cells = capacity.num_cells(&target_type, nullable)?;

            // there is a threshold over which adding values to the request
            // should increase the number of cells
            if let Some(est_value) =
                estimate_average_variable_length_values(&target_type)
            {
                for delta in 0..est_value {
                    let delta_num_cells = Capacity::Values(num_values + delta)
                        .num_cells(&target_type, nullable)?;
                    assert!(delta_num_cells >= num_cells);
                }
                let delta_cells = Capacity::Values(num_values + est_value)
                    .num_cells(&target_type, nullable)?;

                assert!(delta_cells > num_cells);
            } else if let adt::DataType::FixedSizeBinary(ref fl)
            | adt::DataType::FixedSizeList(_, ref fl) = target_type
            {
                let fl = *fl as usize;
                let delta_cells = Capacity::Values(num_values + fl)
                    .num_cells(&target_type, nullable)?;
                assert!(delta_cells > num_cells);
            } else {
                let delta_cells = Capacity::Values(num_values + 1)
                    .num_cells(&target_type, nullable)?;
                assert!(delta_cells > num_cells);
            }
        }
        Capacity::Memory(memory_limit) => {
            let null_translation_buffer_overhead =
                if nullable { a1.len() } else { 0 };

            let a1_memory = a1.get_buffer_memory_size();
            assert!(
                a1_memory + null_translation_buffer_overhead <= memory_limit,
                "a1_memory = {:?}, memory_limit = {:?}",
                a1_memory,
                memory_limit
            );

            let num_cells = capacity.num_cells(&target_type, nullable)?;

            // there should be no room for another full cell within the memory limit
            let a2 = alloc_array(
                target_type.clone(),
                nullable,
                Capacity::Cells(num_cells),
            )?;
            let a2_memory = a2.get_buffer_memory_size();

            let null_overhead = if nullable {
                num_cells + 1 + (num_cells + 8) / 8
            } else {
                0
            };

            if let Some(est_values) =
                estimate_average_variable_length_values(&target_type)
            {
                // the memory limit should have no room for another estimated cell
                let cell_overhead = std::mem::size_of::<i64>();
                assert!(
                    a1_memory
                        + value_width * est_values
                        + cell_overhead
                        + null_overhead
                        > memory_limit
                );
            } else {
                assert_eq!(a1_memory, a2_memory);

                let cell_width = cell_width.unwrap();

                // the memory limit should have no room for another cell
                assert!(
                    a1_memory + cell_width + null_overhead > memory_limit,
                    "memory_limit = {}, a1_memory = {}, cell_width = {}, null_overhead = {}",
                    memory_limit, a1_memory, cell_width, null_overhead
                );
            }
        }
    }

    Ok(())
}

/// Instance of a capacity limits test when the target type is Boolean.
///
/// Boolean data is bit packed in arrow but not in tiledb. When there is a memory
/// limit the buffer which unpacks the bits must be accounted for.
fn instance_capacity_limits_boolean(
    capacity: Capacity,
    nullable: bool,
    a1: Arc<dyn aa::Array>,
) -> anyhow::Result<()> {
    match capacity {
        Capacity::Cells(num_cells) => {
            assert_eq!(num_cells, a1.len());
            assert_eq!(
                a1.as_ref(),
                alloc_array(
                    adt::DataType::Boolean,
                    nullable,
                    Capacity::Values(num_cells)
                )?
                .as_ref()
            );
        }
        Capacity::Values(num_values) => {
            assert_eq!(num_values, a1.len());
            assert_eq!(
                a1.as_ref(),
                alloc_array(
                    adt::DataType::Boolean,
                    nullable,
                    Capacity::Cells(num_values)
                )?
                .as_ref()
            );
        }
        Capacity::Memory(memory_limit) => {
            let num_cells = a1.len();

            let a1_memory = a1.get_buffer_memory_size();
            let a1_translation_memory = a1.len();
            let a1_null_translation_memory =
                if nullable { a1.len() } else { 0 };

            let translation_bytes_per_cell = if nullable { 2 } else { 1 };

            // we may not always be able to use the full memory limit because
            // advancing to the next unpacked byte requires 2 (+1 if nullable)
            // additional bytes of memory
            assert!(
                memory_limit - translation_bytes_per_cell
                    <= a1_memory
                        + a1_translation_memory
                        + a1_null_translation_memory
            );
            assert!(
                a1_memory + a1_translation_memory + a1_null_translation_memory
                    <= memory_limit
            );

            // there should be no room for another full cell within the memory limit
            let a2 = alloc_array(
                adt::DataType::Boolean,
                nullable,
                Capacity::Cells(num_cells + 1),
            )?;
            let a2_memory = a2.get_buffer_memory_size();
            let a2_translation_memory = a2.len();
            let a2_null_translation_memory =
                if nullable { a2.len() } else { 0 };

            assert!(
                a2_memory + a2_translation_memory + a2_null_translation_memory
                    > memory_limit
            );

            if nullable {
                assert!(
                    a2_memory
                        + a2_translation_memory
                        + a2_null_translation_memory
                        > memory_limit
                )
                /*
                if num_cells % 4 == 0 {
                    // a1 saturated a byte, a2 needs an extra
                    assert!(a2_memory + a2_translation_memory > memory_limit)
                } else {
                    assert!(a2_memory + a2_translation_memory >= memory_limit);
                }
                    */
            } else {
                assert!(a2_memory + a2_translation_memory > memory_limit)
                /*
                if num_cells % 8 == 0 {
                    // a1 saturated a byte, a2 needs an extra
                    assert!(a2_memory + a2_translation_memory > memory_limit)
                } else {
                    assert!(a2_memory + a2_translation_memory >= memory_limit);
                }
                        */
            }
        }
    }
    Ok(())
}

fn instance_capacity_limits_boolean_fl(
    _capacity: Capacity,
    _nullable: bool,
    _a1: Arc<dyn aa::Array>,
    _fl: usize,
) -> anyhow::Result<()> {
    // FIXME: gonna do it later
    Ok(())
}

fn instance_capacity_limits_boolean_vl(
    _capacity: Capacity,
    _nullable: bool,
    _a1: Arc<dyn aa::Array>,
) -> anyhow::Result<()> {
    // FIXME: gonna do it later
    Ok(())
}

fn strat_capacity_limits(
) -> impl Strategy<Value = (Capacity, adt::DataType, bool)> {
    let strat_datatype = (any::<Datatype>(), any::<CellValNum>()).prop_map(
        |(dt, cell_val_num)| {
            crate::datatype::default_arrow_type(dt, cell_val_num)
                .unwrap()
                .into_inner()
        },
    );

    (any::<Capacity>(), strat_datatype, any::<bool>())
}

proptest! {
    #[test]
    fn proptest_capacity_limits(
        (capacity, target_type, nullable) in strat_capacity_limits()
    ) {
        instance_capacity_limits(capacity, target_type, nullable).unwrap()
    }
}
