use arrow_proptest_strategies::array::{prop_array, ArrayParameters};
use arrow_proptest_strategies::schema::{
    prop_arrow_datatype, prop_arrow_field,
};
use proptest::prelude::*;
use tiledb_common::Datatype;

use super::*;

impl Arbitrary for BufferTarget {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        let query_type =
            prop_oneof![Just(QueryType::Read), Just(QueryType::Write)];
        (query_type, any::<CellValNum>(), any::<bool>())
            .prop_map(|(query_type, cell_val_num, is_nullable)| Self {
                query_type,
                cell_val_num,
                is_nullable,
            })
            .boxed()
    }
}

/// Returns a deep copy of `buffer`.
fn copy_buffer(buffer: &abuf::Buffer) -> abuf::Buffer {
    abuf::Buffer::from(buffer.as_slice().to_vec())
}

/// Returns a deep copy of `array_data`.
///
/// The returned [ArrayData] does not share any buffers with `array_data`.
fn copy_array_data(array_data: &aa::ArrayData) -> aa::ArrayData {
    let nulls = array_data
        .nulls()
        .map(|n| abuf::Buffer::from(n.validity().to_vec()));
    let buffers = array_data
        .buffers()
        .iter()
        .map(copy_buffer)
        .collect::<Vec<_>>();
    let child_data = array_data
        .child_data()
        .iter()
        .map(copy_array_data)
        .collect::<Vec<_>>();

    aa::ArrayData::try_new(
        array_data.data_type().clone(),
        array_data.len(),
        nulls,
        array_data.offset(),
        buffers,
        child_data,
    )
    .expect("Error copying array data")
}

/// Returns a deep copy of `array`.
///
/// The returned [Array] does not share any buffers with `array`.
fn copy_array(array: &dyn aa::Array) -> Arc<dyn aa::Array> {
    let data_ref = array.to_data();
    let data_copy = copy_array_data(&data_ref);

    aa::make_array(data_copy)
}

fn instance_copy_array(array_in: &dyn aa::Array) {
    let array_out = copy_array(array_in);
    assert_eq!(array_in, array_out.as_ref());
}

proptest! {
    #[test]
    fn proptest_copy_array(
        array in prop_arrow_field(Default::default())
            .prop_flat_map(|field| prop_array(Default::default(), Arc::new(field)))
    ) {
        instance_copy_array(&array)
    }
}

fn instance_list_buffers_roundtrip_var(array_in: aa::LargeListArray) {
    let target = BufferTarget {
        query_type: QueryType::Write,
        cell_val_num: CellValNum::Var,
        is_nullable: true,
    };
    let lb = Box::new({
        let array_in =
            downcast_consume::<aa::LargeListArray>(copy_array(&array_in));
        ListBuffers::try_new(&target, array_in).unwrap()
    });
    let array_out = match lb.into_arrow() {
        Ok(array) => array,
        Err((_, e)) => panic!(
            "For array of type {}, unexpected error in `into_arrow`: {}",
            array_in.data_type(),
            e
        ),
    };

    assert_eq!(&array_in as &dyn Array, array_out.as_ref());
}

fn instance_list_buffers_roundtrip_fixed(
    cell_val_num: CellValNum,
    array_in: aa::LargeListArray,
) {
    let target = BufferTarget {
        query_type: QueryType::Write,
        cell_val_num,
        is_nullable: true,
    };

    let lb = Box::new({
        let array_in =
            downcast_consume::<aa::LargeListArray>(copy_array(&array_in));
        ListBuffers::try_new(&target, array_in).unwrap()
    });
    let array_out = match lb.into_arrow() {
        Ok(array) => array,
        Err((_, e)) => panic!("Unexpected error in `into_arrow`: {}", e),
    };

    assert_eq!(&array_in as &dyn Array, array_out.as_ref());
}

fn strat_list_buffers_roundtrip_var(
) -> impl Strategy<Value = aa::LargeListArray> {
    any::<Datatype>()
        .prop_filter("Boolean in list needs special handling", |dt| {
            *dt != Datatype::Boolean
        })
        .prop_map(|dt| {
            crate::datatype::default_arrow_type(dt, CellValNum::single())
                .unwrap()
                .into_inner()
        })
        .prop_flat_map(|adt| {
            let field = adt::Field::new(
                "unused",
                adt::DataType::LargeList(Arc::new(adt::Field::new_list_field(
                    adt, false,
                ))),
                true,
            );
            arrow_proptest_strategies::prop_array(
                Default::default(),
                Arc::new(field),
            )
        })
        .prop_map(|array| downcast_consume::<aa::LargeListArray>(array))
}

fn strat_list_buffers_roundtrip_fixed(
) -> impl Strategy<Value = (CellValNum, aa::LargeListArray)> {
    (any::<Datatype>(), 1..=256i32)
        .prop_filter(
            "FIXME: Boolean in list needs special handling",
            |(dt, _)| *dt != Datatype::Boolean,
        )
        .prop_map(|(dt, fl)| {
            (
                fl,
                crate::datatype::default_arrow_type(dt, CellValNum::single())
                    .unwrap()
                    .into_inner(),
            )
        })
        .prop_flat_map(|(fl, adt)| {
            let params = ArrayParameters {
                num_collection_elements: (fl as usize).into(),
                ..Default::default()
            };
            let field = adt::Field::new(
                "unused",
                adt::DataType::FixedSizeList(
                    Arc::new(adt::Field::new_list_field(adt, false)),
                    fl,
                ),
                true,
            );
            prop_array(params, Arc::new(field))
        })
        .prop_map(|array| {
            let num_lists = array.len();
            let fl = downcast_consume::<aa::FixedSizeListArray>(dbg!(array));
            let (field, fl, values, nulls) = fl.into_parts();
            let array = aa::LargeListArray::try_new(
                field,
                abuf::OffsetBuffer::<i64>::from_lengths(vec![
                    fl as usize;
                    num_lists
                ]),
                values,
                nulls,
            )
            .unwrap();
            (CellValNum::try_from(fl as u32).unwrap(), array)
        })
}

proptest! {
    #[test]
    fn proptest_list_buffers_roundtrip_var(array in strat_list_buffers_roundtrip_var()) {
        instance_list_buffers_roundtrip_var(array)
    }

    #[test]
    fn proptest_list_buffers_roundtrip_fixed((cvn, array) in strat_list_buffers_roundtrip_fixed()) {
        instance_list_buffers_roundtrip_fixed(cvn, array)
    }
}

/// Test that if a data type can be used to alloc an array then it also
/// can be converted to a mutable buffer
fn instance_make_mut(
    target_type: adt::DataType,
    capacity: Capacity,
    target: BufferTarget,
) {
    let Ok(array) = alloc_array(target_type, target.is_nullable, capacity)
    else {
        return;
    };

    let array_expect = copy_array(&array);

    let entry_mut = to_target_buffers(&target, array)
        .expect("alloc_array succeeded but to_target_buffers failed");

    let array_out = entry_mut
        .into_arrow()
        .expect("to_target_buffers succeeded but into_array failed");

    assert_eq!(array_expect.as_ref(), array_out.as_ref());
}

proptest! {
    #[test]
    fn proptest_make_mut(
        target_type in prop_arrow_datatype(Default::default()),
        capacity in any::<Capacity>(),
        target in any::<BufferTarget>()
    ) {
        instance_make_mut(target_type, capacity, target)
    }
}
