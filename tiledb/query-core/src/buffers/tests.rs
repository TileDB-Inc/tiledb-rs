use arrow_proptest_strategies::array::{prop_array, ArrayParameters};
use arrow_proptest_strategies::schema::prop_arrow_field;
use proptest::prelude::*;
use tiledb_common::Datatype;

use super::*;

fn copy_buffer(buffer: &abuf::Buffer) -> abuf::Buffer {
    abuf::Buffer::from(buffer.as_slice().to_vec())
}

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
