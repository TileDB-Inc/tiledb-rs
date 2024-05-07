use std::sync::Arc;

use arrow::array::{
    Array as ArrowArray, FixedSizeBinaryArray, FixedSizeListArray,
    GenericListArray, LargeBinaryArray, PrimitiveArray,
};
use arrow::datatypes::Field;

use crate::array::CellValNum;
use crate::datatype::arrow::ArrowPrimitiveTypeNative;
use crate::query::buffer::arrow::{Celled, QueryBufferArrowArray};
use crate::query::buffer::TypedQueryBuffers;
use crate::query::read::output::{RawReadOutput, TypedRawReadOutput};
use crate::{typed_query_buffers_go, Datatype};

impl<C> TryFrom<RawReadOutput<'_, C>> for QueryBufferArrowArray<C>
where
    C: ArrowPrimitiveTypeNative,
    PrimitiveArray<<C as ArrowPrimitiveTypeNative>::ArrowPrimitiveType>:
        From<Vec<C>> + From<Vec<Option<C>>>,
{
    type Error = std::num::TryFromIntError;

    fn try_from(value: RawReadOutput<C>) -> Result<Self, Self::Error> {
        Self::try_from(Celled(value.ncells, value.input))
    }
}

impl TryFrom<TypedRawReadOutput<'_>> for Arc<dyn ArrowArray> {
    type Error = std::num::TryFromIntError;

    fn try_from(value: TypedRawReadOutput<'_>) -> Result<Self, Self::Error> {
        /*
        fn assign_logical_type<PT, LT, I, O>(p: I) -> O where
            PT: ArrowPrimitiveTypeNative,
            LT: LogicalType<PhysicalType = PT> + ArrowPrimitiveTypeLogical,
            I: PrimitiveArray<<<LT as LogicalType>::PhysicalType as ArrowPrimitiveTypeNative>::ArrowPrimitiveType>,
            O: PrimitiveArray<<LT as ArrowPrimitiveTypeLogical>::ArrowPrimitiveType>
        {
            unimplemented!()
        }
        */

        fn assign_logical_type<C>(
            datatype: Datatype,
            p: PrimitiveArray<
                <C as ArrowPrimitiveTypeNative>::ArrowPrimitiveType,
            >,
        ) -> Arc<dyn ArrowArray>
        where
            C: ArrowPrimitiveTypeNative,
        {
            let arrow_datatype = crate::datatype::arrow::to_arrow(
                &datatype,
                CellValNum::single(),
            )
            .into_inner();
            arrow::array::make_array(
                p.into_data()
                    .into_builder()
                    .data_type(arrow_datatype)
                    .build()
                    .unwrap(),
            )
        }

        typed_query_buffers_go!(value.buffers, DT, input, {
            let array =
                QueryBufferArrowArray::try_from(Celled(value.ncells, input))?;

            match array {
                QueryBufferArrowArray::Primitive(p) => {
                    // the array data type has the phyiscal type,
                    // we need it to have the logical type
                    Ok(assign_logical_type::<DT>(value.datatype, p))
                }
                QueryBufferArrowArray::FixedSizeList(a) => {
                    let (fl, p) = a.unwrap();
                    let (len, nulls) = {
                        let (_, len, _, nulls) = fl.into_parts();
                        (len, nulls)
                    };
                    let p = assign_logical_type::<DT>(
                        value.datatype,
                        Arc::into_inner(p).unwrap(),
                    );
                    let field = Arc::new(Field::new_list_field(
                        p.data_type().clone(),
                        false,
                    ));

                    let fl = FixedSizeListArray::new(field, len, p, nulls);
                    match value.datatype {
                        Datatype::Blob => {
                            Ok(Arc::new(FixedSizeBinaryArray::from(fl)))
                        }
                        _ => Ok(Arc::new(fl)),
                    }
                }
                QueryBufferArrowArray::VarSizeList(a) => {
                    let (gl, p) = a.unwrap();
                    let (offsets, nulls) = {
                        let (_, offsets, _, nulls) = gl.into_parts();
                        (offsets, nulls)
                    };
                    let p = assign_logical_type::<DT>(
                        value.datatype,
                        Arc::into_inner(p).unwrap(),
                    );
                    let field = Arc::new(Field::new_list_field(
                        p.data_type().clone(),
                        false,
                    ));
                    let gl =
                        GenericListArray::<i64>::new(field, offsets, p, nulls);

                    match value.datatype {
                        Datatype::Blob => {
                            Ok(Arc::new(LargeBinaryArray::from(gl)))
                        }
                        _ => Ok(Arc::new(gl)),
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::any::TypeId;
    use std::num::NonZeroU32;

    use super::*;
    use arrow::datatypes::Field;
    use arrow::record_batch::RecordBatch;
    use proptest::prelude::*;

    use crate::array::CellValNum;
    use crate::query::buffer::QueryBuffers;
    use crate::query::read::output::strategy::RawReadOutputParameters;
    use crate::query::read::output::CellStructure;
    use crate::Datatype;

    fn raw_read_to_arrow<C>(rr: RawReadOutput<C>)
    where
        C: ArrowPrimitiveTypeNative,
        PrimitiveArray<<C as ArrowPrimitiveTypeNative>::ArrowPrimitiveType>:
            From<Vec<C>> + From<Vec<Option<C>>>,
    {
        type MyPrimitiveArray<C> =
            PrimitiveArray<<C as ArrowPrimitiveTypeNative>::ArrowPrimitiveType>;

        let arrow: Arc<dyn ArrowArray> = {
            let rrborrow = RawReadOutput {
                ncells: rr.ncells,
                input: rr.input.borrow(),
            };
            Arc::from(
                QueryBufferArrowArray::<C>::try_from(rrborrow)
                    .expect("Integer overflow")
                    .boxed(),
            )
        };

        match rr.input.cell_structure {
            CellStructure::Fixed(nz) if nz.get() == 1 => {
                assert_eq!(
                    TypeId::of::<MyPrimitiveArray<C>>(),
                    arrow.as_any().type_id()
                );
                let primitive = arrow
                    .as_any()
                    .downcast_ref::<MyPrimitiveArray<C>>()
                    .unwrap();

                assert_eq!(rr.ncells, primitive.len());

                /* ensure that neither or both has validity values */
                if rr.input.validity.is_some() {
                    assert!(primitive.nulls().is_some());
                    assert_eq!(rr.ncells, primitive.nulls().unwrap().len());
                } else {
                    assert_eq!(None, primitive.nulls());
                }

                /* check validity and data in stride */
                for i in 0..primitive.len() {
                    if let Some(v) = rr.input.validity.as_ref().map(|v| v[i]) {
                        assert_eq!(
                            Some(v == 0),
                            primitive.nulls().map(|n| n.is_null(i))
                        );
                    } else {
                        assert_eq!(rr.input.data[i], primitive.value(i));
                    }
                }
            }
            CellStructure::Fixed(nz) => {
                assert_eq!(
                    TypeId::of::<FixedSizeListArray>(),
                    arrow.as_any().type_id()
                );
                let fl = arrow
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .unwrap();

                if let arrow::datatypes::DataType::FixedSizeList(_, flen) =
                    fl.data_type()
                {
                    assert_eq!(*flen, nz.get() as i32);
                } else {
                    unreachable!(
                        "Expected FixedSizeList(_, {}) but found {}",
                        nz.get(),
                        fl.data_type()
                    )
                }

                /* check that the values match */
                let primitive = {
                    assert_eq!(
                        TypeId::of::<MyPrimitiveArray<C>>(),
                        fl.values().as_any().type_id()
                    );
                    fl.values()
                        .as_any()
                        .downcast_ref::<MyPrimitiveArray<C>>()
                        .unwrap()
                };
                assert_eq!(None, primitive.nulls());

                assert_eq!(rr.ncells * nz.get() as usize, primitive.len());
                assert_eq!(
                    rr.nbytes(),
                    primitive.len() * std::mem::size_of::<C>()
                );

                /* check that validity matches */
                if let Some(validity) = rr.input.validity {
                    assert!(fl.nulls().is_some());
                    let arrow_nulls = fl.nulls().unwrap();
                    assert_eq!(rr.ncells, arrow_nulls.len());

                    let arrow_nulls = arrow_nulls
                        .iter()
                        .map(|b| if b { 1 } else { 0 })
                        .collect::<Vec<u8>>();
                    assert_eq!(validity[0..arrow_nulls.len()], arrow_nulls);
                } else {
                    assert_eq!(None, fl.nulls());
                }
            }
            CellStructure::Var(offsets) => {
                assert_eq!(
                    TypeId::of::<GenericListArray<i64>>(),
                    arrow.as_any().type_id()
                );
                let gl = arrow
                    .as_any()
                    .downcast_ref::<GenericListArray<i64>>()
                    .unwrap();

                /* check that values match */
                let primitive = {
                    assert_eq!(
                        TypeId::of::<MyPrimitiveArray<C>>(),
                        gl.values().as_any().type_id()
                    );
                    gl.values()
                        .as_any()
                        .downcast_ref::<MyPrimitiveArray<C>>()
                        .unwrap()
                };
                assert_eq!(None, primitive.nulls());

                /* check the offsets */
                let arrow_offsets = gl.offsets();
                if offsets.is_empty() || rr.ncells == 0 {
                    assert_eq!(0, rr.ncells);
                    assert_eq!(1, arrow_offsets.len());
                    assert_eq!(0, arrow_offsets[0]);
                    assert_eq!(0, primitive.len());
                } else {
                    let noffsets = rr.ncells + 1;
                    assert!(noffsets <= offsets.len());

                    assert_eq!(arrow_offsets.len(), noffsets);

                    /* check that offsets are mapped correctly */
                    for o in 0..noffsets {
                        assert_eq!(arrow_offsets[o], offsets[o] as i64);
                    }

                    assert_eq!(offsets[rr.ncells] as usize, primitive.len());
                }

                for (arrow, tiledb) in primitive
                    .iter()
                    .zip(rr.input.data[0..primitive.len()].iter())
                {
                    assert_eq!(Some(*tiledb), arrow);
                }

                /* check that validity matches */
                if let Some(validity) = rr.input.validity {
                    assert!(rr.ncells <= validity.len());

                    assert!(gl.nulls().is_some());
                    let arrow_nulls = gl.nulls().unwrap();
                    assert_eq!(rr.ncells, arrow_nulls.len());

                    let arrow_nulls = arrow_nulls
                        .iter()
                        .map(|b| if b { 1 } else { 0 })
                        .collect::<Vec<u8>>();
                    assert_eq!(validity[0..arrow_nulls.len()], arrow_nulls);
                } else {
                    assert_eq!(None, gl.nulls());
                }
            }
        }
    }

    proptest! {
        #[test]
        fn raw_read_to_arrow_u8(rr in any::<RawReadOutput<u8>>()) {
            raw_read_to_arrow(rr);
        }

        #[test]
        fn raw_read_to_arrow_u16(rr in any::<RawReadOutput<u16>>()) {
            raw_read_to_arrow(rr);
        }

        #[test]
        fn raw_read_to_arrow_u32(rr in any::<RawReadOutput<u32>>()) {
            raw_read_to_arrow(rr);
        }

        #[test]
        fn raw_read_to_arrow_u64(rr in any::<RawReadOutput<u64>>()) {
            raw_read_to_arrow(rr);
        }

        #[test]
        fn raw_read_to_arrow_i8(rr in any::<RawReadOutput<i8>>()) {
            raw_read_to_arrow(rr);
        }

        #[test]
        fn raw_read_to_arrow_i16(rr in any::<RawReadOutput<i16>>()) {
            raw_read_to_arrow(rr);
        }

        #[test]
        fn raw_read_to_arrow_i32(rr in any::<RawReadOutput<i32>>()) {
            raw_read_to_arrow(rr);
        }

        #[test]
        fn raw_read_to_arrow_i64(rr in any::<RawReadOutput<i64>>()) {
            raw_read_to_arrow(rr);
        }

        #[test]
        fn raw_read_to_arrow_f32(rr in any::<RawReadOutput<f32>>()) {
            raw_read_to_arrow(rr);
        }

        #[test]
        fn raw_read_to_arrow_f64(rr in any::<RawReadOutput<f64>>()) {
            raw_read_to_arrow(rr);
        }
    }

    fn do_raw_read_to_record_batch(rr: TypedRawReadOutput) {
        let arrow = crate::datatype::arrow::to_arrow(
            &rr.datatype,
            rr.cell_structure().as_cell_val_num(),
        )
        .into_inner();

        let arrow_schema = arrow::datatypes::Schema::new(vec![Field::new(
            "f",
            arrow,
            rr.is_nullable(),
        )]);

        let cols = vec![Arc::from(
            Arc::<dyn ArrowArray>::try_from(rr).expect("Integer overflow"),
        )];

        // NB: even constructing this successfully is a big deal due to schema match
        let _ = RecordBatch::try_new(Arc::new(arrow_schema), cols)
            .expect("Error constructing record batch");
    }

    #[test]
    fn raw_read_to_record_batch() {
        let strat = (
            any::<Datatype>(),
            any_with::<CellValNum>(Some(
                NonZeroU32::new(1).unwrap()
                    ..NonZeroU32::new(i32::MAX as u32).unwrap(),
            )),
            any::<bool>(),
        )
            .prop_flat_map(|(dt, cv, is_nullable)| {
                any_with::<TypedRawReadOutput>(Some((
                    dt,
                    RawReadOutputParameters {
                        cell_val_num: Some(cv),
                        is_nullable: Some(is_nullable),
                        ..Default::default()
                    },
                )))
            });

        proptest!(|(rr in strat)| {
            do_raw_read_to_record_batch(rr)
        });
    }
}
