use std::sync::Arc;

use arrow::array::{
    Array as ArrowArray, FixedSizeListArray, GenericListArray, PrimitiveArray,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::Field;

use crate::array::CellValNum;
use crate::datatype::arrow::ArrowPrimitiveTypeNative;
use crate::error::{DatatypeErrorKind, Error};
use crate::query::buffer::{
    Buffer, CellStructure, QueryBuffers, TypedQueryBuffers,
};
use crate::query::read::output::{RawReadOutput, TypedRawReadOutput};
use crate::{typed_query_buffers_go, Result as TileDBResult};

impl<'data, C> TryFrom<RawReadOutput<'data, C>>
    for PrimitiveArray<<C as ArrowPrimitiveTypeNative>::ArrowPrimitiveType>
where
    C: ArrowPrimitiveTypeNative,
    PrimitiveArray<<C as ArrowPrimitiveTypeNative>::ArrowPrimitiveType>:
        From<Vec<C>> + From<Vec<Option<C>>>,
{
    type Error = crate::error::Error;

    fn try_from(value: RawReadOutput<C>) -> TileDBResult<Self> {
        type MyPrimitiveArray<C> =
            PrimitiveArray<<C as ArrowPrimitiveTypeNative>::ArrowPrimitiveType>;

        match value.input.cell_structure {
            CellStructure::Fixed(nz) if nz.get() == 1 => {}
            structure => {
                return Err(Error::Datatype(
                    DatatypeErrorKind::UnexpectedCellStructure {
                        context: None,
                        expected: CellValNum::single(),
                        found: structure.as_cell_val_num(),
                    },
                ))
            }
        }

        Ok(if let Some(validity) = value.input.validity {
            let data: Vec<C> = match value.input.data {
                Buffer::Empty => vec![],
                Buffer::Owned(data) => {
                    let mut data = data.into_vec();
                    data.truncate(value.ncells);
                    data
                }
                Buffer::Borrowed(data) => data[0..value.ncells].to_vec(),
            };

            let validity = match validity {
                Buffer::Empty => vec![],
                Buffer::Owned(v) => {
                    let mut v = v.into_vec();
                    v.truncate(value.ncells);
                    v
                }
                Buffer::Borrowed(v) => v[0..value.ncells].to_vec(),
            };
            let validity = validity
                .into_iter()
                .map(|v| v != 0)
                .collect::<arrow::buffer::NullBuffer>();
            MyPrimitiveArray::<C>::new(data.into(), Some(validity))
        } else {
            let mut v: Vec<C> = match value.input.data {
                Buffer::Empty => vec![],
                Buffer::Owned(b) => b.into_vec(),
                Buffer::Borrowed(b) => b.to_vec(),
            };
            v.truncate(value.ncells);
            v.into()
        })
    }
}

impl<'data, C> From<RawReadOutput<'data, C>> for Arc<dyn ArrowArray>
where
    C: ArrowPrimitiveTypeNative,
    PrimitiveArray<<C as ArrowPrimitiveTypeNative>::ArrowPrimitiveType>:
        From<Vec<C>> + From<Vec<Option<C>>>,
{
    fn from(value: RawReadOutput<'data, C>) -> Self {
        type MyPrimitiveArray<C> =
            PrimitiveArray<<C as ArrowPrimitiveTypeNative>::ArrowPrimitiveType>;

        match value.input.cell_structure {
            CellStructure::Fixed(nz) if nz.get() == 1 => {
                let flat = MyPrimitiveArray::<C>::try_from(value).unwrap();
                Arc::new(flat)
            }
            CellStructure::Fixed(nz) => {
                let flat = {
                    let rr = RawReadOutput {
                        ncells: value.ncells * nz.get() as usize,
                        input: QueryBuffers {
                            data: value.input.data,
                            cell_structure: CellStructure::single(),
                            validity: None, /* TODO */
                        },
                    };
                    // the `unwrap` will succeed because the `Err` conditions are
                    // on the cell val num
                    MyPrimitiveArray::<C>::try_from(rr).unwrap()
                };
                let flat: Arc<dyn ArrowArray> = Arc::new(flat);
                let field =
                    Field::new_list_field(flat.data_type().clone(), false);

                let fixed_len = match i32::try_from(nz.get()) {
                    Ok(len) => len,
                    Err(_) => {
                        /*
                         * What we probably want to do here is cry, I mean conjure up a
                         * generic list array with some large offsets. Leaving it out
                         * for now because we need more work from the Attribute/RecordBatch
                         * to identify if that's actually true.
                         */
                        unimplemented!()
                    }
                };

                let arrow_validity =
                    if let Some(validity) = value.input.validity {
                        let validity: Vec<u8> = match validity {
                            Buffer::Empty => vec![],
                            Buffer::Owned(validity) => validity.into_vec(),
                            Buffer::Borrowed(validity) => {
                                validity[0..value.ncells].to_vec()
                            }
                        };
                        let arrow_validity = validity
                            .into_iter()
                            .take(value.ncells)
                            .map(|v: u8| v != 0)
                            .collect::<arrow::buffer::NullBuffer>();
                        Some(arrow_validity)
                    } else {
                        None
                    };

                let fixed = FixedSizeListArray::new(
                    Arc::new(field),
                    fixed_len,
                    flat,
                    arrow_validity,
                );
                Arc::new(fixed)
            }
            CellStructure::Var(offsets) => {
                let nvalues = if value.ncells < 1 {
                    0
                } else {
                    *offsets.as_ref().last().unwrap() as usize
                };
                let flat = {
                    let rr = RawReadOutput {
                        ncells: nvalues,
                        input: QueryBuffers {
                            data: value.input.data,
                            cell_structure: CellStructure::single(),
                            validity: None,
                        },
                    };
                    // the `unwrap` will succeed because the `Err` conditions are
                    // on the cell val num
                    MyPrimitiveArray::<C>::try_from(rr).unwrap()
                };
                let flat: Arc<dyn ArrowArray> = Arc::new(flat);

                let offsets = if value.ncells == 0 {
                    vec![0u64]
                } else {
                    let noffsets = value.ncells + 1;
                    match offsets {
                        Buffer::Empty => vec![0u64],
                        Buffer::Borrowed(offsets) => {
                            offsets[0..noffsets].to_vec()
                        }
                        Buffer::Owned(offsets) => {
                            let mut offsets = offsets.into_vec();
                            offsets.truncate(noffsets);
                            offsets
                        }
                    }
                };

                // convert u64 byte offsets to i64 element offsets
                let offsets =
                    offsets.into_iter().map(|o| o as i64).collect::<Vec<i64>>();

                let field = Arc::new(Field::new_list_field(
                    flat.data_type().clone(),
                    value.input.validity.is_some(),
                ));

                let arrow_offsets = OffsetBuffer::<i64>::new(
                    ScalarBuffer::<i64>::from(offsets),
                );
                let arrow_validity =
                    if let Some(validity) = value.input.validity {
                        let validity: Vec<u8> = match validity {
                            Buffer::Empty => vec![],
                            Buffer::Owned(validity) => validity.into_vec(),
                            Buffer::Borrowed(validity) => {
                                validity[0..value.ncells].to_vec()
                            }
                        };
                        let arrow_validity = validity
                            .into_iter()
                            .take(value.ncells)
                            .map(|v: u8| v != 0)
                            .collect::<arrow::buffer::NullBuffer>();
                        Some(arrow_validity)
                    } else {
                        None
                    };

                let list_array = GenericListArray::<i64>::try_new(
                    field,
                    arrow_offsets,
                    Arc::new(flat),
                    arrow_validity,
                )
                .expect("TileDB internal error constructing Arrow buffers");

                Arc::new(list_array)
            }
        }
    }
}

impl From<TypedRawReadOutput<'_>> for Arc<dyn ArrowArray> {
    fn from(value: TypedRawReadOutput<'_>) -> Self {
        typed_query_buffers_go!(value.buffers, _DT, input, {
            RawReadOutput {
                ncells: value.ncells,
                input,
            }
            .into()
        })
    }
}

#[cfg(test)]
mod tests {
    use std::any::TypeId;

    use super::*;
    use proptest::prelude::*;

    fn raw_read_to_arrow<C>(rr: RawReadOutput<C>)
    where
        C: ArrowPrimitiveTypeNative,
        PrimitiveArray<<C as ArrowPrimitiveTypeNative>::ArrowPrimitiveType>:
            From<Vec<C>> + From<Vec<Option<C>>>,
    {
        type MyPrimitiveArray<C> =
            PrimitiveArray<<C as ArrowPrimitiveTypeNative>::ArrowPrimitiveType>;

        let arrow = {
            let rrborrow = RawReadOutput {
                ncells: rr.ncells,
                input: rr.input.borrow(),
            };
            Arc::<dyn ArrowArray>::from(rrborrow)
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
}
