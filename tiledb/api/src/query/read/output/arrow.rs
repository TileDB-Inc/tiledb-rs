use std::sync::Arc;

use arrow::array::{Array as ArrowArray, GenericListArray, PrimitiveArray};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::Field;

use crate::datatype::arrow::ArrowPrimitiveTypeNative;
use crate::error::{DatatypeErrorKind, Error};
use crate::query::buffer::{Buffer, QueryBuffers, TypedQueryBuffers};
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

        if value.input.cell_offsets.is_some() {
            return Err(Error::Datatype(DatatypeErrorKind::ExpectedFixedSize(
                None,
            )));
        }

        Ok(if let Some(validity) = value.input.validity {
            let data: Vec<C> = match value.input.data {
                Buffer::Empty => vec![],
                Buffer::Owned(data) => {
                    let mut data = data.into_vec();
                    data.truncate(value.nvalues);
                    data
                }
                Buffer::Borrowed(data) => data[0..value.nvalues].to_vec(),
            };

            let validity = match validity {
                Buffer::Empty => vec![],
                Buffer::Owned(v) => {
                    let mut v = v.into_vec();
                    v.truncate(value.nvalues);
                    v
                }
                Buffer::Borrowed(v) => v[0..value.nvalues].to_vec(),
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
            v.truncate(value.nvalues);
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
    fn from(mut value: RawReadOutput<'data, C>) -> Self {
        /* TODO: needs the cell val num to make the decision properly */

        type MyPrimitiveArray<C> =
            PrimitiveArray<<C as ArrowPrimitiveTypeNative>::ArrowPrimitiveType>;

        let offsets = value.input.cell_offsets.take();

        let mut v_offsets = if let Some(offsets) = offsets {
            match offsets {
                Buffer::Empty => vec![],
                Buffer::Owned(offsets) => {
                    let mut offsets = offsets.into_vec();
                    offsets.truncate(value.nvalues);
                    offsets
                }
                Buffer::Borrowed(offsets) => offsets[0..value.nvalues].to_vec(),
            }
        } else {
            let flat = MyPrimitiveArray::<C>::try_from(value).unwrap();
            return Arc::new(flat);
        };

        let arrow_values = {
            let rr = RawReadOutput {
                nvalues: value.nbytes / std::mem::size_of::<C>(),
                nbytes: value.nbytes, // unused
                input: QueryBuffers {
                    data: value.input.data,
                    cell_offsets: None,
                    validity: None,
                },
            };
            MyPrimitiveArray::<C>::try_from(rr).unwrap()
        };

        v_offsets.push(value.nbytes as u64);

        let v_offsets = v_offsets
            .into_iter()
            .map(|o| (o / std::mem::size_of::<C>() as u64) as i64)
            .collect::<Vec<i64>>();

        let field = Arc::new(Field::new_list_field(
            arrow_values.data_type().clone(),
            value.input.validity.is_some(),
        ));

        let arrow_offsets =
            OffsetBuffer::<i64>::new(ScalarBuffer::<i64>::from(v_offsets));

        let arrow_validity = if let Some(validity) = value.input.validity {
            let validity: Vec<u8> = match validity {
                Buffer::Empty => vec![],
                Buffer::Owned(validity) => validity.into_vec(),
                Buffer::Borrowed(validity) => {
                    validity[0..value.nvalues].to_vec()
                }
            };
            let arrow_validity = validity
                .into_iter()
                .take(value.nvalues)
                .map(|v: u8| v != 0)
                .collect::<arrow::buffer::NullBuffer>();
            Some(arrow_validity)
        } else {
            None
        };

        let list_array = GenericListArray::<i64>::try_new(
            field,
            arrow_offsets,
            Arc::new(arrow_values),
            arrow_validity,
        )
        .expect("TileDB internal error constructing Arrow buffers");

        Arc::new(list_array)
    }
}

impl From<TypedRawReadOutput<'_>> for Arc<dyn ArrowArray> {
    fn from(value: TypedRawReadOutput<'_>) -> Self {
        typed_query_buffers_go!(value.buffers, _DT, input, {
            RawReadOutput {
                nvalues: value.nvalues,
                nbytes: value.nbytes,
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
                nvalues: rr.nvalues,
                nbytes: rr.nbytes,
                input: rr.input.borrow(),
            };
            Arc::<dyn ArrowArray>::from(rrborrow)
        };

        if let Some(offsets) = rr.input.cell_offsets {
            assert_eq!(
                TypeId::of::<GenericListArray<i64>>(),
                arrow.as_any().type_id()
            );
            let gl = arrow
                .as_any()
                .downcast_ref::<GenericListArray<i64>>()
                .unwrap();

            let arrow_offsets = gl.offsets();
            assert!(arrow_offsets.len() <= offsets.len() + 1);
            assert_eq!(arrow_offsets.len(), rr.nvalues + 1);

            /* arrow offsets are value, tiledb offsets are bytes */
            let offset_scale = std::mem::size_of::<C>() as i64;

            /* check that offsets are mapped correctly */
            for o in 0..rr.nvalues {
                assert_eq!(arrow_offsets[o] * offset_scale, offsets[o] as i64);
            }
            assert_eq!(
                arrow_offsets[rr.nvalues] * offset_scale,
                rr.nbytes as i64
            );

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

            assert_eq!(rr.nbytes, primitive.len() * std::mem::size_of::<C>());
            for (arrow, tiledb) in primitive
                .iter()
                .zip(rr.input.data[0..primitive.len()].iter())
            {
                assert_eq!(Some(*tiledb), arrow);
            }

            /* check that validity matches */
            if let Some(validity) = rr.input.validity {
                assert!(gl.nulls().is_some());
                let arrow_nulls = gl.nulls().unwrap();
                assert_eq!(rr.nvalues, arrow_nulls.len());

                let arrow_nulls = arrow_nulls
                    .iter()
                    .map(|b| if b { 1 } else { 0 })
                    .collect::<Vec<u8>>();
                assert_eq!(validity[0..arrow_nulls.len()], arrow_nulls);
            } else {
                assert_eq!(None, gl.nulls());
            }
        } else {
            assert_eq!(
                TypeId::of::<MyPrimitiveArray<C>>(),
                arrow.as_any().type_id()
            );
            let primitive = arrow
                .as_any()
                .downcast_ref::<MyPrimitiveArray<C>>()
                .unwrap();

            assert_eq!(rr.nvalues, primitive.len());

            /* ensure that neither or both has validity values */
            if rr.input.validity.is_some() {
                assert!(primitive.nulls().is_some());
                assert_eq!(rr.nvalues, primitive.nulls().unwrap().len());
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
        };
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
