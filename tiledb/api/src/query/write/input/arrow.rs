use arrow::array::{Array as ArrowArray, PrimitiveArray};
use arrow::datatypes::ArrowPrimitiveType;

use crate::array::CellValNum;
use crate::datatype::PhysicalType;
use crate::error::{DatatypeErrorKind, Error};
use crate::query::buffer::{Buffer, CellStructure, QueryBuffers};
use crate::query::write::input::DataProvider;
use crate::Result as TileDBResult;

impl<A> DataProvider for PrimitiveArray<A>
where
    A: ArrowPrimitiveType,
    <A as ArrowPrimitiveType>::Native: PhysicalType,
{
    type Unit = <A as ArrowPrimitiveType>::Native;

    fn query_buffers(
        &self,
        cell_val_num: CellValNum,
        is_nullable: bool,
    ) -> TileDBResult<QueryBuffers<Self::Unit>> {
        let data = Buffer::Borrowed(self.values().as_ref());

        match cell_val_num {
            CellValNum::Fixed(nz) if nz.get() == 1 => {
                let validity = if let Some(nulls) = self.nulls() {
                    if is_nullable {
                        Some(
                            nulls
                                .iter()
                                .map(|v| if v { 1 } else { 0 })
                                .collect::<Vec<u8>>()
                                .into(),
                        )
                    } else if nulls.null_count() == 0 {
                        None
                    } else {
                        /* TODO: error out, we have nulls but they are not expected */
                        unimplemented!()
                    }
                } else {
                    if is_nullable {
                        Some(vec![1u8; self.values().len()].into())
                    } else {
                        None
                    }
                };

                Ok(QueryBuffers {
                    data,
                    cell_structure: CellStructure::single(),
                    validity,
                })
            }
            CellValNum::Fixed(nz) => {
                /* TODO: also check nulls */
                if self.values().len() % nz.get() as usize == 0 {
                    return Err(Error::Datatype(
                        DatatypeErrorKind::UnexpectedCellStructure {
                            context: None,
                            found: CellValNum::Fixed(nz),
                            expected: CellValNum::single(),
                        },
                    ));
                }

                if self.nulls().map(|n| n.null_count() > 0).unwrap_or(false) {
                    /* TODO: error out, no way to represent this */
                    unimplemented!()
                }

                Ok(QueryBuffers {
                    data,
                    cell_structure: CellStructure::Fixed(nz),
                    validity: None,
                })
            }
            CellValNum::Var => Err(Error::Datatype(
                DatatypeErrorKind::UnexpectedCellStructure {
                    context: None,
                    found: CellValNum::Var,
                    expected: CellValNum::single(),
                },
            )),
        }
    }
}
