use std::rc::Rc;
use std::sync::Arc;

use arrow::array::{
    Array as ArrowArray, FixedSizeBinaryArray, FixedSizeListArray,
    GenericListArray, LargeBinaryArray, LargeStringArray, PrimitiveArray,
    RecordBatch,
};
use arrow::datatypes::{ArrowPrimitiveType, Field};

use crate::array::{CellValNum, Schema};
use crate::datatype::PhysicalType;
use crate::error::{DatatypeErrorKind, Error};
use crate::query::buffer::{
    Buffer, CellStructure, QueryBuffers, TypedQueryBuffers,
};
use crate::query::write::input::{
    DataProvider, RecordProvider, TypedDataProvider,
};
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

impl DataProvider for FixedSizeBinaryArray {
    type Unit = u8;

    fn query_buffers(
        &self,
        cell_val_num: CellValNum,
        is_nullable: bool,
    ) -> TileDBResult<QueryBuffers<Self::Unit>> {
        todo!()
    }
}

impl DataProvider for LargeBinaryArray {
    type Unit = u8;

    fn query_buffers(
        &self,
        cell_val_num: CellValNum,
        is_nullable: bool,
    ) -> TileDBResult<QueryBuffers<Self::Unit>> {
        todo!()
    }
}

impl DataProvider for LargeStringArray {
    type Unit = u8;

    fn query_buffers(
        &self,
        cell_val_num: CellValNum,
        is_nullable: bool,
    ) -> TileDBResult<QueryBuffers<Self::Unit>> {
        todo!()
    }
}

impl TypedDataProvider for FixedSizeListArray {
    fn typed_query_buffers(
        &self,
        cell_val_num: CellValNum,
        is_nullable: bool,
    ) -> TileDBResult<TypedQueryBuffers> {
        todo!()
    }
}

impl TypedDataProvider for GenericListArray<i64> {
    fn typed_query_buffers(
        &self,
        cell_val_num: CellValNum,
        is_nullable: bool,
    ) -> TileDBResult<TypedQueryBuffers> {
        todo!()
    }
}

impl TypedDataProvider for dyn ArrowArray {
    fn typed_query_buffers(
        &self,
        cell_val_num: CellValNum,
        is_nullable: bool,
    ) -> TileDBResult<TypedQueryBuffers> {
        let c = cell_val_num;
        let n = is_nullable;

        use arrow::array::AsArray;
        use arrow::datatypes::{DataType as ADT, *};
        match self.data_type() {
            ADT::Null => unimplemented!(),
            ADT::Boolean => unimplemented!(),
            ADT::Float16 => unimplemented!(),
            ADT::Duration(_) => unimplemented!(), /* possible but bit width is not specified */
            ADT::Interval(_) => unimplemented!(), /* possible but bit width is not specified */
            ADT::Binary => unimplemented!(),      /* offset is 32-bit */
            ADT::Utf8 => unimplemented!(),        /* offset is 32-bit */
            ADT::BinaryView => unimplemented!(),
            ADT::Utf8View => unimplemented!(),
            ADT::List(_) => unimplemented!(), /* 32 bit offsets */
            ADT::ListView(_) => todo!(),
            ADT::LargeListView(_) => todo!(),
            ADT::Struct(_) => todo!(),
            ADT::Union(_, _) => todo!(),
            ADT::Dictionary(_, _) => todo!(),
            ADT::Decimal128(_, _) => todo!(),
            ADT::Decimal256(_, _) => todo!(),
            ADT::Map(_, _) => todo!(),
            ADT::RunEndEncoded(_, _) => todo!(),
            ADT::UInt8 => {
                self.as_primitive::<UInt8Type>().typed_query_buffers(c, n)
            }
            ADT::UInt16 => {
                self.as_primitive::<UInt16Type>().typed_query_buffers(c, n)
            }
            ADT::UInt32 => {
                self.as_primitive::<UInt32Type>().typed_query_buffers(c, n)
            }
            ADT::UInt64 => {
                self.as_primitive::<UInt64Type>().typed_query_buffers(c, n)
            }
            ADT::Int8 => {
                self.as_primitive::<Int8Type>().typed_query_buffers(c, n)
            }
            ADT::Int16 => {
                self.as_primitive::<Int16Type>().typed_query_buffers(c, n)
            }
            ADT::Int32 => {
                self.as_primitive::<Int32Type>().typed_query_buffers(c, n)
            }
            ADT::Int64 => {
                self.as_primitive::<Int64Type>().typed_query_buffers(c, n)
            }
            ADT::Float32 => {
                self.as_primitive::<Float32Type>().typed_query_buffers(c, n)
            }
            ADT::Float64 => {
                self.as_primitive::<Float64Type>().typed_query_buffers(c, n)
            }
            ADT::Timestamp(TimeUnit::Second, _) => self
                .as_primitive::<TimestampSecondType>()
                .typed_query_buffers(c, n),
            ADT::Timestamp(TimeUnit::Millisecond, _) => self
                .as_primitive::<TimestampMillisecondType>()
                .typed_query_buffers(c, n),
            ADT::Timestamp(TimeUnit::Microsecond, _) => self
                .as_primitive::<TimestampMicrosecondType>()
                .typed_query_buffers(c, n),
            ADT::Timestamp(TimeUnit::Nanosecond, _) => self
                .as_primitive::<TimestampNanosecondType>()
                .typed_query_buffers(c, n),
            ADT::Date32 => {
                self.as_primitive::<Date32Type>().typed_query_buffers(c, n)
            }
            ADT::Date64 => {
                self.as_primitive::<Date64Type>().typed_query_buffers(c, n)
            }
            ADT::Time32(TimeUnit::Second) => self
                .as_primitive::<Time32SecondType>()
                .typed_query_buffers(c, n),
            ADT::Time32(TimeUnit::Millisecond) => self
                .as_primitive::<Time32MillisecondType>()
                .typed_query_buffers(c, n),
            ADT::Time32(_) => unreachable!(),
            ADT::Time64(TimeUnit::Microsecond) => self
                .as_primitive::<Time64MicrosecondType>()
                .typed_query_buffers(c, n),
            ADT::Time64(TimeUnit::Nanosecond) => self
                .as_primitive::<Time64NanosecondType>()
                .typed_query_buffers(c, n),
            ADT::Time64(_) => unreachable!(),
            ADT::FixedSizeBinary(_) => {
                self.as_fixed_size_binary().typed_query_buffers(c, n)
            }
            ADT::LargeBinary => {
                self.as_binary::<i64>().typed_query_buffers(c, n)
            }
            ADT::LargeUtf8 => self.as_string::<i64>().typed_query_buffers(c, n),
            ADT::FixedSizeList(_, _) => {
                self.as_fixed_size_list().typed_query_buffers(c, n)
            }
            ADT::LargeList(_) => {
                self.as_list::<i64>().typed_query_buffers(c, n)
            }
        }
    }
}

impl<'data> RecordProvider<'data> for RecordBatch {
    type Iter = RecordBatchTileDBInputs<'data>;

    fn tiledb_inputs(
        &'data self,
        schema: Rc<Schema>,
    ) -> RecordBatchTileDBInputs<'data> {
        RecordBatchTileDBInputs::new(self, schema)
    }
}

pub struct RecordBatchTileDBInputs<'data> {
    schema: Rc<Schema>,
    fields: core::slice::Iter<'data, Arc<Field>>,
    columns: core::slice::Iter<'data, Arc<dyn ArrowArray>>,
}

impl<'data> RecordBatchTileDBInputs<'data> {
    pub fn new(batch: &'data RecordBatch, schema: Rc<Schema>) -> Self {
        RecordBatchTileDBInputs {
            schema,
            fields: batch.schema_ref().fields.iter(),
            columns: batch.columns().iter(),
        }
    }
}

impl<'data> Iterator for RecordBatchTileDBInputs<'data> {
    type Item = TileDBResult<(String, TypedQueryBuffers<'data>)>;
    fn next(&mut self) -> Option<Self::Item> {
        match (self.fields.next(), self.columns.next()) {
            (None, None) => None,
            (Some(f), Some(c)) => {
                let Some((datatype, cell_val_num)) =
                    crate::datatype::arrow::from_arrow(f.data_type()).ok()
                else {
                    /* TODO: error */
                    unimplemented!()
                };

                let tiledb_field = match self.schema.field(f.name()) {
                    Ok(field) => field,
                    Err(e) => return Some(Err(e)),
                };
                let field_datatype = match tiledb_field.datatype() {
                    Ok(datatype) => datatype,
                    Err(e) => return Some(Err(e)),
                };
                if datatype != field_datatype {
                    return Some(Err(Error::Datatype(
                        DatatypeErrorKind::InvalidDatatype {
                            context: Some(f.name().clone()),
                            found: datatype,
                            expected: field_datatype,
                        },
                    )));
                }
                let field_cell_val_num = match tiledb_field.cell_val_num() {
                    Ok(cvn) => cvn,
                    Err(e) => return Some(Err(e)),
                };
                if cell_val_num != field_cell_val_num {
                    /* TODO: we can be more flexible, e.g. fixed size list can go to Var */
                    return Some(Err(Error::Datatype(
                        DatatypeErrorKind::UnexpectedCellStructure {
                            context: Some(f.name().clone()),
                            found: cell_val_num,
                            expected: field_cell_val_num,
                        },
                    )));
                }
                let field_is_nullable = match tiledb_field.nullability() {
                    Ok(is_nullable) => is_nullable,
                    Err(e) => return Some(Err(e)),
                };
                Some(
                    c.typed_query_buffers(cell_val_num, field_is_nullable)
                        .map(|qb| (f.name().clone(), qb)),
                )
            }
            _ => {
                /* arrow documentation asserts they have the same length */
                unreachable!()
            }
        }
    }
}