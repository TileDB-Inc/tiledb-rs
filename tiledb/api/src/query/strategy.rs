use std::collections::hash_map::Entry;
use std::collections::HashMap;

use paste::paste;
use proptest::bits::{BitSetLike, VarBitSet};

use crate::datatype::LogicalType;
use crate::query::read::output::{
    FixedDataIterator, RawReadOutput, TypedRawReadOutput, VarDataIterator,
};
use crate::query::read::{
    CallbackVarArgReadBuilder, FieldMetadata, ManagedBuffer, RawReadHandle,
    ReadCallbackVarArg, ReadQueryBuilder, TypedReadHandle,
};
use crate::query::WriteBuilder;
use crate::{fn_typed, typed_query_buffers_go, Result as TileDBResult};

/// Represents the write query input for a single field.
/// For each variant, the outer Vec is the collection of records, and the interior is value in the
/// cell for the record. Fields with cell val num of 1 are flat, and other cell values use the
/// inner Vec. For fixed-size attributes, the inner Vecs shall all have the same length; for
/// var-sized attributes that is obviously not required.
#[derive(Clone, Debug, PartialEq)]
pub enum FieldData {
    UInt8(Vec<u8>),
    UInt16(Vec<u16>),
    UInt32(Vec<u32>),
    UInt64(Vec<u64>),
    Int8(Vec<i8>),
    Int16(Vec<i16>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    VecUInt8(Vec<Vec<u8>>),
    VecUInt16(Vec<Vec<u16>>),
    VecUInt32(Vec<Vec<u32>>),
    VecUInt64(Vec<Vec<u64>>),
    VecInt8(Vec<Vec<i8>>),
    VecInt16(Vec<Vec<i16>>),
    VecInt32(Vec<Vec<i32>>),
    VecInt64(Vec<Vec<i64>>),
    VecFloat32(Vec<Vec<f32>>),
    VecFloat64(Vec<Vec<f64>>),
}

macro_rules! typed_field_data {
    ($($V:ident : $U:ty),+) => {
        $(
            impl From<Vec<$U>> for FieldData {
                fn from(value: Vec<$U>) -> Self {
                    FieldData::$V(value)
                }
            }

            impl From<Vec<Vec<$U>>> for FieldData {
                fn from(value: Vec<Vec<$U>>) -> Self {
                    paste! {
                        FieldData::[< Vec $V >](value)
                    }
                }
            }
        )+
    };
}

typed_field_data!(UInt8: u8, UInt16: u16, UInt32: u32, UInt64: u64);
typed_field_data!(Int8: i8, Int16: i16, Int32: i32, Int64: i64);
typed_field_data!(Float32: f32, Float64: f64);

impl From<Vec<String>> for FieldData {
    fn from(value: Vec<String>) -> Self {
        FieldData::from(
            value
                .into_iter()
                .map(|s| s.into_bytes())
                .collect::<Vec<Vec<u8>>>(),
        )
    }
}

impl From<&TypedRawReadOutput<'_>> for FieldData {
    fn from(value: &TypedRawReadOutput) -> Self {
        typed_query_buffers_go!(value.buffers, DT, ref handle, {
            let rr = RawReadOutput {
                ncells: value.ncells,
                input: handle.borrow(),
            };
            if rr.input.cell_structure.is_var() {
                Self::from(
                    VarDataIterator::try_from(rr)
                        .unwrap()
                        .map(|s| s.to_vec())
                        .collect::<Vec<Vec<DT>>>(),
                )
            } else {
                Self::from(
                    FixedDataIterator::try_from(rr)
                        .unwrap()
                        .collect::<Vec<DT>>(),
                )
            }
        })
    }
}

#[macro_export]
macro_rules! typed_field_data_go {
    ($field:expr, $DT:ident, $data:pat, $fixed:expr, $var:expr) => {{
        use $crate::query::strategy::FieldData;
        match $field {
            FieldData::UInt8($data) => {
                type $DT = u8;
                $fixed
            }
            FieldData::UInt16($data) => {
                type $DT = u16;
                $fixed
            }
            FieldData::UInt32($data) => {
                type $DT = u32;
                $fixed
            }
            FieldData::UInt64($data) => {
                type $DT = u64;
                $fixed
            }
            FieldData::Int8($data) => {
                type $DT = i8;
                $fixed
            }
            FieldData::Int16($data) => {
                type $DT = i16;
                $fixed
            }
            FieldData::Int32($data) => {
                type $DT = i32;
                $fixed
            }
            FieldData::Int64($data) => {
                type $DT = i64;
                $fixed
            }
            FieldData::Float32($data) => {
                type $DT = f32;
                $fixed
            }
            FieldData::Float64($data) => {
                type $DT = f64;
                $fixed
            }
            FieldData::VecUInt8($data) => {
                type $DT = u8;
                $var
            }
            FieldData::VecUInt16($data) => {
                type $DT = u16;
                $var
            }
            FieldData::VecUInt32($data) => {
                type $DT = u32;
                $var
            }
            FieldData::VecUInt64($data) => {
                type $DT = u64;
                $var
            }
            FieldData::VecInt8($data) => {
                type $DT = i8;
                $var
            }
            FieldData::VecInt16($data) => {
                type $DT = i16;
                $var
            }
            FieldData::VecInt32($data) => {
                type $DT = i32;
                $var
            }
            FieldData::VecInt64($data) => {
                type $DT = i64;
                $var
            }
            FieldData::VecFloat32($data) => {
                type $DT = f32;
                $var
            }
            FieldData::VecFloat64($data) => {
                type $DT = f64;
                $var
            }
        }
    }};
    ($field:expr, $data:pat, $then:expr) => {
        typed_field_data_go!($field, _DT, $data, $then, $then)
    };
    ($lexpr:expr, $rexpr:expr, $DT:ident, $lpat:pat, $rpat:pat, $same_type:expr, $else:expr) => {{
        use $crate::query::strategy::FieldData;
        match ($lexpr, $rexpr) {
            (FieldData::UInt8($lpat), FieldData::UInt8($rpat)) => {
                type $DT = u8;
                $same_type
            }
            (FieldData::UInt16($lpat), FieldData::UInt16($rpat)) => {
                type $DT = u16;
                $same_type
            }
            (FieldData::UInt32($lpat), FieldData::UInt32($rpat)) => {
                type $DT = u32;
                $same_type
            }
            (FieldData::UInt64($lpat), FieldData::UInt64($rpat)) => {
                type $DT = u64;
                $same_type
            }
            (FieldData::Int8($lpat), FieldData::Int8($rpat)) => {
                type $DT = i8;
                $same_type
            }
            (FieldData::Int16($lpat), FieldData::Int16($rpat)) => {
                type $DT = i16;
                $same_type
            }
            (FieldData::Int32($lpat), FieldData::Int32($rpat)) => {
                type $DT = i32;
                $same_type
            }
            (FieldData::Int64($lpat), FieldData::Int64($rpat)) => {
                type $DT = i64;
                $same_type
            }
            (FieldData::Float32($lpat), FieldData::Float32($rpat)) => {
                type $DT = f32;
                $same_type
            }
            (FieldData::Float64($lpat), FieldData::Float64($rpat)) => {
                type $DT = f64;
                $same_type
            }
            (FieldData::VecUInt8($lpat), FieldData::VecUInt8($rpat)) => {
                type $DT = u8;
                $same_type
            }
            (FieldData::VecUInt16($lpat), FieldData::VecUInt16($rpat)) => {
                type $DT = u16;
                $same_type
            }
            (FieldData::VecUInt32($lpat), FieldData::VecUInt32($rpat)) => {
                type $DT = u32;
                $same_type
            }
            (FieldData::VecUInt64($lpat), FieldData::VecUInt64($rpat)) => {
                type $DT = u64;
                $same_type
            }
            (FieldData::VecInt8($lpat), FieldData::VecInt8($rpat)) => {
                type $DT = i8;
                $same_type
            }
            (FieldData::VecInt16($lpat), FieldData::VecInt16($rpat)) => {
                type $DT = i16;
                $same_type
            }
            (FieldData::VecInt32($lpat), FieldData::VecInt32($rpat)) => {
                type $DT = i32;
                $same_type
            }
            (FieldData::VecInt64($lpat), FieldData::VecInt64($rpat)) => {
                type $DT = i64;
                $same_type
            }
            (FieldData::VecFloat32($lpat), FieldData::VecFloat32($rpat)) => {
                type $DT = f32;
                $same_type
            }
            (FieldData::VecFloat64($lpat), FieldData::VecFloat64($rpat)) => {
                type $DT = f64;
                $same_type
            }
            _ => $else,
        }
    }};
}

impl FieldData {
    pub fn is_empty(&self) -> bool {
        typed_field_data_go!(self, v, v.is_empty())
    }

    pub fn len(&self) -> usize {
        typed_field_data_go!(self, v, v.len())
    }

    pub fn filter(&self, set: &VarBitSet) -> FieldData {
        typed_field_data_go!(self, ref values, {
            FieldData::from(
                values
                    .clone()
                    .into_iter()
                    .enumerate()
                    .filter(|&(i, _)| set.test(i))
                    .map(|(_, e)| e)
                    .collect::<Vec<_>>(),
            )
        })
    }
}

pub struct RawReadQueryResult(pub HashMap<String, FieldData>);

pub struct RawResultCallback {
    field_order: Vec<String>,
}

impl ReadCallbackVarArg for RawResultCallback {
    type Intermediate = RawReadQueryResult;
    type Final = RawReadQueryResult;
    type Error = std::convert::Infallible;

    fn intermediate_result(
        &mut self,
        args: Vec<TypedRawReadOutput>,
    ) -> Result<Self::Intermediate, Self::Error> {
        Ok(RawReadQueryResult(
            self.field_order
                .iter()
                .zip(args.iter())
                .map(|(f, a)| (f.clone(), FieldData::from(a)))
                .collect::<HashMap<String, FieldData>>(),
        ))
    }

    fn final_result(
        mut self,
        args: Vec<TypedRawReadOutput>,
    ) -> Result<Self::Intermediate, Self::Error> {
        self.intermediate_result(args)
    }
}

#[derive(Clone, Debug)]
pub struct Cells {
    pub fields: HashMap<String, FieldData>,
}

impl Cells {
    pub fn attach_write<'ctx, 'data>(
        &'data self,
        b: WriteBuilder<'ctx, 'data>,
    ) -> TileDBResult<WriteBuilder<'ctx, 'data>> {
        let mut b = b;
        for f in self.fields.iter() {
            b = typed_field_data_go!(f.1, data, b.data_typed(f.0, data))?;
        }
        Ok(b)
    }

    pub fn attach_read<'ctx, 'data, B>(
        &self,
        b: B,
    ) -> TileDBResult<CallbackVarArgReadBuilder<'data, RawResultCallback, B>>
    where
        B: ReadQueryBuilder<'ctx, 'data>,
    {
        let field_order = self.fields.keys().cloned().collect::<Vec<_>>();
        let handles = {
            let schema = b.base().array().schema().unwrap();

            field_order
                .iter()
                .map(|name| {
                    let field = schema.field(name.clone()).unwrap();
                    fn_typed!(field.datatype().unwrap(), LT, {
                        type DT = <LT as LogicalType>::PhysicalType;
                        let managed: ManagedBuffer<DT> = ManagedBuffer::new(
                            field.query_scratch_allocator().unwrap(),
                        );
                        let metadata = FieldMetadata::try_from(&field).unwrap();
                        let rr = RawReadHandle::managed(metadata, managed);
                        TypedReadHandle::from(rr)
                    })
                })
                .collect::<Vec<TypedReadHandle>>()
        };

        b.register_callback_var(handles, RawResultCallback { field_order })
    }

    /// Copies data from the argument.
    /// Overwrites data at common indices and extends `self` where necessary.
    pub fn copy_from(&mut self, cells: Self) {
        for (field, data) in cells.fields.into_iter() {
            match self.fields.entry(field) {
                Entry::Vacant(v) => {
                    v.insert(data);
                }
                Entry::Occupied(mut o) => {
                    let prev_write_data = o.get_mut();
                    typed_field_data_go!(
                        prev_write_data,
                        data,
                        _DT,
                        ref mut mine,
                        theirs,
                        {
                            if mine.len() <= theirs.len() {
                                *mine = theirs;
                            } else {
                                mine[0..theirs.len()]
                                    .clone_from_slice(theirs.as_slice());
                            }
                        },
                        unreachable!()
                    );
                }
            }
        }
    }
}
