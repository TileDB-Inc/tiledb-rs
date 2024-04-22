use std::fmt::Debug;
use std::rc::Rc;

use paste::paste;
use proptest::bits::{BitSetLike, VarBitSet};
use proptest::prelude::*;
use proptest::strategy::{NewTree, ValueTree};
use proptest::test_runner::TestRunner;

use super::*;
use crate::array::{ArrayType, CellValNum, SchemaData};
use crate::query::read::output::{
    FixedDataIterator, RawReadOutput, TypedRawReadOutput, VarDataIterator,
};
use crate::query::read::{
    CallbackVarArgReadBuilder, ManagedBuffer, RawReadHandle,
    ReadCallbackVarArg, TypedReadHandle,
};
use crate::{fn_typed, typed_query_buffers_go};

trait WriteFieldInput<C>: DataProvider<Unit = C> + Debug {}

impl<T, C> WriteFieldInput<C> for T where T: DataProvider<Unit = C> + Debug {}

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

impl From<&TypedRawReadOutput<'_>> for FieldData {
    fn from(value: &TypedRawReadOutput) -> Self {
        typed_query_buffers_go!(value.buffers, DT, ref handle, {
            let rr = RawReadOutput {
                nvalues: value.nvalues,
                nbytes: value.nbytes,
                input: handle,
            };
            if rr.input.cell_offsets.is_some() {
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

macro_rules! typed_field_data_go {
    ($field:expr, $DT:ident, $data:pat, $then:expr) => {
        match $field {
            FieldData::UInt8($data) => {
                type $DT = Vec<u8>;
                $then
            }
            FieldData::UInt16($data) => {
                type $DT = Vec<u16>;
                $then
            }
            FieldData::UInt32($data) => {
                type $DT = Vec<u32>;
                $then
            }
            FieldData::UInt64($data) => {
                type $DT = Vec<u64>;
                $then
            }
            FieldData::Int8($data) => {
                type $DT = Vec<i8>;
                $then
            }
            FieldData::Int16($data) => {
                type $DT = Vec<i16>;
                $then
            }
            FieldData::Int32($data) => {
                type $DT = Vec<i32>;
                $then
            }
            FieldData::Int64($data) => {
                type $DT = Vec<i64>;
                $then
            }
            FieldData::Float32($data) => {
                type $DT = Vec<f32>;
                $then
            }
            FieldData::Float64($data) => {
                type $DT = Vec<f64>;
                $then
            }
            FieldData::VecUInt8($data) => {
                type $DT = Vec<Vec<u8>>;
                $then
            }
            FieldData::VecUInt16($data) => {
                type $DT = Vec<Vec<u16>>;
                $then
            }
            FieldData::VecUInt32($data) => {
                type $DT = Vec<Vec<u32>>;
                $then
            }
            FieldData::VecUInt64($data) => {
                type $DT = Vec<Vec<u64>>;
                $then
            }
            FieldData::VecInt8($data) => {
                type $DT = Vec<Vec<i8>>;
                $then
            }
            FieldData::VecInt16($data) => {
                type $DT = Vec<Vec<i16>>;
                $then
            }
            FieldData::VecInt32($data) => {
                type $DT = Vec<Vec<i32>>;
                $then
            }
            FieldData::VecInt64($data) => {
                type $DT = Vec<Vec<i64>>;
                $then
            }
            FieldData::VecFloat32($data) => {
                type $DT = Vec<Vec<f32>>;
                $then
            }
            FieldData::VecFloat64($data) => {
                type $DT = Vec<Vec<f64>>;
                $then
            }
        }
    };
}

impl FieldData {
    pub fn len(&self) -> usize {
        typed_field_data_go!(self, _DT, v, v.len())
    }

    pub fn filter(&self, set: &VarBitSet) -> FieldData {
        typed_field_data_go!(self, _DT, ref values, {
            FieldData::from(
                values
                    .iter()
                    .enumerate()
                    .filter(|&(i, _)| set.test(i))
                    .map(|(_, e)| e.clone())
                    .collect::<Vec<_>>(),
            )
        })
    }
}

pub struct RawReadQueryResult(pub Vec<FieldData>);

pub struct RawResultCallback {}

impl ReadCallbackVarArg for RawResultCallback {
    type Intermediate = RawReadQueryResult;
    type Final = RawReadQueryResult;
    type Error = std::convert::Infallible;

    fn intermediate_result(
        &mut self,
        args: &[TypedRawReadOutput],
    ) -> Result<Self::Intermediate, Self::Error> {
        Ok(RawReadQueryResult(
            args.iter()
                .map(|a| FieldData::from(a))
                .collect::<Vec<FieldData>>(),
        ))
    }

    fn final_result(
        self,
        args: &[TypedRawReadOutput],
    ) -> Result<Self::Intermediate, Self::Error> {
        Ok(RawReadQueryResult(
            args.iter()
                .map(|a| FieldData::from(a))
                .collect::<Vec<FieldData>>(),
        ))
    }
}

#[derive(Clone, Debug)]
pub struct WriteQueryData {
    fields: Vec<(String, FieldData)>,
}

impl WriteQueryData {
    pub fn attach_write<'ctx, 'data>(
        &'data self,
        b: WriteBuilder<'ctx, 'data>,
    ) -> TileDBResult<WriteBuilder<'ctx, 'data>> {
        let mut b = b;
        for f in self.fields.iter() {
            b = typed_field_data_go!(
                &f.1,
                DT,
                data,
                b.data_typed::<_, DT>(&f.0, data)
            )?;
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
        let handles = {
            let schema = b.base().array().schema().unwrap();

            self.fields
                .iter()
                .map(|(name, _)| {
                    let field = schema.field(name.clone()).unwrap();
                    fn_typed!(field.datatype().unwrap(), DT, {
                        let managed: ManagedBuffer<DT> = ManagedBuffer::new(
                            field.query_scratch_allocator().unwrap(),
                        );
                        let rr = RawReadHandle::managed(name, managed);
                        TypedReadHandle::from(rr)
                    })
                })
                .collect::<Vec<TypedReadHandle>>()
        };

        b.register_callback_var(handles, RawResultCallback {})
    }
}

/// Mask for whether a field should be included in a write query.
// As of this writing, core does not support default values being filled in,
// so this construct is not terribly useful. But someday that may change
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum WriteFieldMask {
    /// This field must appear in the write set
    Include,
    /// This field appears in the write set but simplification may change that
    _TentativelyInclude,
    /// This field may appear in the write set again after complication
    _TentativelyExclude,
    /// This field may not appear in the write set again
    Exclude,
}

impl WriteFieldMask {
    pub fn is_included(&self) -> bool {
        matches!(
            self,
            WriteFieldMask::Include | WriteFieldMask::_TentativelyInclude
        )
    }
}

struct WriteQueryDataValueTree {
    schema: Rc<SchemaData>,
    field_mask: Vec<WriteFieldMask>,
    field_data: Vec<Option<FieldData>>,
    record_mask: VarBitSet,
}

impl ValueTree for WriteQueryDataValueTree {
    type Value = WriteQueryData;

    fn current(&self) -> Self::Value {
        let fields = self
            .field_mask
            .iter()
            .enumerate()
            .filter(|(_, f)| f.is_included())
            .map(|(i, _)| {
                let f = self.schema.field(i);
                (
                    f.name.clone(),
                    self.field_data[i]
                        .as_ref()
                        .unwrap()
                        .filter(&self.record_mask),
                )
            })
            .collect::<Vec<(String, FieldData)>>();

        WriteQueryData { fields }
    }

    fn simplify(&mut self) -> bool {
        unimplemented!()
    }

    fn complicate(&mut self) -> bool {
        unimplemented!()
    }
}

#[derive(Debug)]
struct WriteQueryDataStrategy {
    schema: Rc<SchemaData>,
}

impl WriteQueryDataStrategy {
    pub fn new(schema: &Rc<SchemaData>) -> Self {
        WriteQueryDataStrategy {
            schema: Rc::clone(schema),
        }
    }
}

impl Strategy for WriteQueryDataStrategy {
    type Tree = WriteQueryDataValueTree;
    type Value = WriteQueryData;

    fn new_tree(&self, runner: &mut TestRunner) -> NewTree<Self> {
        const WRITE_QUERY_MIN_RECORDS: usize = 0;
        const WRITE_QUERY_MAX_RECORDS: usize = 1024 * 1024;

        const WRITE_QUERY_MIN_VAR_SIZE: usize = 0;
        const WRITE_QUERY_MAX_VAR_SIZE: usize = 1024 * 128;

        /* Choose the maximum number of records */
        let nrecords = (WRITE_QUERY_MIN_RECORDS..=WRITE_QUERY_MAX_RECORDS)
            .new_tree(runner)?
            .current();

        /* generate a random set of fields to query */
        let field_mask = {
            let ndimensions = self.schema.domain.dimension.len();
            let nattributes = self.schema.attributes.len();

            let dimensions_mask = match self.schema.array_type {
                ArrayType::Dense => {
                    /* dense array coordinates are handled by a subarray */
                    vec![WriteFieldMask::Exclude; ndimensions]
                }
                ArrayType::Sparse => {
                    /* sparse array must write coordinates */
                    vec![WriteFieldMask::Include; ndimensions]
                }
            };

            /* choose a random set of attributes to initially manifest */
            let attributes_mask =
                std::iter::repeat(WriteFieldMask::Include).take(nattributes);

            dimensions_mask
                .into_iter()
                .chain(attributes_mask.into_iter())
                .collect::<Vec<_>>()
        };

        let field_data = field_mask
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if f.is_included() {
                    let field = self.schema.field(i);
                    let datatype = field.datatype;
                    let cell_val_num = field
                        .cell_val_num
                        .unwrap_or(CellValNum::try_from(1).unwrap());

                    if cell_val_num == 1u32 {
                        Some(fn_typed!(datatype, DT, {
                            let data = proptest::collection::vec(
                                any::<DT>(),
                                nrecords..=nrecords,
                            )
                            .new_tree(runner)
                            .expect("Error generating query data")
                            .current();

                            FieldData::from(data)
                        }))
                    } else {
                        let (min, max) = if cell_val_num.is_var_sized() {
                            (WRITE_QUERY_MIN_VAR_SIZE, WRITE_QUERY_MAX_VAR_SIZE)
                        } else {
                            let fixed_bound =
                                Into::<u32>::into(cell_val_num) as usize;
                            (fixed_bound, fixed_bound)
                        };
                        Some(fn_typed!(datatype, DT, {
                            let data = proptest::collection::vec(
                                proptest::collection::vec(
                                    any::<DT>(),
                                    min..=max,
                                ),
                                nrecords..=nrecords,
                            )
                            .new_tree(runner)
                            .expect("Error generating query data")
                            .current();

                            FieldData::from(data)
                        }))
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<Option<FieldData>>>();

        Ok(WriteQueryDataValueTree {
            schema: self.schema.clone(),
            field_mask,
            field_data,
            record_mask: VarBitSet::saturated(nrecords),
        })
    }
}

impl Arbitrary for WriteQueryData {
    type Parameters = Option<Rc<SchemaData>>;
    type Strategy = BoxedStrategy<WriteQueryData>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        if let Some(schema) = args {
            WriteQueryDataStrategy::new(&schema).boxed()
        } else {
            any::<SchemaData>()
                .prop_flat_map(|schema| {
                    WriteQueryDataStrategy::new(&Rc::new(schema))
                })
                .boxed()
        }
    }
}

#[derive(Debug)]
pub struct WriteSequence {
    writes: Vec<WriteQueryData>,
}

impl Arbitrary for WriteSequence {
    type Parameters = Option<Rc<SchemaData>>;
    type Strategy = BoxedStrategy<WriteSequence>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        if let Some(schema) = args {
            prop_write_sequence(&schema).boxed()
        } else {
            any::<SchemaData>()
                .prop_flat_map(|schema| prop_write_sequence(&Rc::new(schema)))
                .boxed()
        }
    }
}

impl IntoIterator for WriteSequence {
    type Item = WriteQueryData;
    type IntoIter = <Vec<Self::Item> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.writes.into_iter()
    }
}

pub fn prop_write_sequence(
    schema: &Rc<SchemaData>,
) -> impl Strategy<Value = WriteSequence> {
    const MAX_WRITES: usize = 8;
    proptest::collection::vec(
        any_with::<WriteQueryData>(Some(Rc::clone(schema))),
        0..MAX_WRITES,
    )
    .prop_map(|writes| WriteSequence { writes })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    use crate::array::Mode;
    use crate::Factory;

    fn do_write_readback(
        ctx: &Context,
        schema_spec: Rc<SchemaData>,
        write_sequence: WriteSequence,
    ) {
        let tempdir = TempDir::new().expect("Error creating temp dir");
        let uri = String::from("file:///")
            + tempdir.path().join("array").to_str().unwrap();

        let schema_in = schema_spec
            .create(&ctx)
            .expect("Error constructing arbitrary schema");
        Array::create(&ctx, &uri, schema_in).expect("Error creating array");

        let mut array =
            Array::open(&ctx, &uri, Mode::Write).expect("Error opening array");

        for write in write_sequence {
            /* write data */
            {
                let write = write
                    .attach_write(
                        WriteBuilder::new(array)
                            .expect("Error building write query"),
                    )
                    .expect("Error building write query")
                    .build();
                write.submit().expect("Error running write query");
                array = write.finalize().expect("Error finalizing write query");
            }

            /* then read it back */
            {
                let mut cursors = std::iter::repeat(0)
                    .take(write.fields.len())
                    .collect::<Vec<_>>();

                let mut read = write
                    .attach_read(
                        ReadBuilder::new(array)
                            .expect("Error building read query"),
                    )
                    .expect("Error building read query")
                    .build();

                loop {
                    let res = read.step().expect("Error in read query step");
                    match res.as_ref().into_inner() {
                        None => unimplemented!(), /* TODO: allocate more */
                        Some((raw, _)) => {
                            let raw = &raw.0;
                            /* TODO: when attributes have different cell val nums this will
                             * have to look different */
                            let mut nvalues = None;
                            for i in 0..raw.len() {
                                let r = &raw[i];
                                let w = &write.fields[i].1;

                                let nv = if let Some(nv) = nvalues {
                                    assert_eq!(nv, r.len());
                                    nv
                                } else {
                                    nvalues = Some(r.len());
                                    r.len()
                                };

                                let w = typed_field_data_go!(w, _DT, w, {
                                    FieldData::from(
                                        w[cursors[i]..cursors[i] + nv].to_vec(),
                                    )
                                });

                                assert_eq!(w, *r);

                                cursors[i] += nv;
                            }
                        }
                    }

                    if res.is_final() {
                        break;
                    }
                }

                array = read.finalize().expect("Error finalizing read query");
            }
        }
    }

    /// Test that each write in the sequence can be read back correctly at the right timestamp
    #[test]
    fn write_readback() {
        let ctx = Context::new().expect("Error creating context");

        let strategy = any::<SchemaData>().prop_flat_map(|schema| {
            let schema = Rc::new(schema);
            (
                Just(Rc::clone(&schema)),
                any_with::<WriteSequence>(Some(Rc::clone(&schema))),
            )
        });

        proptest!(|((schema_spec, write_sequence) in strategy)| {
            do_write_readback(&ctx, schema_spec, write_sequence)
        })
    }
}
