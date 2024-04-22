use std::fmt::Debug;
use std::rc::Rc;

use paste::paste;
use proptest::prelude::*;
use proptest::strategy::{NewTree, ValueTree};
use proptest::test_runner::TestRunner;

use super::*;
use crate::array::{ArrayType, CellValNum, SchemaData};
use crate::fn_typed;

trait WriteFieldInput<C>: DataProvider<Unit = C> + Debug {}

impl<T, C> WriteFieldInput<C> for T where T: DataProvider<Unit = C> + Debug {}

#[derive(Clone, Debug)]
pub enum WriteFieldData {
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

macro_rules! typed_write_field {
    ($($V:ident : $U:ty),+) => {
        $(
            impl From<Vec<$U>> for WriteFieldData {
                fn from(value: Vec<$U>) -> Self {
                    WriteFieldData::$V(value)
                }
            }

            impl From<Vec<Vec<$U>>> for WriteFieldData {
                fn from(value: Vec<Vec<$U>>) -> Self {
                    paste! {
                        WriteFieldData::[< Vec $V >](value)
                    }
                }
            }
        )+
    };
}

typed_write_field!(UInt8: u8, UInt16: u16, UInt32: u32, UInt64: u64);
typed_write_field!(Int8: i8, Int16: i16, Int32: i32, Int64: i64);
typed_write_field!(Float32: f32, Float64: f64);

macro_rules! fn_write_field {
    ($field:expr, $DT:ident, $data:ident, $then:expr) => {
        match $field {
            WriteFieldData::UInt8(ref $data) => {
                type $DT = Vec<u8>;
                $then
            }
            WriteFieldData::UInt16(ref $data) => {
                type $DT = Vec<u16>;
                $then
            }
            WriteFieldData::UInt32(ref $data) => {
                type $DT = Vec<u32>;
                $then
            }
            WriteFieldData::UInt64(ref $data) => {
                type $DT = Vec<u64>;
                $then
            }
            WriteFieldData::Int8(ref $data) => {
                type $DT = Vec<i8>;
                $then
            }
            WriteFieldData::Int16(ref $data) => {
                type $DT = Vec<i16>;
                $then
            }
            WriteFieldData::Int32(ref $data) => {
                type $DT = Vec<i32>;
                $then
            }
            WriteFieldData::Int64(ref $data) => {
                type $DT = Vec<i64>;
                $then
            }
            WriteFieldData::Float32(ref $data) => {
                type $DT = Vec<f32>;
                $then
            }
            WriteFieldData::Float64(ref $data) => {
                type $DT = Vec<f64>;
                $then
            }
            WriteFieldData::VecUInt8(ref $data) => {
                type $DT = Vec<Vec<u8>>;
                $then
            }
            WriteFieldData::VecUInt16(ref $data) => {
                type $DT = Vec<Vec<u16>>;
                $then
            }
            WriteFieldData::VecUInt32(ref $data) => {
                type $DT = Vec<Vec<u32>>;
                $then
            }
            WriteFieldData::VecUInt64(ref $data) => {
                type $DT = Vec<Vec<u64>>;
                $then
            }
            WriteFieldData::VecInt8(ref $data) => {
                type $DT = Vec<Vec<i8>>;
                $then
            }
            WriteFieldData::VecInt16(ref $data) => {
                type $DT = Vec<Vec<i16>>;
                $then
            }
            WriteFieldData::VecInt32(ref $data) => {
                type $DT = Vec<Vec<i32>>;
                $then
            }
            WriteFieldData::VecInt64(ref $data) => {
                type $DT = Vec<Vec<i64>>;
                $then
            }
            WriteFieldData::VecFloat32(ref $data) => {
                type $DT = Vec<Vec<f32>>;
                $then
            }
            WriteFieldData::VecFloat64(ref $data) => {
                type $DT = Vec<Vec<f64>>;
                $then
            }
        }
    };
}

#[derive(Clone, Debug)]
pub struct WriteQueryData {
    fields: Vec<(String, WriteFieldData)>,
}

impl WriteQueryData {
    pub fn attach_write<'ctx, 'data>(
        &'data self,
        b: WriteBuilder<'ctx, 'data>,
    ) -> TileDBResult<WriteBuilder<'ctx, 'data>> {
        let mut b = b;
        for f in self.fields.iter() {
            b = fn_write_field!(
                &f.1,
                DT,
                data,
                b.data_typed::<_, DT>(&f.0, data)
            )?;
        }
        Ok(b)
    }

    pub fn attach_read<'ctx, 'data, B>(
        &'data mut self,
        b: B,
    ) -> VarCallbackReadQueryBuilder<'data, B>
    where
        B: ReadQueryBuilder<'ctx, 'data>,
    {
        self.fields.iter_mut().map(|(name, data)| unimplemented!());
        unimplemented!()
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum WriteFieldMask {
    /// This field must appear in the write set
    Include,
    /// This field appears in the write set but simplification may change that
    TentativelyInclude,
    /// This field may appear in the write set again after complication
    TentativelyExclude,
    /// This field may not appear in the write set again
    Exclude,
}

impl WriteFieldMask {
    pub fn is_included(&self) -> bool {
        matches!(
            self,
            WriteFieldMask::Include | WriteFieldMask::TentativelyInclude
        )
    }
}

struct WriteQueryDataValueTree {
    schema: Rc<SchemaData>,
    field_mask: Vec<WriteFieldMask>,
    field_data: Vec<Option<WriteFieldData>>,
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
                (f.name.clone(), self.field_data[i].clone().unwrap())
            })
            .collect::<Vec<(String, WriteFieldData)>>();

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
            let attributes_mask: Vec<WriteFieldMask> =
                proptest::collection::vec(
                    prop_oneof![
                        Just(WriteFieldMask::TentativelyInclude),
                        Just(WriteFieldMask::Exclude)
                    ],
                    nattributes..=nattributes,
                )
                .prop_shuffle()
                .new_tree(runner)?
                .current();

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

                            WriteFieldData::from(data)
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

                            WriteFieldData::from(data)
                        }))
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<Option<WriteFieldData>>>();

        Ok(WriteQueryDataValueTree {
            schema: self.schema.clone(),
            field_mask,
            field_data,
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
            let tempdir = TempDir::new().expect("Error creating temp dir");
            let uri = String::from("file:///") + tempdir.path().join("array").to_str().unwrap();

            let schema_in = schema_spec.create(&ctx)
                .expect("Error constructing arbitrary schema");
            Array::create(&ctx, &uri, schema_in)
                .expect("Error creating array");

            let array = Array::open(&ctx, &uri, Mode::Write).expect("Error opening array");

            for write in write_sequence {
                let write = write.attach_write(WriteBuilder::new(array)).expect("Error building write query")
                    .build();
                write.submit()?;
                array = write.finalize()?;
            }
        })
    }
}
