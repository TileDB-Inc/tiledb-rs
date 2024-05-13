use std::fmt::Debug;
use std::rc::Rc;

use proptest::prelude::*;

use crate::array::{ArrayType, SchemaData};
use crate::datatype::LogicalType;
use crate::query::strategy::{Cells, CellsParameters, CellsStrategySchema};
use crate::query::{QueryBuilder, WriteBuilder};
use crate::range::SingleValueRange;
use crate::{fn_typed, single_value_range_go, Result as TileDBResult};

#[derive(Debug)]
pub struct DenseWriteInput {
    pub data: Cells,
    pub subarray: Vec<SingleValueRange>,
}

impl DenseWriteInput {
    pub fn attach_write<'ctx, 'data>(
        &'data self,
        b: WriteBuilder<'ctx, 'data>,
    ) -> TileDBResult<WriteBuilder<'ctx, 'data>> {
        let mut subarray = self.data.attach_write(b)?.start_subarray()?;

        for i in 0..self.subarray.len() {
            subarray = subarray.add_range(i, self.subarray[i].clone())?;
        }

        subarray.finish_subarray()
    }
}

fn prop_dense_write(
    schema: &Rc<SchemaData>,
) -> impl Strategy<Value = DenseWriteInput> {
    /* determine range for each dimension */
    let strat_ranges = schema
        .domain
        .dimension
        .iter()
        .map(|d| {
            let Some(domain) = d.domain.as_ref() else {
                unreachable!()
            };

            fn_typed!(d.datatype, LT, {
                type DT = <LT as LogicalType>::PhysicalType;
                let lower = serde_json::from_value::<DT>(domain[0].clone()).unwrap();
                let upper = serde_json::from_value::<DT>(domain[1].clone()).unwrap();

                (lower..upper)
                    .prop_flat_map(move |upper| ((lower..=upper), Just(upper)))
                    .prop_map(|(lower, upper)| {
                        SingleValueRange::from(&[lower, upper])
                    })
                    .boxed()
            })

            /* TODO: bound each dimension so as to bound total number of cells */
        })
        .collect::<Vec<BoxedStrategy<SingleValueRange>>>();

    let schema = Rc::clone(schema);

    strat_ranges.prop_flat_map(move |ranges| {
        let ncells = ranges
            .iter()
            .map(|r| {
                single_value_range_go!(
                    r,
                    _DT,
                    ref lower,
                    ref upper,
                    (upper - lower) as usize
                )
            })
            .product();

        let params = CellsParameters {
            schema: Some(CellsStrategySchema::WriteSchema(Rc::clone(&schema))),
            min_records: ncells,
            max_records: ncells,
            ..Default::default()
        };

        (Just(ranges), any_with::<Cells>(params)).prop_map(|(ranges, cells)| {
            DenseWriteInput {
                data: cells,
                subarray: ranges,
            }
        })
    })
}

impl Arbitrary for DenseWriteInput {
    type Parameters = Option<Rc<SchemaData>>;
    type Strategy = BoxedStrategy<DenseWriteInput>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        if let Some(schema) = args {
            prop_dense_write(&schema).boxed()
        } else {
            any::<SchemaData>()
                .prop_flat_map(|schema| prop_dense_write(&Rc::new(schema)))
                .boxed()
        }
    }
}

pub struct SparseWriteInput {
    pub data: Cells,
}

#[derive(Debug)]
pub struct DenseWriteSequence {
    writes: Vec<DenseWriteInput>,
}

impl Arbitrary for DenseWriteSequence {
    type Parameters = Option<Rc<SchemaData>>;
    type Strategy = BoxedStrategy<DenseWriteSequence>;

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

impl IntoIterator for DenseWriteSequence {
    type Item = DenseWriteInput;
    type IntoIter = <Vec<Self::Item> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.writes.into_iter()
    }
}

pub fn prop_write_sequence(
    schema: &Rc<SchemaData>,
) -> impl Strategy<Value = DenseWriteSequence> {
    const MAX_WRITES: usize = 8;
    proptest::collection::vec(
        any_with::<DenseWriteInput>(Some(Rc::clone(schema))),
        0..MAX_WRITES,
    )
    .prop_map(|writes| DenseWriteSequence { writes })
}

#[derive(Debug)]
pub enum WriteInput {
    Dense(DenseWriteInput),
}

impl WriteInput {
    pub fn cells(&self) -> &Cells {
        let Self::Dense(ref dense) = self;
        &dense.data
    }

    pub fn unwrap_cells(self) -> Cells {
        let Self::Dense(dense) = self;
        dense.data
    }

    pub fn attach_write<'ctx, 'data>(
        &'data self,
        b: WriteBuilder<'ctx, 'data>,
    ) -> TileDBResult<WriteBuilder<'ctx, 'data>> {
        let Self::Dense(ref d) = self;
        d.attach_write(b)
    }
}

impl Arbitrary for WriteInput {
    type Parameters = Option<Rc<SchemaData>>;
    type Strategy = BoxedStrategy<WriteInput>;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        let strat_schema = |schema: Rc<SchemaData>| match schema.array_type {
            ArrayType::Dense => {
                any_with::<DenseWriteInput>(Some(schema.clone()))
                    .prop_map(WriteInput::Dense)
                    .boxed()
            }
            ArrayType::Sparse => unimplemented!(),
        };

        if let Some(schema) = params {
            strat_schema(schema)
        } else {
            any::<SchemaData>()
                .prop_flat_map(move |schema| strat_schema(Rc::new(schema)))
                .boxed()
        }
    }
}

#[derive(Debug)]
pub enum WriteSequence {
    Dense(DenseWriteSequence),
}

impl From<WriteInput> for WriteSequence {
    fn from(value: WriteInput) -> Self {
        let WriteInput::Dense(dense) = value;
        Self::Dense(DenseWriteSequence {
            writes: vec![dense],
        })
    }
}

impl IntoIterator for WriteSequence {
    type Item = WriteInput;
    type IntoIter = WriteSequenceIter;

    fn into_iter(self) -> Self::IntoIter {
        let Self::Dense(dense) = self;
        WriteSequenceIter::Dense(dense.into_iter())
    }
}

impl Arbitrary for WriteSequence {
    type Parameters = Option<Rc<SchemaData>>;
    type Strategy = BoxedStrategy<WriteSequence>;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        let strat_schema = |schema: Rc<SchemaData>| match schema.array_type {
            ArrayType::Dense => any_with::<DenseWriteSequence>(Some(schema))
                .prop_map(WriteSequence::Dense)
                .boxed(),
            ArrayType::Sparse => unimplemented!(),
        };

        if let Some(schema) = params {
            strat_schema(schema)
        } else {
            any::<SchemaData>()
                .prop_flat_map(move |schema| strat_schema(Rc::new(schema)))
                .boxed()
        }
    }
}

pub enum WriteSequenceIter {
    Dense(<DenseWriteSequence as IntoIterator>::IntoIter),
}

impl Iterator for WriteSequenceIter {
    type Item = WriteInput;

    fn next(&mut self) -> Option<Self::Item> {
        let Self::Dense(ref mut dense) = self;
        dense.next().map(WriteInput::Dense)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tempfile::TempDir;

    use super::*;
    use crate::array::{Array, Mode};
    use crate::query::{
        Query, QueryBuilder, ReadBuilder, ReadQuery, WriteBuilder,
    };
    use crate::typed_field_data_go;
    use crate::{Context, Factory};

    fn do_write_readback(
        ctx: &Context,
        schema_spec: Rc<SchemaData>,
        write_sequence: WriteSequence,
    ) {
        let tempdir = TempDir::new().expect("Error creating temp dir");
        let uri = String::from("file:///")
            + tempdir.path().join("array").to_str().unwrap();

        let schema_in = schema_spec
            .create(ctx)
            .expect("Error constructing arbitrary schema");
        Array::create(ctx, &uri, schema_in).expect("Error creating array");

        let mut array =
            Array::open(ctx, &uri, Mode::Write).expect("Error opening array");

        let mut accumulated_write: Option<Cells> = None;

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

            /* update accumulated expected array data */
            if let Some(acc) = accumulated_write.as_mut() {
                acc.copy_from(write.unwrap_cells())
            } else {
                accumulated_write = Some(write.unwrap_cells());
            }

            let accumulated_write = accumulated_write.as_ref().unwrap();

            /* then read it back */
            {
                let mut cursors = accumulated_write
                    .fields()
                    .keys()
                    .map(|key| (key.clone(), 0))
                    .collect::<HashMap<String, usize>>();

                let mut read = accumulated_write
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
                            let mut ncells = None;
                            for (key, rdata) in raw.iter() {
                                let wdata = &accumulated_write.fields()[key];

                                let nv = if let Some(nv) = ncells {
                                    assert_eq!(nv, rdata.len());
                                    nv
                                } else {
                                    ncells = Some(rdata.len());
                                    rdata.len()
                                };

                                let wdata =
                                    typed_field_data_go!(wdata, wdata, {
                                        FieldData::from(
                                            wdata[cursors[key]
                                                ..cursors[key] + nv]
                                                .to_vec(),
                                        )
                                    });

                                assert_eq!(wdata, *rdata);

                                *cursors.get_mut(key).unwrap() += nv;
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

    /// Test that a single write can be read back correctly
    #[test]
    fn write_once_readback() {
        let ctx = Context::new().expect("Error creating context");

        let strategy = any::<SchemaData>().prop_flat_map(|schema| {
            let schema = Rc::new(schema);
            (
                Just(Rc::clone(&schema)),
                any_with::<WriteInput>(Some(Rc::clone(&schema)))
                    .prop_map(|w| WriteSequence::from(w)),
            )
        });

        proptest!(|((schema_spec, write_sequence) in strategy)| {
            do_write_readback(&ctx, schema_spec, write_sequence)
        })
    }

    /// Test that each write in the sequence can be read back correctly at the right timestamp
    #[test]
    #[ignore]
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
