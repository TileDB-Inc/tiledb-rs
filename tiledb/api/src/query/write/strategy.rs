use std::cmp::Ordering;
use std::fmt::Debug;
use std::rc::Rc;

use proptest::prelude::*;

use crate::array::{ArrayType, CellOrder, CellValNum, SchemaData};
use crate::datatype::physical::BitsOrd;
use crate::query::strategy::{
    Cells, CellsParameters, CellsStrategySchema, FieldDataParameters,
};
use crate::query::{QueryBuilder, WriteBuilder};
use crate::range::SingleValueRange;
use crate::{
    dimension_constraints_go, single_value_range_go, Result as TileDBResult,
};

#[derive(Debug)]
pub struct DenseWriteInput {
    pub layout: CellOrder,
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

        subarray.finish_subarray()?.layout(self.layout)
    }
}

#[derive(Clone, Debug, Default)]
pub struct DenseWriteParameters {
    schema: Option<Rc<SchemaData>>,
    layout: Option<CellOrder>,
    memory_limit: Option<usize>,
}

fn prop_dense_write(
    schema: Rc<SchemaData>,
    layout: CellOrder,
    params: DenseWriteParameters,
) -> impl Strategy<Value = DenseWriteInput> {
    /*
     * For simplicity, we will bound the memory used at each dimension
     * rather than keeping a moving product of the accumulated memory
     */
    let memory_limit: usize = {
        const MEMORY_LIMIT_DEFAULT: usize = 1 * 1024; // chosen arbitrarily
        let memory_limit = params.memory_limit.unwrap_or(MEMORY_LIMIT_DEFAULT);
        memory_limit / schema.domain.dimension.len()
    };

    if matches!(layout, CellOrder::Global) {
        // necessary to align to tile boundaries
        unimplemented!()
    }

    let est_cell_size: usize = schema
        .fields()
        .map(|field| {
            match field.cell_val_num().unwrap_or(CellValNum::single()) {
                CellValNum::Fixed(nz) => {
                    /* exact */
                    nz.get() as usize * field.datatype().size() as usize
                }
                CellValNum::Var => {
                    /* estimate */
                    let params = <FieldDataParameters as Default>::default();
                    let est_nvalues = (params.value_min_var_size
                        + params.value_max_var_size)
                        / 2;
                    est_nvalues * field.datatype().size() as usize
                }
            }
        })
        .sum();

    let cell_limit: usize = memory_limit / est_cell_size;

    /* determine range for each dimension */
    let strat_ranges = schema
        .domain
        .dimension
        .iter()
        .map(|d| {
            dimension_constraints_go!(
                d.constraints,
                DT,
                ref domain,
                _,
                {
                    let dim_lower = domain[0]; // copy so we don't borrow schema for closure
                    let dim_range = domain[1] - dim_lower;

                    let lower_cell_bound = 0 as DT;
                    let upper_cell_bound =
                        match dim_range.bits_cmp(&(cell_limit as DT)) {
                            Ordering::Less => dim_range,
                            _ => cell_limit as DT,
                        };

                    (lower_cell_bound..upper_cell_bound)
                        .prop_flat_map(move |upper| {
                            ((lower_cell_bound..=upper), Just(upper))
                        })
                        .prop_map(move |(lower, upper)| {
                            SingleValueRange::from(&[
                                dim_lower + lower,
                                dim_lower + upper,
                            ])
                        })
                        .boxed()
                },
                unimplemented!()
            )
        })
        .collect::<Vec<BoxedStrategy<SingleValueRange>>>();

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

        (Just(ranges), any_with::<Cells>(params)).prop_map(
            move |(ranges, cells)| DenseWriteInput {
                layout,
                data: cells,
                subarray: ranges,
            },
        )
    })
}

impl Arbitrary for DenseWriteInput {
    type Parameters = DenseWriteParameters;
    type Strategy = BoxedStrategy<DenseWriteInput>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        let mut args = args;
        let strat_schema = match args.schema.take() {
            None => {
                let schema_req = crate::array::schema::strategy::Requirements {
                    array_type: Some(ArrayType::Dense),
                };
                any_with::<SchemaData>(Rc::new(schema_req))
                    .prop_map(Rc::new)
                    .boxed()
            }
            Some(schema) => Just(schema).boxed(),
        };
        let strat_layout = match args.layout.take() {
            None => prop_oneof![
                Just(CellOrder::RowMajor),
                Just(CellOrder::ColumnMajor),
                /* TODO: CellOrder::Global is possible but has more constraints */
            ].boxed(),
            Some(layout) => Just(layout).boxed()
        };

        (strat_schema, strat_layout)
            .prop_flat_map(move |(schema, layout)| {
                prop_dense_write(schema, layout, args.clone())
            })
            .boxed()
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
    type Parameters = DenseWriteParameters;
    type Strategy = BoxedStrategy<DenseWriteSequence>;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        if let Some(schema) = params.schema.as_ref() {
            prop_write_sequence(Rc::clone(schema), params).boxed()
        } else {
            any::<SchemaData>()
                .prop_flat_map(move |schema| {
                    prop_write_sequence(Rc::new(schema), params.clone())
                })
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
    schema: Rc<SchemaData>,
    params: DenseWriteParameters,
) -> impl Strategy<Value = DenseWriteSequence> {
    let params = DenseWriteParameters {
        schema: Some(schema),
        ..params
    };

    const MAX_WRITES: usize = 8;
    proptest::collection::vec(
        any_with::<DenseWriteInput>(params),
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

#[derive(Debug)]
pub enum WriteParameters {
    Dense(DenseWriteParameters),
}

impl WriteParameters {
    pub fn default_for(schema: Rc<SchemaData>) -> Self {
        match schema.array_type {
            ArrayType::Dense => Self::Dense(DenseWriteParameters {
                schema: Some(schema),
                ..Default::default()
            }),
            ArrayType::Sparse => unimplemented!(),
        }
    }
}

impl Default for WriteParameters {
    fn default() -> Self {
        Self::Dense(DenseWriteParameters::default())
    }
}

impl Arbitrary for WriteInput {
    type Parameters = WriteParameters;
    type Strategy = BoxedStrategy<WriteInput>;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        let WriteParameters::Dense(d) = params;
        any_with::<DenseWriteInput>(d)
            .prop_map(WriteInput::Dense)
            .boxed()
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
            ArrayType::Dense => {
                let params = DenseWriteParameters {
                    schema: Some(schema),
                    ..Default::default()
                };

                any_with::<DenseWriteSequence>(params)
                    .prop_map(WriteSequence::Dense)
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

            array = Array::open(ctx, array.uri(), Mode::Read).unwrap();

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
                any_with::<WriteInput>(WriteParameters::default_for(schema))
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
