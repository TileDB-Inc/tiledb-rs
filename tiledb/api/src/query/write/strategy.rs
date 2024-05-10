use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;
use std::rc::Rc;

use proptest::bits::{BitSetLike, VarBitSet};
use proptest::prelude::*;
use proptest::strategy::{NewTree, ValueTree};
use proptest::test_runner::TestRunner;

use crate::array::schema::FieldData as SchemaField;
use crate::array::{ArrayType, CellValNum, SchemaData};
use crate::datatype::{LogicalType, PhysicalType};
use crate::fn_typed;
use crate::query::strategy::{Cells, FieldData};

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

/// Tracks the last step taken for the write shrinking.
enum ShrinkSearchStep {
    /// Remove a range of records
    Explore(usize),
    Recur,
    Done,
}

const WRITE_QUERY_DATA_VALUE_TREE_EXPLORE_PIECES: usize = 8;

/// Value tree to shrink a write query input.
/// For a failing test which writes N records, there are 2^N possible
/// candidate subsets and we want to find the smallest one which fails the test
/// in the shortest number of iterations.
/// That would be ideal but really finding any input that's small enough
/// to be human readable sounds good enough. We divide the record space
/// into WRITE_QUERY_DATA_VALUE_TREE_EXPLORE_PIECES chunks and identify which
/// of those chunks are necessary for the failure.
/// Recur until all of the chunks are necessary for failure, or there
/// is only one record.
///
/// TODO: for var sized attributes, follow up by shrinking the values.
struct WriteQueryDataValueTree {
    field_data: HashMap<String, (WriteFieldMask, Option<FieldData>)>,
    nrecords: usize,
    records_included: Vec<usize>,
    explore_results: [Option<bool>; WRITE_QUERY_DATA_VALUE_TREE_EXPLORE_PIECES],
    search: Option<ShrinkSearchStep>,
}

impl WriteQueryDataValueTree {
    pub fn new(
        field_data: HashMap<String, (WriteFieldMask, Option<FieldData>)>,
    ) -> Self {
        let nrecords = field_data
            .values()
            .filter_map(|&(_, ref f)| f.as_ref())
            .take(1)
            .next()
            .unwrap()
            .len();
        let records_included = (0..nrecords).collect::<Vec<usize>>();

        WriteQueryDataValueTree {
            field_data,
            nrecords,
            records_included,
            explore_results: [None; WRITE_QUERY_DATA_VALUE_TREE_EXPLORE_PIECES],
            search: None,
        }
    }

    fn explore_step(&mut self, failed: bool) -> bool {
        match self.search {
            None => {
                if failed && self.nrecords > 0 {
                    /* failed on the whole input, begin the search */
                    self.search = Some(ShrinkSearchStep::Explore(0));
                    true
                } else {
                    /* passed on the whole input, nothing to do */
                    false
                }
            }
            Some(ShrinkSearchStep::Explore(c)) => {
                let nchunks = std::cmp::min(
                    self.records_included.len(),
                    WRITE_QUERY_DATA_VALUE_TREE_EXPLORE_PIECES,
                );

                self.explore_results[c] = Some(failed);

                match (c + 1).cmp(&nchunks) {
                    Ordering::Less => {
                        self.search = Some(ShrinkSearchStep::Explore(c + 1));
                        true
                    }
                    Ordering::Equal => {
                        /* finished exploring at this level, either recur or finish */
                        let approx_chunk_len =
                            self.records_included.len() / nchunks;
                        let mut new_records_included = vec![];
                        for i in 0..nchunks {
                            let chunk_min = i * approx_chunk_len;
                            let chunk_max = if i + 1 == nchunks {
                                self.records_included.len()
                            } else {
                                (i + 1) * approx_chunk_len
                            };

                            if !self.explore_results[i].take().unwrap() {
                                /* the test passed when chunk `i` was not included; keep it */
                                new_records_included.extend_from_slice(
                                    &self.records_included
                                        [chunk_min..chunk_max],
                                );
                            }
                        }

                        if new_records_included == self.records_included {
                            /* everything was needed to pass */
                            self.search = Some(ShrinkSearchStep::Done);
                        } else {
                            self.records_included = new_records_included;
                            self.search = Some(ShrinkSearchStep::Recur);
                        }
                        /* run another round on the updated input */
                        true
                    }
                    Ordering::Greater => {
                        assert_eq!(0, nchunks);
                        false
                    }
                }
            }
            Some(ShrinkSearchStep::Recur) => {
                /* we must have failed unless the test itself is non-deterministic */
                assert!(failed);

                self.search = Some(ShrinkSearchStep::Explore(0));
                true
            }
            Some(ShrinkSearchStep::Done) => false,
        }
    }
}

impl ValueTree for WriteQueryDataValueTree {
    type Value = Cells;

    fn current(&self) -> Self::Value {
        let record_mask = match self.search {
            None => VarBitSet::saturated(self.nrecords),
            Some(ShrinkSearchStep::Explore(c)) => {
                let nchunks = self
                    .records_included
                    .len()
                    .clamp(1, WRITE_QUERY_DATA_VALUE_TREE_EXPLORE_PIECES);

                let approx_chunk_len = self.records_included.len() / nchunks;

                if approx_chunk_len == 0 {
                    /* no records are included, we have shrunk down to empty */
                    VarBitSet::new_bitset(self.nrecords)
                } else {
                    let mut record_mask = VarBitSet::new_bitset(self.nrecords);

                    let exclude_min = c * approx_chunk_len;
                    let exclude_max = if c + 1 == nchunks {
                        self.records_included.len()
                    } else {
                        (c + 1) * approx_chunk_len
                    };

                    for r in self.records_included[0..exclude_min]
                        .iter()
                        .chain(self.records_included[exclude_max..].iter())
                    {
                        record_mask.set(*r)
                    }

                    record_mask
                }
            }
            Some(ShrinkSearchStep::Recur) | Some(ShrinkSearchStep::Done) => {
                let mut record_mask = VarBitSet::new_bitset(self.nrecords);
                for r in self.records_included.iter() {
                    record_mask.set(*r);
                }
                record_mask
            }
        };

        let fields = self
            .field_data
            .iter()
            .filter(|(_, &(mask, _))| mask.is_included())
            .map(|(name, &(_, ref data))| {
                (name.clone(), data.as_ref().unwrap().filter(&record_mask))
            })
            .collect::<HashMap<String, FieldData>>();

        Cells { fields }
    }

    fn simplify(&mut self) -> bool {
        self.explore_step(true)
    }

    fn complicate(&mut self) -> bool {
        self.explore_step(false)
    }
}

#[derive(Clone, Debug)]
pub struct WriteQueryDataParameters {
    pub schema: Option<Rc<SchemaData>>,
    pub min_records: usize,
    pub max_records: usize,
    pub value_min_var_size: usize,
    pub value_max_var_size: usize,
}

impl Default for WriteQueryDataParameters {
    fn default() -> Self {
        const WRITE_QUERY_MIN_RECORDS: usize = 0;
        const WRITE_QUERY_MAX_RECORDS: usize = 1024 * 1024;

        const WRITE_QUERY_MIN_VAR_SIZE: usize = 0;
        const WRITE_QUERY_MAX_VAR_SIZE: usize = 1024 * 128;

        WriteQueryDataParameters {
            schema: None,
            min_records: WRITE_QUERY_MIN_RECORDS,
            max_records: WRITE_QUERY_MAX_RECORDS,
            value_min_var_size: WRITE_QUERY_MIN_VAR_SIZE,
            value_max_var_size: WRITE_QUERY_MAX_VAR_SIZE,
        }
    }
}

#[derive(Debug)]
struct WriteQueryDataStrategy {
    schema: Rc<SchemaData>,
    params: WriteQueryDataParameters,
}

impl WriteQueryDataStrategy {
    pub fn new(
        schema: &Rc<SchemaData>,
        params: WriteQueryDataParameters,
    ) -> Self {
        WriteQueryDataStrategy {
            schema: Rc::clone(schema),
            params,
        }
    }
}

fn new_write_field_impl<DT: Arbitrary + PhysicalType>(
    runner: &mut TestRunner,
    params: &WriteQueryDataParameters,
    field: &SchemaField,
    nrecords: usize,
) -> FieldData
where
    FieldData: From<Vec<DT>> + From<Vec<Vec<DT>>>,
    std::ops::Range<DT>: Strategy<Value = DT>,
{
    let value_strat = match field {
        SchemaField::Dimension(d) => {
            if let Some(domain) = d.domain.as_ref() {
                let lower_bound =
                    serde_json::from_value::<DT>(domain[0].clone()).unwrap();
                let upper_bound =
                    serde_json::from_value::<DT>(domain[1].clone()).unwrap();
                (lower_bound..upper_bound).boxed()
            } else {
                any::<DT>().boxed()
            }
        }
        SchemaField::Attribute(_) => any::<DT>().boxed(),
    };

    let cell_val_num = field.cell_val_num().unwrap_or(CellValNum::single());
    if cell_val_num == 1u32 {
        let data = proptest::collection::vec(value_strat, nrecords..=nrecords)
            .new_tree(runner)
            .expect("Error generating query data")
            .current();

        FieldData::from(data)
    } else {
        let (min, max) = if cell_val_num.is_var_sized() {
            (params.value_min_var_size, params.value_max_var_size)
        } else {
            let fixed_bound = Into::<u32>::into(cell_val_num) as usize;
            (fixed_bound, fixed_bound)
        };

        let data = proptest::collection::vec(
            proptest::collection::vec(value_strat, min..=max),
            nrecords..=nrecords,
        )
        .new_tree(runner)
        .expect("Error generating query data")
        .current();

        FieldData::from(data)
    }
}

fn new_write_field(
    runner: &mut TestRunner,
    params: &WriteQueryDataParameters,
    field: &SchemaField,
    nrecords: usize,
) -> FieldData {
    fn_typed!(field.datatype(), LT, {
        type DT = <LT as LogicalType>::PhysicalType;
        new_write_field_impl::<DT>(runner, params, field, nrecords)
    })
}

impl Strategy for WriteQueryDataStrategy {
    type Tree = WriteQueryDataValueTree;
    type Value = Cells;

    fn new_tree(&self, runner: &mut TestRunner) -> NewTree<Self> {
        /* Choose the maximum number of records */
        let nrecords = (self.params.min_records..=self.params.max_records)
            .new_tree(runner)?
            .current();

        /* generate an initial set of fields to write */
        let field_mask = {
            use crate::array::schema::FieldData;

            let dimensions_mask = {
                let mask = match self.schema.array_type {
                    ArrayType::Dense => {
                        /* dense array coordinates are handled by a subarray */
                        WriteFieldMask::Exclude
                    }
                    ArrayType::Sparse => {
                        /* sparse array must write coordinates */
                        WriteFieldMask::Include
                    }
                };
                self.schema
                    .domain
                    .dimension
                    .iter()
                    .map(|d| (FieldData::from(d.clone()), mask))
                    .collect::<Vec<(FieldData, WriteFieldMask)>>()
            };

            /* as of this writing, write queries must write to all attributes */
            let attributes_mask = self
                .schema
                .attributes
                .iter()
                .map(|a| (FieldData::from(a.clone()), WriteFieldMask::Include))
                .collect::<Vec<(FieldData, WriteFieldMask)>>();

            dimensions_mask
                .into_iter()
                .chain(attributes_mask)
                .collect::<Vec<(FieldData, WriteFieldMask)>>()
        };

        let field_data = field_mask
            .into_iter()
            .map(|(field, mask)| {
                let field_data = if mask.is_included() {
                    Some(new_write_field(
                        runner,
                        &self.params,
                        &field,
                        nrecords,
                    ))
                } else {
                    None
                };
                (field.name().to_string(), (mask, field_data))
            })
            .collect::<HashMap<String, (WriteFieldMask, Option<FieldData>)>>();

        Ok(WriteQueryDataValueTree::new(field_data))
    }
}

impl Arbitrary for Cells {
    type Parameters = WriteQueryDataParameters;
    type Strategy = BoxedStrategy<Cells>;

    fn arbitrary_with(mut args: Self::Parameters) -> Self::Strategy {
        if let Some(schema) = args.schema.take() {
            WriteQueryDataStrategy::new(&schema, args).boxed()
        } else {
            any::<SchemaData>()
                .prop_flat_map(move |schema| {
                    WriteQueryDataStrategy::new(&Rc::new(schema), args.clone())
                })
                .boxed()
        }
    }
}

#[derive(Debug)]
pub struct WriteSequence {
    writes: Vec<Cells>,
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
    type Item = Cells;
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
        any_with::<Cells>(WriteQueryDataParameters {
            schema: Some(Rc::clone(schema)),
            ..Default::default()
        }),
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
            .create(ctx)
            .expect("Error constructing arbitrary schema");
        Array::create(ctx, &uri, schema_in).expect("Error creating array");

        let mut array =
            Array::open(ctx, &uri, Mode::Write).expect("Error opening array");

        let mut accumulated_write: Option<WriteQueryData> = None;

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
                acc.copy_from(write)
            } else {
                accumulated_write = Some(write);
            }

            let accumulated_write = accumulated_write.as_ref().unwrap();

            /* then read it back */
            {
                let mut cursors = accumulated_write
                    .fields
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
                                let wdata = &accumulated_write.fields[key];

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
