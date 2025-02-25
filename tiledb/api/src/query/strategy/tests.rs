use cells::write::strategy::{WriteParameters, WriteSequenceParameters};
use cells::write::{DenseWriteInput, SparseWriteInput, WriteSequence};
use proptest::prelude::*;
use tiledb_common::array::schema::EnumerationKey;
use tiledb_common::query::condition::strategy::{
    Parameters as QueryConditionParameters, QueryConditionField,
    QueryConditionSchema,
};
use tiledb_common::query::condition::{
    EqualityOp, Field as ASTField, SetMembers,
};
use tiledb_common::range::{NonEmptyDomain, Range, VarValueRange};
use tiledb_common::Datatype;
use tiledb_pod::array::schema::{FieldData as SchemaField, SchemaData};
use uri::TestArrayUri;

use super::*;
use crate::array::{Array, ArrayOpener, Mode};
use crate::error::Error;
use crate::query::{Query, QueryBuilder, ReadBuilder, ReadQuery, WriteBuilder};
use crate::{Context, Factory};

#[derive(Debug)]
struct DenseCellsAccumulator {
    // TODO: implement accepting more than one write for dense write sequence
    write: Option<DenseWriteInput>,
}

impl DenseCellsAccumulator {
    pub fn new(_: &SchemaData) -> Self {
        DenseCellsAccumulator { write: None }
    }

    pub fn cells(&self) -> &Cells {
        // will not be called until first cells are written
        &self.write.as_ref().unwrap().data
    }

    pub fn accumulate(&mut self, write: DenseWriteInput) {
        if self.write.is_some() {
            unimplemented!()
        }
        self.write = Some(write)
    }

    pub fn attach_read<'data, B>(
        &'data self,
        b: B,
    ) -> TileDBResult<
        CallbackVarArgReadBuilder<
            'data,
            MapAdapter<CellsConstructor, RawResultCallback>,
            B,
        >,
    >
    where
        B: ReadQueryBuilder<'data>,
    {
        // TODO: this is not correct as we accumulate multiple writes
        self.write.as_ref().unwrap().attach_read(b)
    }
}

#[derive(Debug)]
struct SparseCellsAccumulator {
    cells: Option<Cells>,
    dedup_keys: Option<Vec<String>>,
}

impl SparseCellsAccumulator {
    pub fn new(schema: &SchemaData) -> Self {
        let dedup_keys = if schema.allow_duplicates.unwrap_or(false) {
            None
        } else {
            Some(
                schema
                    .domain
                    .dimension
                    .iter()
                    .map(|d| d.name.clone())
                    .collect::<Vec<String>>(),
            )
        };
        SparseCellsAccumulator {
            cells: None,
            dedup_keys,
        }
    }

    pub fn cells(&self) -> &Cells {
        // will not be called until first cells arrive
        self.cells.as_ref().unwrap()
    }

    /// Update state representing what we expect to see in the array.
    /// For a sparse array this means adding this write's coordinates,
    /// overwriting the old coordinates if they overlap.
    pub fn accumulate(&mut self, mut write: SparseWriteInput) {
        if let Some(cells) = self.cells.take() {
            write.data.extend(cells);
            if let Some(dedup_keys) = self.dedup_keys.as_ref() {
                self.cells = Some(write.data.dedup(dedup_keys));
            } else {
                self.cells = Some(write.data);
            }
        } else {
            self.cells = Some(write.data);
        }
    }

    pub fn attach_read<'data, B>(
        &'data self,
        b: B,
    ) -> TileDBResult<
        CallbackVarArgReadBuilder<
            'data,
            MapAdapter<CellsConstructor, RawResultCallback>,
            B,
        >,
    >
    where
        B: ReadQueryBuilder<'data>,
    {
        Ok(self.cells().attach_read(b)?.map(CellsConstructor::new()))
    }
}

#[derive(Debug)]
enum CellsAccumulator {
    Dense(DenseCellsAccumulator),
    Sparse(SparseCellsAccumulator),
}

impl CellsAccumulator {
    pub fn new(schema: &SchemaData) -> Self {
        match schema.array_type {
            ArrayType::Dense => Self::Dense(DenseCellsAccumulator::new(schema)),
            ArrayType::Sparse => {
                Self::Sparse(SparseCellsAccumulator::new(schema))
            }
        }
    }

    pub fn cells(&self) -> &Cells {
        match self {
            Self::Dense(ref d) => d.cells(),
            Self::Sparse(ref s) => s.cells(),
        }
    }

    pub fn accumulate(&mut self, write: WriteInput) {
        match write {
            WriteInput::Sparse(w) => {
                let Self::Sparse(ref mut sparse) = self else {
                    unreachable!()
                };
                sparse.accumulate(w)
            }
            WriteInput::Dense(w) => {
                let Self::Dense(ref mut dense) = self else {
                    unreachable!()
                };
                dense.accumulate(w)
            }
        }
    }

    pub fn attach_read<'data, B>(
        &'data self,
        b: B,
    ) -> TileDBResult<
        CallbackVarArgReadBuilder<
            'data,
            MapAdapter<CellsConstructor, RawResultCallback>,
            B,
        >,
    >
    where
        B: ReadQueryBuilder<'data>,
    {
        match self {
            Self::Dense(ref d) => d.attach_read(b),
            Self::Sparse(ref s) => s.attach_read(b),
        }
    }
}

fn do_write_readback(
    ctx: &Context,
    schema_spec: Rc<SchemaData>,
    write_sequence: WriteSequence,
) -> TileDBResult<()> {
    let test_uri =
        uri::get_uri_generator().map_err(|e| Error::Other(e.to_string()))?;
    let uri = test_uri
        .with_path("array")
        .map_err(|e| Error::Other(e.to_string()))?;

    let schema_in = schema_spec
        .create(ctx)
        .expect("Error constructing arbitrary schema");
    Array::create(ctx, &uri, schema_in).expect("Error creating array");

    let mut accumulated_domain: Option<NonEmptyDomain> = None;
    let mut accumulated_write = CellsAccumulator::new(&schema_spec);

    /*
     * Results do not come back in a defined order, so we must sort and
     * compare. Writes currently have to write all fields.
     */
    let sort_keys = match write_sequence {
        WriteSequence::Dense(_) => schema_spec
            .attributes
            .iter()
            .map(|f| f.name.clone())
            .collect::<Vec<String>>(),
        WriteSequence::Sparse(_) => schema_spec
            .fields()
            .map(|f| f.name().to_owned())
            .collect::<Vec<String>>(),
    };

    for write in write_sequence {
        /* write data and preserve ranges for sanity check */
        let write_ranges = {
            let array = Array::open(ctx, &uri, Mode::Write)
                .expect("Error opening array");

            let write_query = write
                .attach_write(
                    WriteBuilder::new(array)
                        .expect("Error building write query"),
                )
                .expect("Error building write query")
                .build();
            write_query.submit().expect("Error running write query");

            let write_ranges = if let Some(ranges) = write.subarray() {
                let generic_ranges = ranges
                    .iter()
                    .cloned()
                    .map(|r| vec![r])
                    .collect::<Vec<Vec<Range>>>();
                assert_eq!(
                    generic_ranges,
                    write_query.subarray().unwrap().ranges().unwrap()
                );
                Some(generic_ranges)
            } else {
                None
            };

            let _ = write_query
                .finalize()
                .expect("Error finalizing write query");

            write_ranges
        };

        if write.cells().is_empty() {
            // in this case, writing and finalizing does not create a new fragment
            // TODO
            continue;
        }

        /* NB: results are not read back in a defined order, so we must sort and compare */

        let mut array = ArrayOpener::new(ctx, &uri, Mode::Read)
            .unwrap()
            .open()
            .unwrap();

        /*
         * First check fragment - its domain should match what we just wrote, and we need the
         * timestamp so we can read back only this fragment
         */
        let [timestamp_min, timestamp_max] = {
            let fi = array.fragment_info().unwrap();
            let nf = fi.num_fragments().unwrap();
            assert!(nf > 0);

            let this_fragment = fi.get_fragment(nf - 1).unwrap();

            if let Some(write_domain) = write.domain() {
                let nonempty_domain =
                    this_fragment.non_empty_domain().unwrap().untyped();
                assert_eq!(write_domain, nonempty_domain);
            } else {
                // most recent fragment should be empty,
                // what does that look like if no data was written?
            }

            this_fragment.timestamp_range().unwrap()
        };

        let safety_write_start = std::time::Instant::now();

        /*
         * Then re-open the array to read back what we just wrote
         * into the most recent fragment only
         */
        {
            array = array
                .reopen()
                .start_timestamp(timestamp_min)
                .unwrap()
                .end_timestamp(timestamp_max)
                .unwrap()
                .open()
                .unwrap();

            let mut read = write
                .attach_read(ReadBuilder::new(array).unwrap())
                .unwrap()
                .build();

            if let Some(write_ranges) = write_ranges {
                let read_ranges = read.subarray().unwrap().ranges().unwrap();
                assert_eq!(write_ranges, read_ranges);
            }

            let (mut cells, _) = read.execute().unwrap();

            /* `cells` should match the write */
            {
                let write_sorted = write.cells().sorted(&sort_keys);
                cells.sort(&sort_keys);
                assert_eq!(write_sorted, cells);
            }

            array = read.finalize().unwrap();
        }

        /* finally, check that everything written up until now is correct */
        array = array.reopen().start_timestamp(0).unwrap().open().unwrap();

        /* check array non-empty domain */
        if let Some(accumulated_domain) = accumulated_domain.as_mut() {
            let Some(write_domain) = write.domain() else {
                unreachable!()
            };
            *accumulated_domain = accumulated_domain.union(&write_domain);
        } else {
            accumulated_domain = write.domain();
        }
        {
            let Some(acc) = accumulated_domain.as_ref() else {
                unreachable!()
            };
            let nonempty = array.nonempty_domain().unwrap().unwrap().untyped();
            assert_eq!(*acc, nonempty);
        }

        /* update accumulated expected array data */
        accumulated_write.accumulate(write);
        {
            let acc = accumulated_write.cells().sorted(&sort_keys);

            let cells = {
                let mut read = accumulated_write
                    .attach_read(ReadBuilder::new(array).unwrap())
                    .unwrap()
                    .build();

                let (mut cells, _) = read.execute().unwrap();
                cells.sort(&sort_keys);
                cells
            };

            assert_eq!(acc, cells);
        }

        // safety valve to ensure we don't write two fragments in the same millisecond
        if safety_write_start.elapsed() < std::time::Duration::from_millis(1) {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    Ok(())
}

/// Test that a single write can be read back correctly
#[test]
fn write_once_readback() -> TileDBResult<()> {
    let ctx = Context::new().expect("Error creating context");

    let schema_req = query_write_schema_requirements(None);

    let strategy =
        any_with::<SchemaData>(Rc::new(schema_req)).prop_flat_map(|schema| {
            let schema = Rc::new(schema);
            (
                Just(Rc::clone(&schema)),
                any_with::<WriteInput>(WriteParameters::default_for(schema))
                    .prop_map(WriteSequence::from),
            )
        });

    proptest!(|((schema_spec, write_sequence) in strategy)| {
        do_write_readback(&ctx, schema_spec, write_sequence)?;
    });

    Ok(())
}

/// Test that each write in the sequence can be read back correctly at the right timestamp
#[test]
fn write_sequence_readback() -> TileDBResult<()> {
    let ctx = Context::new().expect("Error creating context");

    let schema_req = query_write_schema_requirements(Some(ArrayType::Sparse));

    let strategy =
        any_with::<SchemaData>(Rc::new(schema_req)).prop_flat_map(|schema| {
            let schema = Rc::new(schema);
            (
                Just(Rc::clone(&schema)),
                any_with::<WriteSequence>(
                    WriteSequenceParameters::default_for(Rc::clone(&schema)),
                ),
            )
        });

    proptest!(|((schema_spec, write_sequence) in strategy)| {
        do_write_readback(&ctx, schema_spec, write_sequence)?;
    });

    Ok(())
}

fn instance_query_condition(
    schema_spec: Rc<SchemaData>,
    write_sequence: Rc<WriteSequence>,
    accumulated_write: Rc<CellsAccumulator>,
    qc: Vec<QueryConditionExpr>,
) -> anyhow::Result<()> {
    let ctx = Context::new()?;

    let test_uri =
        uri::get_uri_generator().map_err(|e| Error::Other(e.to_string()))?;
    let uri = test_uri
        .with_path("array")
        .map_err(|e| Error::Other(e.to_string()))?;

    let schema_in = schema_spec.create(&ctx)?;
    Array::create(&ctx, &uri, schema_in)?;

    for write in write_sequence.iter() {
        let array = Array::open(&ctx, &uri, Mode::Write)?;

        let write_query =
            write.attach_write(WriteBuilder::new(array)?)?.build();
        write_query.submit()?;

        let _ = write_query.finalize()?;
    }

    // Results do not come back in a defined order, so we must sort and
    // compare. Writes currently have to write all fields.
    let sort_keys = match write_sequence.as_ref() {
        WriteSequence::Dense(_) => schema_spec
            .attributes
            .iter()
            .map(|f| f.name.clone())
            .collect::<Vec<String>>(),
        WriteSequence::Sparse(_) => schema_spec
            .fields()
            .map(|f| f.name().to_owned())
            .collect::<Vec<String>>(),
    };
    let expect_cells_unfiltered = accumulated_write.cells().sorted(&sort_keys);

    let mut a = Array::open(&ctx, &uri, Mode::Read)?;

    for qc in qc.into_iter() {
        let expect_cells = expect_cells_unfiltered.query_condition(&qc);

        let mut rq = accumulated_write
            .attach_read(ReadBuilder::new(a)?.query_condition(qc)?)?
            .build();
        let read_cells = {
            let (mut c, _) = rq.execute()?;
            c.sort(&sort_keys);
            c
        };

        assert_eq!(expect_cells, read_cells);

        a = rq.finalize()?;
    }

    Ok(())
}

fn strat_query_condition_value_datatype(
) -> impl Strategy<Value = (Datatype, CellValNum)> {
    let valid = vec![
        (Datatype::Char, CellValNum::Var),
        (Datatype::StringAscii, CellValNum::Var),
        (Datatype::StringUtf8, CellValNum::Var),
        (Datatype::UInt8, CellValNum::single()),
        (Datatype::UInt16, CellValNum::single()),
        (Datatype::UInt32, CellValNum::single()),
        (Datatype::UInt64, CellValNum::single()),
        (Datatype::Int8, CellValNum::single()),
        (Datatype::Int16, CellValNum::single()),
        (Datatype::Int32, CellValNum::single()),
        (Datatype::Int64, CellValNum::single()),
        (Datatype::Float32, CellValNum::single()),
        (Datatype::Float64, CellValNum::single()),
        (Datatype::UInt8, CellValNum::single()),
        (Datatype::DateTimeYear, CellValNum::single()),
        (Datatype::DateTimeMonth, CellValNum::single()),
        (Datatype::DateTimeWeek, CellValNum::single()),
        (Datatype::DateTimeDay, CellValNum::single()),
        (Datatype::DateTimeHour, CellValNum::single()),
        (Datatype::DateTimeMinute, CellValNum::single()),
        (Datatype::DateTimeSecond, CellValNum::single()),
        (Datatype::DateTimeMillisecond, CellValNum::single()),
        (Datatype::DateTimeMicrosecond, CellValNum::single()),
        (Datatype::DateTimeNanosecond, CellValNum::single()),
        (Datatype::DateTimePicosecond, CellValNum::single()),
        (Datatype::DateTimeFemtosecond, CellValNum::single()),
        (Datatype::DateTimeAttosecond, CellValNum::single()),
        (Datatype::TimeHour, CellValNum::single()),
        (Datatype::TimeMinute, CellValNum::single()),
        (Datatype::TimeSecond, CellValNum::single()),
        (Datatype::TimeMillisecond, CellValNum::single()),
        (Datatype::TimeMicrosecond, CellValNum::single()),
        (Datatype::TimeNanosecond, CellValNum::single()),
        (Datatype::TimePicosecond, CellValNum::single()),
        (Datatype::TimeFemtosecond, CellValNum::single()),
        (Datatype::TimeAttosecond, CellValNum::single()),
    ];
    proptest::strategy::Union::new(
        valid.into_iter().map(|datatype| Just(datatype)),
    )
    .boxed()
}

struct SchemaWithDomain {
    fields: Vec<FieldWithDomain>,
}

impl SchemaWithDomain {
    pub fn new(schema: Rc<SchemaData>, cells: &Cells) -> Self {
        Self {
            fields: cells
                .domain()
                .into_iter()
                .map(|(f, domain)| FieldWithDomain {
                    schema: Rc::clone(&schema),
                    field: schema.field(f).unwrap(),
                    domain,
                })
                .collect::<Vec<_>>(),
        }
    }
}

struct FieldWithDomain {
    schema: Rc<SchemaData>,
    field: SchemaField,
    domain: Option<Range>,
}

impl QueryConditionSchema for SchemaWithDomain {
    fn fields(&self) -> Vec<&dyn QueryConditionField> {
        self.fields
            .iter()
            .map(|f| f as &dyn QueryConditionField)
            .collect::<Vec<_>>()
    }
}

impl QueryConditionField for FieldWithDomain {
    fn name(&self) -> &str {
        self.field.name()
    }

    fn equality_ops(&self) -> Option<Vec<EqualityOp>> {
        match self.field {
            SchemaField::Dimension(_) => None,
            SchemaField::Attribute(ref a) => {
                if let Some(edata) = self
                    .schema
                    .enumeration(EnumerationKey::AttributeName(&a.name))
                {
                    if !ASTField::is_allowed_type(
                        edata.datatype,
                        edata.cell_val_num.unwrap_or(CellValNum::single()),
                    ) {
                        // only null test allowed for these
                        Some(vec![])
                    } else if matches!(edata.ordered, Some(true)) {
                        // anything goes
                        None
                    } else {
                        Some(vec![EqualityOp::Equal, EqualityOp::NotEqual])
                    }
                } else {
                    if !ASTField::is_allowed_type(
                        a.datatype,
                        a.cell_val_num.unwrap_or(CellValNum::single()),
                    ) {
                        // only null test allowed for these
                        Some(vec![])
                    } else {
                        None
                    }
                }
            }
        }
    }

    fn domain(&self) -> Option<Range> {
        if matches!(
            self.domain,
            Some(Range::Single(_) | Range::Var(VarValueRange::UInt8(_, _)))
        ) {
            self.domain.clone()
        } else {
            None
        }
    }

    fn set_members(&self) -> Option<SetMembers> {
        match self.field {
            SchemaField::Dimension(_) => None,
            SchemaField::Attribute(ref a) => {
                let Some(edata) = self
                    .schema
                    .enumeration(EnumerationKey::AttributeName(&a.name))
                else {
                    return None;
                };
                let records = edata.records();
                if matches!(
                    edata.datatype,
                    Datatype::StringAscii | Datatype::StringUtf8
                ) && !matches!(
                    edata.cell_val_num,
                    Some(CellValNum::Fixed(_))
                ) {
                    Some(
                        records
                            .into_iter()
                            .map(|v| {
                                String::from_utf8_lossy(v.as_slice())
                                    .into_owned()
                            })
                            .collect::<Vec<String>>()
                            .into(),
                    )
                } else if edata
                    .cell_val_num
                    .map(|c| c.is_single_valued())
                    .unwrap_or(true)
                {
                    physical_type_go!(edata.datatype, DT, {
                        const WIDTH: usize = std::mem::size_of::<DT>();
                        type ByteArray = [u8; WIDTH];
                        Some(SetMembers::from(
                            records
                                .into_iter()
                                .map(|v| {
                                    assert_eq!(WIDTH, v.len());
                                    DT::from_le_bytes(
                                        ByteArray::try_from(v.as_slice())
                                            .unwrap(),
                                    )
                                })
                                .collect::<Vec<_>>(),
                        ))
                    })
                } else {
                    None
                }
            }
        }
    }
}

fn strat_query_condition() -> impl Strategy<
    Value = (
        Rc<SchemaData>,
        Rc<WriteSequence>,
        Rc<CellsAccumulator>,
        Vec<QueryConditionExpr>,
    ),
> {
    let mut schema_req =
        query_write_schema_requirements(Some(ArrayType::Sparse));
    schema_req.attributes.as_mut().unwrap().datatype =
        Some(strat_query_condition_value_datatype().boxed());

    any_with::<SchemaData>(Rc::new(schema_req))
        .prop_flat_map(|schema| {
            let schema = Rc::new(schema);
            (
                Just(Rc::clone(&schema)),
                any_with::<WriteSequence>(
                    WriteSequenceParameters::default_for(Rc::clone(&schema)),
                ),
            )
        })
        .prop_map(|(schema, write_sequence)| {
            let mut acc = CellsAccumulator::new(&schema);
            write_sequence
                .iter()
                .for_each(|w| acc.accumulate(w.cloned()));
            (schema, Rc::new(write_sequence), Rc::new(acc))
        })
        .prop_flat_map(|(schema, write_sequence, acc)| {
            let qc_params = QueryConditionParameters {
                domain: Some(Rc::new(SchemaWithDomain::new(
                    Rc::clone(&schema),
                    acc.cells(),
                ))),
                ..Default::default()
            };
            (
                Just(schema),
                Just(write_sequence),
                Just(acc),
                strategy_ext::records::vec_records_strategy(
                    any_with::<QueryConditionExpr>(qc_params),
                    1..=32,
                ),
            )
        })
}

proptest! {
    #[test]
    fn proptest_query_condition((schema, writes, acc, qcs) in strat_query_condition()) {
        instance_query_condition(schema, writes, acc, qcs).expect("Error in instance_query_condition");
    }
}
