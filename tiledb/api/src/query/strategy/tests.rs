use cells::write::strategy::{WriteParameters, WriteSequenceParameters};
use cells::write::{DenseWriteInput, SparseWriteInput, WriteSequence};
use proptest::prelude::*;
use tiledb_common::array::schema::EnumerationKey;
use tiledb_common::array::{
    CellOrder, TileOrder, dimension::DimensionConstraints,
};
use tiledb_common::datatype::physical::BitsKeyAdapter;
use tiledb_common::metadata::Value;
use tiledb_common::query::condition::strategy::{
    Parameters as QueryConditionParameters, QueryConditionField,
    QueryConditionSchema,
};
use tiledb_common::query::condition::{
    EqualityOp, Field as ASTField, SetMembers,
};
use tiledb_common::range::{
    NonEmptyDomain, Range, SingleValueRange, VarValueRange,
};
use tiledb_common::{Datatype, set_members_go};
use tiledb_pod::array::attribute::{AttributeData, FillData};
use tiledb_pod::array::schema::{FieldData as SchemaField, SchemaData};
use tiledb_pod::array::{DimensionData, DomainData, EnumerationData};
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

    pub fn fold(schema: &SchemaData, seq: &WriteSequence) -> Self {
        let mut acc = Self::new(schema);
        seq.iter().for_each(|w| acc.accumulate(w.cloned()));
        acc
    }

    pub fn cells(&self) -> &Cells {
        match self {
            Self::Dense(d) => d.cells(),
            Self::Sparse(s) => s.cells(),
        }
    }

    pub fn accumulate(&mut self, write: WriteInput) {
        match write {
            WriteInput::Sparse(w) => {
                let Self::Sparse(sparse) = self else {
                    unreachable!()
                };
                sparse.accumulate(w)
            }
            WriteInput::Dense(w) => {
                let Self::Dense(dense) = self else {
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
            Self::Dense(d) => d.attach_read(b),
            Self::Sparse(s) => s.attach_read(b),
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
    let expect_cells_unfiltered = accumulated_write
        .cells()
        .sorted(&sort_keys)
        .with_enumerations(
            schema_spec
                .attributes
                .iter()
                .filter_map(|a| {
                    schema_spec
                        .enumeration(EnumerationKey::AttributeName(&a.name))
                        .zip(Some(a.name.clone()))
                })
                .map(|(enumeration, key)| {
                    (key, FieldData::from(enumeration.clone()))
                })
                .collect::<HashMap<String, FieldData>>(),
        );

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
                } else if !ASTField::is_allowed_type(
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

    fn domain(&self) -> Option<Range> {
        #[allow(clippy::collapsible_if)]
        if let SchemaField::Attribute(ref a) = self.field {
            if let Some(edata) = self
                .schema
                .enumeration(EnumerationKey::AttributeName(&a.name))
            {
                // query condition domain is in terms of the enumerated values,
                // not the attribute values domaion (which are indexes into the enumerated values)
                let members = edata.query_condition_set_members()?;
                return Some(set_members_go!(
                    members,
                    _DT,
                    ref members,
                    {
                        let min = *members.iter().min()?;
                        let max = *members.iter().max()?;
                        Range::Single(SingleValueRange::from(min..=max))
                    },
                    {
                        let min = *members.iter().map(BitsKeyAdapter).min()?.0;
                        let max = *members.iter().map(BitsKeyAdapter).max()?.0;
                        Range::Single(SingleValueRange::from(min..=max))
                    },
                    {
                        let min = members.iter().min()?.clone();
                        let max = members.iter().max()?.clone();
                        Range::Var(VarValueRange::from((
                            min.into_bytes().into_boxed_slice(),
                            max.into_bytes().into_boxed_slice(),
                        )))
                    }
                ));
            }
        }

        // see query_ast.cc
        if matches!(
            self.field.datatype(),
            Datatype::Any
                | Datatype::StringUtf16
                | Datatype::StringUtf32
                | Datatype::StringUcs2
                | Datatype::StringUcs4
                | Datatype::Blob
                | Datatype::GeometryWkb
                | Datatype::GeometryWkt
        ) {
            None
        } else if matches!(self.domain, Some(Range::Single(_)))
            || (matches!(
                self.field.datatype(),
                Datatype::StringAscii | Datatype::StringUtf8
            ) && matches!(
                self.field.cell_val_num(),
                None | Some(CellValNum::Var)
            ))
        {
            self.domain.clone()
        } else {
            None
        }
    }

    fn set_members(&self) -> Option<SetMembers> {
        match self.field {
            SchemaField::Dimension(_) => None,
            SchemaField::Attribute(ref a) => {
                let edata = self
                    .schema
                    .enumeration(EnumerationKey::AttributeName(&a.name))?;
                edata.query_condition_set_members()
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
    let schema_req = query_write_schema_requirements(Some(ArrayType::Sparse));
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
            let acc = CellsAccumulator::fold(&schema, &write_sequence);
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
                proptest::collection::vec(
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

#[test]
fn shrinking_query_condition_1() -> anyhow::Result<()> {
    let schema = SchemaData {
        array_type: ArrayType::Sparse,
        domain: DomainData {
            dimension: vec![DimensionData {
                name: "__9clS_8u_EwY_7X_CUz70_".to_owned(),
                datatype: Datatype::TimePicosecond,
                constraints: DimensionConstraints::Int64(
                    [-1826241097139635319, 3393001123887180702],
                    Some(3633),
                ),
                filters: None,
            }],
        },
        capacity: Some(100000),
        cell_order: Some(CellOrder::ColumnMajor),
        tile_order: Some(TileOrder::RowMajor),
        allow_duplicates: Some(true),
        attributes: vec![
            AttributeData {
                name: "HAR_".to_owned(),
                datatype: Datatype::Int16,
                nullability: Some(true),
                cell_val_num: Some(CellValNum::single()),
                fill: Some(FillData {
                    data: Value::Int16Value(vec![32082]),
                    nullability: Some(true),
                }),
                filters: vec![],
                enumeration: Some("shtH7o__TGyFZ_H36J".to_owned()),
            },
            AttributeData {
                name: "R_0".to_owned(),
                datatype: Datatype::Float32,
                nullability: Some(true),
                cell_val_num: Some(CellValNum::single()),
                fill: Some(FillData {
                    data: Value::Float32Value(vec![-0.00014772698]),
                    nullability: Some(false),
                }),
                filters: vec![],
                enumeration: None,
            },
        ],
        enumerations: vec![EnumerationData {
            name: "shtH7o__TGyFZ_H36J".to_owned(),
            datatype: Datatype::UInt8,
            cell_val_num: Some(CellValNum::single()),
            ordered: Some(true),
            data: vec![
                0, 1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
                19, 20, 21, 22, 23, 24, 26, 27, 28, 29, 30, 32, 33, 34, 35, 36,
                38, 39, 40, 41, 42, 43, 44, 45, 46, 48, 49, 50, 51, 52, 53, 54,
                55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70,
                71, 72, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87,
                88, 89, 90, 91, 92, 93, 95, 96, 97, 98, 99, 100, 101, 103, 104,
                105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116,
                117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128,
                129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140,
                141, 142, 143, 144, 145, 147, 148, 149, 150, 151, 152, 153,
                154, 155, 156, 157, 158, 159, 160, 161, 162, 164, 165, 168,
                170, 171, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182,
                183, 184, 185, 187, 188, 189, 190, 191, 192, 193, 194, 195,
                196, 198, 199, 201, 202, 203, 204, 205, 207, 209, 210, 211,
                212, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224,
                225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236,
                237, 238, 239, 240, 242, 243, 244, 245, 246, 247, 248, 249,
                250, 251, 252, 253, 254, 255,
            ]
            .into_boxed_slice(),
            offsets: None,
        }],
        coordinate_filters: Default::default(),
        offsets_filters: Default::default(),
        validity_filters: Default::default(),
    };
    let writes =
        WriteSequence::Sparse(cells::write::SparseWriteSequence::from(vec![
            SparseWriteInput {
                dimensions: vec![(
                    "__9clS_8u_EwY_7X_CUz70_".to_owned(),
                    CellValNum::single(),
                )],
                data: Cells::new(HashMap::from([
                    ("HAR_".to_owned(), FieldData::Int16(vec![85])),
                    ("R_0".to_owned(), FieldData::Float32(vec![0.0])),
                    (
                        "__9clS_8u_EwY_7X_CUz70_".to_owned(),
                        FieldData::Int64(vec![3208642038087383807]),
                    ),
                ])),
            },
        ]));
    let qc1 = QueryConditionExpr::field("R_0").le(-0f32);
    let qc2 = QueryConditionExpr::field("__9clS_8u_EwY_7X_CUz70_")
        .gt(3208642038087383807i64);
    let qc3 = QueryConditionExpr::field("HAR_").le(186u8);

    let qc = (qc1 | qc2) & qc3;

    let acc = CellsAccumulator::fold(&schema, &writes);
    instance_query_condition(schema.into(), writes.into(), acc.into(), vec![qc])
}

#[test]
fn shrinking_query_condition_2() -> anyhow::Result<()> {
    let schema = SchemaData {
        array_type: ArrayType::Sparse,
        domain: DomainData {
            dimension: vec![DimensionData {
                name: "G_2x0u0nImT_z5__S_LpDF".to_owned(),
                datatype: Datatype::TimeNanosecond,
                constraints: DimensionConstraints::Int64(
                    [-1724306171463955564, 2590083558631104178],
                    Some(4365),
                ),
                filters: None,
            }],
        },
        capacity: Some(100000),
        cell_order: Some(CellOrder::ColumnMajor),
        tile_order: Some(TileOrder::ColumnMajor),
        allow_duplicates: Some(true),
        attributes: vec![
            AttributeData {
                name: "G".to_owned(),
                datatype: Datatype::Int16,
                nullability: Some(false),
                cell_val_num: CellValNum::single().into(),
                fill: None,
                filters: Default::default(),
                enumeration: Some("t55pbZ".to_owned()),
            },
            AttributeData {
                name: "NVjjN97iS_6T0y7XATzd3kCH4".to_owned(),
                datatype: Datatype::StringUtf8,
                nullability: Some(false),
                cell_val_num: CellValNum::Var.into(),
                fill: None,
                filters: Default::default(),
                enumeration: None,
            },
        ],
        enumerations: vec![EnumerationData {
            name: "t55pbZ".to_owned(),
            datatype: Datatype::Int16,
            cell_val_num: CellValNum::single().into(),
            ordered: Some(true),
            data: vec![
                19, 128, 170, 128, 142, 129, 150, 129, 134, 130, 56, 132, 75,
                132, 79, 132, 82, 133, 82, 134, 88, 134, 156, 134, 175, 134,
                238, 134, 54, 135, 72, 135, 88, 135, 171, 135, 177, 135, 164,
                136, 212, 136, 192, 138, 137, 139, 243, 139, 146, 141, 55, 142,
                73, 142, 79, 142, 64, 143, 18, 145, 91, 145, 144, 146, 145,
                146, 243, 146, 130, 147, 124, 148, 247, 148, 1, 149, 62, 149,
                153, 149, 69, 150, 87, 151, 6, 152, 128, 152, 172, 152, 191,
                152, 70, 153, 164, 153, 74, 154, 127, 154, 117, 155, 151, 155,
                213, 156, 217, 157, 105, 159, 107, 159, 140, 159, 222, 159, 68,
                160, 82, 160, 61, 161, 72, 161, 212, 161, 214, 161, 241, 162,
                49, 163, 14, 164, 74, 164, 132, 164, 254, 164, 127, 165, 181,
                165, 199, 165, 151, 166, 36, 167, 37, 167, 112, 167, 126, 167,
                133, 167, 189, 167, 136, 168, 155, 168, 144, 169, 82, 170, 141,
                170, 251, 170, 24, 171, 62, 171, 192, 171, 239, 171, 44, 172,
                232, 172, 10, 173, 93, 173, 28, 174, 109, 174, 134, 174, 120,
                176, 182, 176, 222, 176, 75, 178, 74, 179, 80, 179, 236, 179,
                237, 179, 49, 180, 229, 180, 250, 180, 172, 181, 233, 181, 152,
                182, 71, 183, 35, 184, 188, 184, 134, 186, 206, 186, 213, 186,
                242, 186, 47, 187, 56, 187, 131, 187, 196, 187, 218, 187, 235,
                188, 15, 189, 31, 189, 93, 189, 232, 189, 255, 189, 74, 190,
                81, 190, 157, 190, 143, 191, 171, 191, 220, 191, 31, 192, 232,
                192, 63, 193, 93, 194, 12, 195, 6, 196, 10, 196, 243, 196, 126,
                197, 197, 198, 93, 199, 130, 199, 139, 199, 234, 199, 169, 200,
                176, 200, 198, 200, 52, 201, 138, 201, 159, 201, 220, 201, 70,
                202, 104, 202, 8, 203, 23, 203, 59, 203, 140, 203, 144, 203,
                193, 204, 205, 204, 28, 205, 64, 205, 255, 205, 89, 207, 155,
                207, 9, 208, 133, 209, 239, 209, 79, 211, 95, 212, 105, 212,
                181, 212, 210, 212, 21, 213, 107, 213, 229, 213, 41, 214, 40,
                215, 208, 215, 211, 215, 221, 215, 54, 217, 158, 217, 237, 217,
                131, 218, 67, 219, 71, 219, 211, 219, 79, 220, 235, 220, 14,
                221, 100, 222, 94, 224, 164, 224, 166, 224, 252, 224, 185, 225,
                41, 226, 10, 228, 80, 228, 204, 228, 205, 228, 14, 229, 237,
                229, 211, 230, 33, 231, 54, 231, 96, 231, 198, 231, 13, 234,
                85, 234, 140, 234, 182, 234, 68, 236, 20, 237, 77, 237, 87,
                237, 138, 237, 145, 237, 196, 237, 60, 238, 85, 238, 88, 238,
                18, 239, 195, 239, 29, 240, 78, 240, 172, 240, 238, 240, 249,
                240, 82, 241, 94, 241, 205, 241, 212, 241, 19, 242, 237, 242,
                127, 243, 199, 243, 220, 243, 99, 244, 178, 245, 198, 245, 199,
                245, 252, 245, 220, 246, 92, 247, 115, 247, 204, 248, 238, 249,
                98, 250, 207, 250, 14, 251, 24, 251, 37, 251, 17, 253, 40, 253,
                110, 253, 209, 253, 114, 254, 151, 254, 3, 255, 20, 255, 50,
                255, 169, 255, 240, 255, 91, 0, 150, 0, 185, 0, 58, 1, 201, 1,
                21, 2, 132, 2, 165, 2, 175, 2, 198, 2, 24, 3, 7, 4, 222, 4,
                246, 4, 106, 5, 201, 5, 22, 7, 53, 7, 151, 7, 221, 7, 254, 7,
                168, 8, 170, 8, 212, 9, 76, 10, 188, 11, 160, 12, 201, 12, 3,
                13, 121, 13, 177, 13, 127, 14, 64, 15, 168, 15, 109, 16, 60,
                17, 94, 17, 26, 18, 49, 18, 122, 18, 202, 18, 239, 18, 76, 19,
                48, 21, 61, 21, 199, 22, 230, 22, 70, 23, 144, 23, 223, 23, 28,
                24, 33, 25, 22, 26, 36, 26, 45, 26, 193, 26, 5, 28, 1, 29, 34,
                29, 59, 29, 101, 29, 144, 30, 176, 30, 193, 30, 207, 30, 53,
                31, 90, 31, 181, 31, 209, 31, 29, 32, 132, 32, 160, 32, 65, 33,
                160, 33, 41, 34, 88, 34, 153, 34, 206, 34, 219, 34, 226, 34,
                86, 35, 151, 35, 162, 35, 4, 36, 151, 36, 189, 36, 113, 37,
                184, 39, 245, 39, 90, 40, 96, 40, 83, 41, 151, 41, 171, 42, 96,
                43, 115, 43, 226, 43, 19, 44, 135, 44, 156, 45, 20, 46, 118,
                46, 129, 46, 199, 47, 254, 47, 36, 48, 84, 48, 133, 48, 185,
                48, 204, 48, 31, 49, 33, 49, 53, 49, 71, 49, 87, 49, 209, 49,
                17, 50, 42, 51, 86, 52, 118, 53, 85, 54, 202, 54, 64, 55, 117,
                55, 62, 56, 135, 56, 196, 56, 45, 57, 31, 59, 185, 59, 189, 59,
                1, 60, 60, 60, 227, 61, 70, 62, 74, 62, 32, 63, 73, 63, 195,
                63, 43, 64, 164, 64, 220, 64, 249, 64, 74, 66, 64, 67, 82, 67,
                195, 67, 111, 69, 152, 69, 108, 70, 206, 70, 219, 70, 2, 71, 1,
                73, 44, 73, 244, 74, 77, 75, 248, 75, 34, 76, 54, 76, 184, 76,
                58, 77, 156, 77, 174, 77, 76, 78, 82, 78, 7, 79, 95, 79, 187,
                79, 180, 80, 222, 80, 62, 81, 54, 82, 67, 82, 10, 83, 178, 84,
                249, 84, 8, 85, 203, 85, 140, 86, 184, 86, 198, 86, 71, 88,
                191, 88, 109, 89, 166, 89, 100, 91, 221, 91, 80, 92, 24, 93,
                210, 93, 224, 94, 244, 95, 73, 96, 176, 96, 194, 96, 174, 97,
                187, 97, 87, 98, 126, 98, 192, 98, 233, 98, 70, 99, 148, 99,
                162, 99, 200, 99, 158, 100, 176, 101, 219, 101, 79, 102, 159,
                102, 203, 102, 85, 103, 4, 105, 48, 105, 141, 105, 122, 106,
                37, 107, 123, 107, 146, 107, 31, 108, 235, 108, 20, 110, 91,
                110, 242, 110, 41, 111, 46, 111, 69, 112, 74, 112, 198, 113,
                136, 114, 193, 114, 197, 115, 225, 115, 41, 117, 210, 117, 26,
                118, 33, 118, 31, 119, 42, 119, 134, 119, 15, 120, 55, 122, 71,
                122, 34, 123, 160, 123, 218, 124, 113, 125, 67, 126, 72, 127,
                155, 127,
            ]
            .into_boxed_slice(),
            offsets: None,
        }],
        coordinate_filters: Default::default(),
        offsets_filters: Default::default(),
        validity_filters: Default::default(),
    };

    let writes =
        WriteSequence::Sparse(cells::write::SparseWriteSequence::from(vec![
            SparseWriteInput {
                dimensions: vec![(
                    "G_2x0u0nImT_z5__S_LpDF".to_owned(),
                    CellValNum::single(),
                )],
                data: Cells::new(HashMap::from([
                    (
                        "G_2x0u0nImT_z5__S_LpDF".to_owned(),
                        FieldData::Int64(vec![
                            2153809892049519861,
                            -901913982103951590,
                            1862771018046098338,
                            2354164672943450129,
                            825505323377647216,
                            1629621268782148297,
                            281162692126792941,
                        ]),
                    ),
                    (
                        "NVjjN97iS_6T0y7XATzd3kCH4".to_owned(),
                        FieldData::VecUInt8(vec![
                            vec![184, 57, 77],
                            vec![167, 251, 251, 181, 182],
                            vec![63, 52, 57],
                            vec![49, 128, 208, 157, 237],
                            vec![189, 201],
                            vec![53, 232, 35],
                            vec![117, 117, 224],
                        ]),
                    ),
                    (
                        "G".to_owned(),
                        FieldData::Int16(vec![
                            245, 619, 13, 403, 292, 131, 712,
                        ]),
                    ),
                ])),
            },
        ]));

    let qc = QueryConditionExpr::field("G").ge(30281i16);

    let acc = CellsAccumulator::fold(&schema, &writes);
    instance_query_condition(
        schema.into(),
        writes.into(),
        acc.into(),
        vec![!qc],
    )
}
