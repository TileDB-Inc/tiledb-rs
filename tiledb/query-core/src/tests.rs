use std::rc::Rc;

use cells::write::strategy::{WriteParameters, WriteSequenceParameters};
use cells::write::{
    DenseWriteInput, SparseWriteInput, WriteInput, WriteSequence,
};
use cells::{self, Cells};
use proptest::prelude::*;
use tiledb_api::array::{Array, ArrayOpener};
use tiledb_api::query::strategy::query_write_schema_requirements;
use tiledb_api::Factory;
use tiledb_common::array::{ArrayType, Mode};
use tiledb_common::range::NonEmptyDomain;
use tiledb_pod::array::SchemaData;
use uri::TestArrayUri;

use super::*;

#[test]
fn query_roundtrip() -> anyhow::Result<()> {
    let ctx = Context::new()?;

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
        do_query_roundtrip(&ctx, schema_spec, write_sequence)
            .expect("Error in query round trip")
    });

    Ok(())
}

#[test]
fn query_roundtrip_accumulated() -> anyhow::Result<()> {
    let ctx = Context::new()?;

    let schema_req = query_write_schema_requirements(None);

    let strategy =
        any_with::<SchemaData>(Rc::new(schema_req)).prop_flat_map(|schema| {
            let schema = Rc::new(schema);
            (
                Just(Rc::clone(&schema)),
                any_with::<WriteSequence>(
                    WriteSequenceParameters::default_for(schema),
                ),
            )
        });

    proptest!(|((schema_spec, write_sequence) in strategy)| {
        do_query_roundtrip(&ctx, schema_spec, write_sequence)
            .expect("Error in query round trip");
    });

    Ok(())
}

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
}

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
}

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
}

trait BuildReadQuery {
    fn read_same_fields(&self, array: Array) -> anyhow::Result<QueryBuilder>;
}

impl BuildReadQuery for Cells {
    fn read_same_fields(&self, array: Array) -> anyhow::Result<QueryBuilder> {
        Ok(self
            .fields()
            .keys()
            .fold(
                QueryBuilder::new(array, QueryType::Read).start_fields(),
                |b, k| b.field(k),
            )
            .end_fields())
    }
}

impl BuildReadQuery for DenseCellsAccumulator {
    fn read_same_fields(&self, array: Array) -> anyhow::Result<QueryBuilder> {
        self.write.as_ref().unwrap().read_same_fields(array)
    }
}

impl BuildReadQuery for SparseCellsAccumulator {
    fn read_same_fields(&self, array: Array) -> anyhow::Result<QueryBuilder> {
        self.cells.as_ref().unwrap().read_same_fields(array)
    }
}

impl BuildReadQuery for CellsAccumulator {
    fn read_same_fields(&self, array: Array) -> anyhow::Result<QueryBuilder> {
        match self {
            Self::Dense(ref d) => d.read_same_fields(array),
            Self::Sparse(ref s) => s.read_same_fields(array),
        }
    }
}

impl BuildReadQuery for DenseWriteInput {
    fn read_same_fields(&self, array: Array) -> anyhow::Result<QueryBuilder> {
        Ok(self
            .subarray
            .iter()
            .enumerate()
            .fold(
                self.data.read_same_fields(array)?.start_subarray(),
                |b, (d, r)| b.add_range(d, r.clone()),
            )
            .end_subarray())
    }
}

impl BuildReadQuery for SparseWriteInput {
    fn read_same_fields(&self, array: Array) -> anyhow::Result<QueryBuilder> {
        self.data.read_same_fields(array)
    }
}

impl BuildReadQuery for WriteInput {
    fn read_same_fields(&self, array: Array) -> anyhow::Result<QueryBuilder> {
        match self {
            Self::Dense(ref d) => d.read_same_fields(array),
            Self::Sparse(ref s) => s.read_same_fields(array),
        }
    }
}

trait BuildWriteQuery {
    fn write_query_builder(&self, array: Array)
        -> anyhow::Result<QueryBuilder>;
}

impl BuildWriteQuery for Cells {
    fn write_query_builder(
        &self,
        array: Array,
    ) -> anyhow::Result<QueryBuilder> {
        Ok(self
            .fields()
            .iter()
            .fold(
                QueryBuilder::new(array, QueryType::Write).start_fields(),
                |b, (k, v)| b.field_with_buffer(k, v.to_arrow()),
            )
            .end_fields())
    }
}

/*
impl BuildWriteQuery for DenseCellsAccumulator {
    fn write_query_builder(
        &self,
        array: Array,
    ) -> anyhow::Result<QueryBuilder> {
        self.write.as_ref().map(|w| w.write_query_
        todo!()
    }
}

impl BuildWriteQuery for SparseCellsAccumulator {
    fn write_query_builder(
        &self,
        array: Array,
    ) -> anyhow::Result<QueryBuilder> {
        todo!()
    }
}

impl BuildWriteQuery for CellsAccumulator {
    fn write_query_builder(
        &self,
        array: Array,
    ) -> anyhow::Result<QueryBuilder> {
        match self {
            Self::Dense(ref d) => d.write_query_builder(array),
            Self::Sparse(ref s) => s.write_query_builder(array),
        }
    }
}
*/

impl BuildWriteQuery for DenseWriteInput {
    fn write_query_builder(
        &self,
        array: Array,
    ) -> anyhow::Result<QueryBuilder> {
        Ok(self
            .subarray
            .iter()
            .enumerate()
            .fold(
                self.data.write_query_builder(array)?.start_subarray(),
                |b, (d, r)| b.add_range(d, r.clone()),
            )
            .end_subarray())
    }
}

impl BuildWriteQuery for SparseWriteInput {
    fn write_query_builder(
        &self,
        array: Array,
    ) -> anyhow::Result<QueryBuilder> {
        self.data.write_query_builder(array)
    }
}

impl BuildWriteQuery for WriteInput {
    fn write_query_builder(
        &self,
        array: Array,
    ) -> anyhow::Result<QueryBuilder> {
        match self {
            Self::Dense(ref d) => d.write_query_builder(array),
            Self::Sparse(ref s) => s.write_query_builder(array),
        }
    }
}

fn do_query_roundtrip(
    ctx: &Context,
    schema_spec: Rc<SchemaData>,
    write_sequence: WriteSequence,
) -> anyhow::Result<()> {
    let test_uri = uri::get_uri_generator()?;
    let uri = test_uri.with_path("array")?;

    let schema_in = schema_spec
        .create(ctx)
        .expect("Error constructing arbitrary schema");
    Array::create(ctx, &uri, schema_in).expect("Error creating array");

    let mut accumulated = AccumulatedArray::new(&schema_spec);

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
        apply_write(ctx, &uri, write, &mut accumulated, &sort_keys)?;
    }
    Ok(())
}

struct AccumulatedArray {
    domain: Option<NonEmptyDomain>,
    cells: CellsAccumulator,
}

impl AccumulatedArray {
    pub fn new(schema: &SchemaData) -> Self {
        Self {
            domain: None,
            cells: CellsAccumulator::new(schema),
        }
    }
}

fn apply_write(
    ctx: &Context,
    uri: &str,
    write: WriteInput,
    accumulated_array: &mut AccumulatedArray,
    cmp_sort_keys: &[String],
) -> anyhow::Result<()> {
    /* write data and preserve ranges for sanity check */
    let write_ranges = {
        let array =
            Array::open(ctx, &uri, Mode::Write).expect("Error opening array");

        let mut write_query = write
            .write_query_builder(array)
            .expect("Error building write query")
            .build()
            .unwrap();
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
        return Ok(());
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

        let mut read = write.read_same_fields(array).unwrap().build().unwrap();

        if let Some(write_ranges) = write_ranges {
            let read_ranges = read.subarray().unwrap().ranges().unwrap();
            assert_eq!(write_ranges, read_ranges);
        }

        let mut cells = {
            let status = read.submit().unwrap();
            assert_eq!(status, QueryStatus::Completed);

            let record_batch = read.records().unwrap();
            cells::arrow::from_record_batch(&record_batch).unwrap()
        };

        /* `cells` should match the write */
        {
            let write_sorted = write.cells().sorted(&cmp_sort_keys);
            cells.sort(&cmp_sort_keys);
            assert_eq!(write_sorted, cells);
        }

        (array, _) = read.finalize().unwrap();
    }

    /* finally, check that everything written up until now is correct */
    array = array.reopen().start_timestamp(0).unwrap().open().unwrap();

    /* check array non-empty domain */
    if let Some(accumulated_domain) = accumulated_array.domain.as_mut() {
        let Some(write_domain) = write.domain() else {
            unreachable!()
        };
        *accumulated_domain = accumulated_domain.union(&write_domain);
    } else {
        accumulated_array.domain = write.domain();
    }
    {
        let Some(acc) = accumulated_array.domain.as_ref() else {
            unreachable!()
        };
        let nonempty = array.nonempty_domain().unwrap().unwrap().untyped();
        assert_eq!(*acc, nonempty);
    }

    /* update accumulated expected array data */
    accumulated_array.cells.accumulate(write);
    {
        let acc = accumulated_array.cells.cells().sorted(&cmp_sort_keys);

        let cells = {
            let mut read = accumulated_array
                .cells
                .read_same_fields(array)
                .unwrap()
                .build()
                .unwrap();

            let mut cells = {
                let status = read.submit().unwrap();
                assert_eq!(status, QueryStatus::Completed);

                let record_batch = read.records().unwrap();
                cells::arrow::from_record_batch(&record_batch).unwrap()
            };
            cells.sort(&cmp_sort_keys);
            cells
        };

        assert_eq!(acc, cells);
    }

    // safety valve to ensure we don't write two fragments in the same millisecond
    if safety_write_start.elapsed() < std::time::Duration::from_millis(1) {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }

    Ok(())
}
