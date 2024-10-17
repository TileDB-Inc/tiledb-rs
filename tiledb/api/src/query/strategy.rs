use std::collections::HashMap;
use std::rc::Rc;

use cells::write::{
    DenseWriteInput, SparseWriteInput, WriteInput, WriteInputRef,
};
use cells::{typed_field_data_go, Cells, FieldData};
use tiledb_common::array::{ArrayType, CellValNum};
use tiledb_common::physical_type_go;
use tiledb_pod::array::dimension::strategy::Requirements as DimensionRequirements;
use tiledb_pod::array::domain::strategy::Requirements as DomainRequirements;
use tiledb_pod::array::schema::strategy::Requirements as SchemaRequirements;
use tiledb_pod::filter::strategy::Requirements as FilterRequirements;

use super::*;
use crate::query::read::output::{
    CellStructureSingleIterator, FixedDataIterator, RawReadOutput,
    TypedRawReadOutput, VarDataIterator,
};
use crate::query::read::{
    CallbackVarArgReadBuilder, FieldMetadata, ManagedBuffer, Map, MapAdapter,
    RawReadHandle, ReadCallbackVarArg, TypedReadHandle,
};
use crate::typed_query_buffers_go;

/// Returns a base set of requirements for filters to be used
/// in write queries.
///
/// Requirements are chosen to either avoid
/// constraints on input (e.g. positive delta filtering requires
/// sorted input, float scale filtering is not invertible)
/// or to avoid issues in the tiledb core library in as
/// many scenarios as possible.
// now that we're actually writing data we will hit the fun bugs.
// there are several in the filter pipeline, so we must heavily
// restrict what is allowed until the bugs are fixed.
pub fn query_write_filter_requirements() -> FilterRequirements {
    FilterRequirements {
        allow_bit_reduction: false,     // SC-47560
        allow_bit_shuffle: false,       // SC-48409
        allow_byte_shuffle: false,      // SC-48409
        allow_positive_delta: false,    // nothing yet to ensure sort order
        allow_scale_float: false,       // not invertible due to precision loss
        allow_xor: false,               // SC-47328
        allow_compression_rle: false, // probably can be enabled but nontrivial
        allow_compression_dict: false, // probably can be enabled but nontrivial
        allow_compression_delta: false, // SC-47328
        allow_webp: false,            // SC-51250
        ..Default::default()
    }
}

/// Returns a base set of schema requirements for running a query.
///
/// Requirements are chosen to either avoid constraints on write input
/// or to avoid issues in the tiledb core library in as many scenarios as possible.
pub fn query_write_schema_requirements(
    array_type: Option<ArrayType>,
) -> SchemaRequirements {
    // NB: 1 is the highest number that passes all cases (so don't use the value given by
    // `DomainRequirements::default()`) but we want to enable environmental override.
    let env_max_dimensions =
        DomainRequirements::env_max_dimensions().unwrap_or(1);

    SchemaRequirements {
        domain: Some(Rc::new(DomainRequirements {
            array_type,
            num_dimensions: 1..=env_max_dimensions,
            dimension: Some(DimensionRequirements {
                filters: Some(Rc::new(query_write_filter_requirements())),
                ..Default::default()
            }),
            ..Default::default()
        })),
        attribute_filters: Some(Rc::new(query_write_filter_requirements())),
        coordinates_filters: Some(Rc::new(query_write_filter_requirements())),
        offsets_filters: Some(Rc::new(query_write_filter_requirements())),
        validity_filters: Some(Rc::new(query_write_filter_requirements())),
        ..Default::default()
    }
}

impl From<&TypedRawReadOutput<'_>> for FieldData {
    fn from(value: &TypedRawReadOutput) -> Self {
        typed_query_buffers_go!(value.buffers, DT, ref handle, {
            let rr = RawReadOutput {
                ncells: value.ncells,
                input: handle.borrow(),
            };
            match rr.input.cell_structure.as_cell_val_num() {
                CellValNum::Fixed(nz) if nz.get() == 1 => Self::from(
                    CellStructureSingleIterator::try_from(rr)
                        .unwrap()
                        .collect::<Vec<DT>>(),
                ),
                CellValNum::Fixed(_) => Self::from(
                    FixedDataIterator::try_from(rr)
                        .unwrap()
                        .map(|slice| slice.to_vec())
                        .collect::<Vec<Vec<DT>>>(),
                ),
                CellValNum::Var => Self::from(
                    VarDataIterator::try_from(rr)
                        .unwrap()
                        .map(|s| s.to_vec())
                        .collect::<Vec<Vec<DT>>>(),
                ),
            }
        })
    }
}

impl ToReadQuery for Cells {
    type ReadBuilder<'data, B> =
        CallbackVarArgReadBuilder<'data, RawResultCallback, B>;

    fn attach_read<'data, B>(
        &self,
        b: B,
    ) -> TileDBResult<Self::ReadBuilder<'data, B>>
    where
        B: ReadQueryBuilder<'data>,
    {
        let field_order = self.fields().keys().cloned().collect::<Vec<_>>();
        let handles = {
            let schema = b.base().array().schema().unwrap();

            field_order
                .iter()
                .map(|name| {
                    let field = schema.field(name.clone()).unwrap();
                    physical_type_go!(field.datatype().unwrap(), DT, {
                        let managed: ManagedBuffer<DT> = ManagedBuffer::new(
                            field.query_scratch_allocator(None).unwrap(),
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
}

impl ToReadQuery for DenseWriteInput {
    type ReadBuilder<'data, B> = CallbackVarArgReadBuilder<
        'data,
        MapAdapter<CellsConstructor, RawResultCallback>,
        B,
    > where Self: 'data;

    fn attach_read<'data, B>(
        &'data self,
        b: B,
    ) -> TileDBResult<Self::ReadBuilder<'data, B>>
    where
        B: ReadQueryBuilder<'data>,
    {
        let mut subarray = b.start_subarray()?;

        for i in 0..self.subarray.len() {
            subarray = subarray.add_range(i, self.subarray[i].clone())?;
        }

        let b: B = subarray.finish_subarray()?.layout(self.layout)?;

        Ok(self.data.attach_read(b)?.map(CellsConstructor::new()))
    }
}

impl ToReadQuery for SparseWriteInput {
    type ReadBuilder<'data, B> = CallbackVarArgReadBuilder<
        'data,
        MapAdapter<CellsConstructor, RawResultCallback>,
        B,
    >;

    fn attach_read<'data, B>(
        &'data self,
        b: B,
    ) -> TileDBResult<Self::ReadBuilder<'data, B>>
    where
        B: ReadQueryBuilder<'data>,
    {
        Ok(self.data.attach_read(b)?.map(CellsConstructor::new()))
    }
}

impl ToReadQuery for WriteInput {
    type ReadBuilder<'data, B> = CallbackVarArgReadBuilder<
        'data,
        MapAdapter<CellsConstructor, RawResultCallback>,
        B,
    >;

    fn attach_read<'data, B>(
        &'data self,
        b: B,
    ) -> TileDBResult<Self::ReadBuilder<'data, B>>
    where
        B: ReadQueryBuilder<'data>,
    {
        match self {
            Self::Dense(ref d) => d.attach_read(b),
            Self::Sparse(ref s) => s.attach_read(b),
        }
    }
}

impl ToReadQuery for WriteInputRef<'_> {
    type ReadBuilder<'data, B> = CallbackVarArgReadBuilder<
        'data,
        MapAdapter<CellsConstructor, RawResultCallback>,
        B,
    > where Self: 'data;

    fn attach_read<'data, B>(
        &'data self,
        b: B,
    ) -> TileDBResult<Self::ReadBuilder<'data, B>>
    where
        B: ReadQueryBuilder<'data>,
    {
        match self {
            Self::Dense(d) => d.attach_read(b),
            Self::Sparse(s) => s.attach_read(b),
        }
    }
}

impl ToWriteQuery for Cells {
    fn attach_write<'data>(
        &'data self,
        b: WriteBuilder<'data>,
    ) -> TileDBResult<WriteBuilder<'data>> {
        let mut b = b;
        for f in self.fields().iter() {
            b = typed_field_data_go!(f.1, data, b.data_typed(f.0, data))?;
        }
        Ok(b)
    }
}

impl ToWriteQuery for DenseWriteInput {
    fn attach_write<'data>(
        &'data self,
        b: WriteBuilder<'data>,
    ) -> TileDBResult<WriteBuilder<'data>> {
        let mut subarray = self.data.attach_write(b)?.start_subarray()?;

        for i in 0..self.subarray.len() {
            subarray = subarray.add_range(i, self.subarray[i].clone())?;
        }

        subarray.finish_subarray()?.layout(self.layout)
    }
}

impl ToWriteQuery for SparseWriteInput {
    fn attach_write<'data>(
        &'data self,
        b: WriteBuilder<'data>,
    ) -> TileDBResult<WriteBuilder<'data>> {
        self.data.attach_write(b)
    }
}

impl ToWriteQuery for WriteInput {
    fn attach_write<'data>(
        &'data self,
        b: WriteBuilder<'data>,
    ) -> TileDBResult<WriteBuilder<'data>> {
        match self {
            Self::Dense(ref d) => d.attach_write(b),
            Self::Sparse(ref s) => s.attach_write(b),
        }
    }
}

impl ToWriteQuery for WriteInputRef<'_> {
    fn attach_write<'data>(
        &'data self,
        b: WriteBuilder<'data>,
    ) -> TileDBResult<WriteBuilder<'data>> {
        match self {
            Self::Dense(d) => d.attach_write(b),
            Self::Sparse(s) => s.attach_write(b),
        }
    }
}

// TODO: where should these go
pub struct RawReadQueryResult(pub HashMap<String, FieldData>);

pub struct RawResultCallback {
    pub field_order: Vec<String>,
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

/// Query callback which accumulates results from each step into `Cells`
/// and returns the `Cells` as the final result.
#[derive(Default)]
pub struct CellsConstructor {
    cells: Option<Cells>,
}

impl CellsConstructor {
    pub fn new() -> Self {
        CellsConstructor { cells: None }
    }
}

impl Map<RawReadQueryResult, RawReadQueryResult> for CellsConstructor {
    type Intermediate = ();
    type Final = Cells;

    fn map_intermediate(
        &mut self,
        batch: RawReadQueryResult,
    ) -> Self::Intermediate {
        let batch = Cells::new(batch.0);
        if let Some(cells) = self.cells.as_mut() {
            cells.extend(batch);
        } else {
            self.cells = Some(batch)
        }
    }

    fn map_final(mut self, batch: RawReadQueryResult) -> Self::Final {
        self.map_intermediate(batch);
        self.cells.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use cells::write::strategy::{WriteParameters, WriteSequenceParameters};
    use cells::write::{DenseWriteInput, SparseWriteInput, WriteSequence};
    use proptest::prelude::*;
    use tiledb_common::range::{NonEmptyDomain, Range};
    use tiledb_pod::array::schema::SchemaData;
    use uri::TestArrayUri;

    use super::*;
    use crate::array::{Array, ArrayOpener, Mode};
    use crate::error::Error;
    use crate::query::{
        Query, QueryBuilder, ReadBuilder, ReadQuery, WriteBuilder,
    };
    use crate::{Context, Factory};

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

    enum CellsAccumulator {
        Dense(DenseCellsAccumulator),
        Sparse(SparseCellsAccumulator),
    }

    impl CellsAccumulator {
        pub fn new(schema: &SchemaData) -> Self {
            match schema.array_type {
                ArrayType::Dense => {
                    Self::Dense(DenseCellsAccumulator::new(schema))
                }
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
        let test_uri = uri::get_uri_generator()
            .map_err(|e| Error::Other(e.to_string()))?;
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
                    let read_ranges =
                        read.subarray().unwrap().ranges().unwrap();
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
                let nonempty =
                    array.nonempty_domain().unwrap().unwrap().untyped();
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
            if safety_write_start.elapsed()
                < std::time::Duration::from_millis(1)
            {
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

        let strategy = any_with::<SchemaData>(Rc::new(schema_req))
            .prop_flat_map(|schema| {
                let schema = Rc::new(schema);
                (
                    Just(Rc::clone(&schema)),
                    any_with::<WriteInput>(WriteParameters::default_for(
                        schema,
                    ))
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

        let schema_req =
            query_write_schema_requirements(Some(ArrayType::Sparse));

        let strategy = any_with::<SchemaData>(Rc::new(schema_req))
            .prop_flat_map(|schema| {
                let schema = Rc::new(schema);
                (
                    Just(Rc::clone(&schema)),
                    any_with::<WriteSequence>(
                        WriteSequenceParameters::default_for(Rc::clone(
                            &schema,
                        )),
                    ),
                )
            });

        proptest!(|((schema_spec, write_sequence) in strategy)| {
            do_write_readback(&ctx, schema_spec, write_sequence)?;
        });

        Ok(())
    }
}
