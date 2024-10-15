use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::ops::{Deref, RangeInclusive};
use std::rc::Rc;

use proptest::prelude::*;
use proptest::strategy::{NewTree, ValueTree};
use proptest::test_runner::TestRunner;
use serde_json::json;

use crate::array::{ArrayType, CellOrder, CellValNum, SchemaData};
use crate::datatype::physical::BitsOrd;
use crate::filter::strategy::Requirements as FilterRequirements;
use crate::query::read::{CallbackVarArgReadBuilder, MapAdapter};
use crate::query::strategy::{
    Cells, CellsConstructor, CellsParameters, CellsStrategySchema,
    FieldDataParameters, RawResultCallback, StructuredCells,
};
use crate::query::{QueryBuilder, ReadQueryBuilder, WriteBuilder};
use crate::range::{NonEmptyDomain, Range, SingleValueRange};
use crate::{
    single_value_range_go, typed_field_data_go, Result as TileDBResult,
};

type BoxedValueTree<T> = Box<dyn ValueTree<Value = T>>;

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
) -> crate::array::schema::strategy::Requirements {
    // NB: 1 is the highest number that passes all cases (so don't use the value given by
    // `DomainRequirements::default()`) but we want to enable environmental override.
    use crate::array::domain::strategy::Requirements as DomainRequirements;
    let env_max_dimensions =
        DomainRequirements::env_max_dimensions().unwrap_or(1);

    crate::array::schema::strategy::Requirements {
        domain: Some(Rc::new(crate::array::domain::strategy::Requirements {
            array_type,
            num_dimensions: 1..=env_max_dimensions,
            dimension: Some(crate::array::dimension::strategy::Requirements {
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

#[cfg(test)]
mod tests {
    use tiledb_test_utils::{self, TestArrayUri};

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
        let test_uri = tiledb_test_utils::get_uri_generator()
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
