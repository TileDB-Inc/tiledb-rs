use std::fmt::Debug;
use std::rc::Rc;

use proptest::prelude::*;

use crate::array::SchemaData;
use crate::query::strategy::{Cells, CellsParameters, CellsStrategySchema};

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
        any_with::<Cells>(CellsParameters {
            schema: Some(CellsStrategySchema::WriteSchema(Rc::clone(schema))),
            ..Default::default()
        }),
        0..MAX_WRITES,
    )
    .prop_map(|writes| WriteSequence { writes })
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
                acc.copy_from(write)
            } else {
                accumulated_write = Some(write);
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
