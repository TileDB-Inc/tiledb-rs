use std::rc::Rc;

use proptest::prelude::*;
use proptest::test_runner::TestRunner;
use tiledb_serde::array::schema::SchemaData;
use uri::TestArrayUri;

use crate::error::Error;
use crate::tests::prelude::*;
use crate::tests::strategy::prelude::*;
use crate::{Context, Factory, Result as TileDBResult};

pub mod quickstart;
pub mod sparse_all;

pub struct TestArray {
    _location: Box<dyn TestArrayUri>,
    pub uri: String,
    pub context: Context,
    pub schema: Rc<SchemaData>,
}

impl TestArray {
    pub fn new(name: &str, schema: Rc<SchemaData>) -> TileDBResult<Self> {
        let test_uri = uri::get_uri_generator()
            .map_err(|e| Error::Other(e.to_string()))?;
        let uri = test_uri
            .with_path(name)
            .map_err(|e| Error::Other(e.to_string()))?;

        let context = Context::new()?;
        {
            let s = schema.create(&context)?;
            Array::create(&context, &uri, s)?;
        }

        Ok(TestArray {
            _location: Box::new(test_uri),
            uri,
            context,
            schema,
        })
    }

    fn open(&self, mode: Mode) -> TileDBResult<Array> {
        Array::open(&self.context, &self.uri, mode)
    }

    pub fn for_read(&self) -> TileDBResult<Array> {
        self.open(Mode::Read)
    }

    pub fn for_write(&mut self) -> TileDBResult<Array> {
        self.open(Mode::Write)
    }

    pub fn arbitrary_input(&self, runner: &mut TestRunner) -> WriteInput {
        match self.schema.array_type {
            ArrayType::Sparse => {
                let strat_input =
                    any_with::<SparseWriteInput>(SparseWriteParameters {
                        schema: Some(Rc::clone(&self.schema)),
                        ..Default::default()
                    });

                WriteInput::Sparse(
                    strat_input.new_tree(runner).unwrap().current(),
                )
            }
            ArrayType::Dense => todo!(), // probably just mimic the above
        }
    }

    pub fn try_insert(&mut self, input: &WriteInput) -> TileDBResult<Array> {
        let w = input
            .attach_write(WriteBuilder::new(self.for_write()?)?)?
            .build();
        w.submit()?;
        w.finalize()
    }
}
