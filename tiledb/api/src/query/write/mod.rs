use super::*;

use std::collections::HashMap;
use std::pin::Pin;
use std::rc::Rc;

use crate::config::Config;
use crate::error::{DatatypeError, Error};
use crate::query::buffer::{CellStructure, QueryBuffers, TypedQueryBuffers};
use crate::query::write::input::{
    DataProvider, RecordProvider, TypedDataProvider,
};
use crate::typed_query_buffers_go;

pub mod input;

struct RawWriteInput<'data> {
    _data_size: Pin<Box<u64>>,
    _offsets_size: Option<Pin<Box<u64>>>,
    _validity_size: Option<Pin<Box<u64>>>,
    _input: TypedQueryBuffers<'data>,
}

type InputMap<'data> = HashMap<String, RawWriteInput<'data>>;

pub struct WriteQuery<'data> {
    base: QueryBase,

    /// Hold on to query inputs to ensure they live long enough
    _inputs: InputMap<'data>,
}

impl ContextBound for WriteQuery<'_> {
    fn context(&self) -> Context {
        self.base.context()
    }
}

impl Query for WriteQuery<'_> {
    fn base(&self) -> &QueryBase {
        self.base.base()
    }

    fn finalize(self) -> TileDBResult<Array> {
        self.base.finalize()
    }
}

impl WriteQuery<'_> {
    pub fn submit(&self) -> TileDBResult<()> {
        self.base.do_submit()
    }
}

pub struct WriteBuilder<'data> {
    base: BuilderBase,
    inputs: InputMap<'data>,
}

impl ContextBound for WriteBuilder<'_> {
    fn context(&self) -> Context {
        self.base.context()
    }
}

impl<'data> QueryBuilder for WriteBuilder<'data> {
    type Query = WriteQuery<'data>;

    fn base(&self) -> &BuilderBase {
        &self.base
    }

    fn build(self) -> Self::Query {
        WriteQuery {
            base: self.base.build(),
            _inputs: self.inputs,
        }
    }
}

impl<'data> WriteBuilder<'data> {
    pub fn new(array: Array) -> TileDBResult<Self> {
        let base = BuilderBase::new(array, QueryType::Write)?;

        {
            let mut config = Config::new()?;
            config.set("sm.var_offsets.bitsize", "64")?;
            config.set("sm.var_offsets.mode", "elements")?;
            config.set("sm.var_offsets.extra_element", "true")?;

            /*
             * TODO: make sure that users can't override this somehow,
             * else we will be very very sad
             */
            let c_query = **base.cquery();

            base.capi_call(|c_context| unsafe {
                ffi::tiledb_query_set_config(c_context, c_query, config.capi())
            })?;
        }

        Ok(WriteBuilder {
            base,
            inputs: HashMap::new(),
        })
    }

    pub fn buffers<S>(
        mut self,
        field: S,
        input: TypedQueryBuffers<'data>,
    ) -> TileDBResult<Self>
    where
        S: AsRef<str>,
    {
        let field_name = field.as_ref().to_string();

        // validate data type against schema
        // NB: core will validate cell val num but raw pointers can't
        // help with target data type
        {
            let schema = self.base().array().schema()?;
            let schema_field = schema.field(&field_name)?;
            let target_dt = schema_field.datatype()?;
            typed_query_buffers_go!(input, DT, _, {
                if !target_dt.is_compatible_type::<DT>() {
                    return Err(Error::Datatype(
                        DatatypeError::physical_type_incompatible::<DT>(
                            target_dt,
                        ),
                    ));
                }
            })
        }

        let c_query = **self.base().cquery();
        let c_name = cstring!(field_name.clone());

        let (c_bufptr, mut data_size) =
            typed_query_buffers_go!(input, _DT, ref qb, {
                let c_bufptr =
                    qb.data.as_ref().as_ptr() as *mut std::ffi::c_void;
                let data_size = Box::pin(qb.data.size() as u64);
                (c_bufptr, data_size)
            });

        let c_sizeptr = data_size.as_mut().get_mut() as *mut u64;

        self.capi_call(|ctx| unsafe {
            ffi::tiledb_query_set_data_buffer(
                ctx,
                c_query,
                c_name.as_ptr(),
                c_bufptr,
                c_sizeptr,
            )
        })?;

        let offsets_size = if let CellStructure::Var(offsets) =
            input.cell_structure().borrow()
        {
            let mut offsets_size = Box::pin(offsets.size() as u64);

            let c_offptr = offsets.as_ref().as_ptr() as *mut u64;
            let c_sizeptr = offsets_size.as_mut().get_mut() as *mut u64;

            self.capi_call(|ctx| unsafe {
                ffi::tiledb_query_set_offsets_buffer(
                    ctx,
                    c_query,
                    c_name.as_ptr(),
                    c_offptr,
                    c_sizeptr,
                )
            })?;
            Some(offsets_size)
        } else {
            None
        };

        let mut validity_size =
            input.validity().map(|b| Box::pin(b.size() as u64));

        if let Some(ref mut validity_size) = validity_size.as_mut() {
            let c_validityptr =
                input.validity().unwrap().as_ref().as_ptr() as *mut u8;
            let c_sizeptr = validity_size.as_mut().get_mut() as *mut u64;

            self.capi_call(|ctx| unsafe {
                ffi::tiledb_query_set_validity_buffer(
                    ctx,
                    c_query,
                    c_name.as_ptr(),
                    c_validityptr,
                    c_sizeptr,
                )
            })?;
        }

        let raw_write_input = RawWriteInput {
            _data_size: data_size,
            _offsets_size: offsets_size,
            _validity_size: validity_size,
            _input: input,
        };

        self.inputs.insert(field_name, raw_write_input);

        Ok(self)
    }

    pub fn data<S, T>(self, field: S, data: &'data T) -> TileDBResult<Self>
    where
        S: AsRef<str>,
        T: DataProvider,
        QueryBuffers<'data, <T as DataProvider>::Unit>:
            Into<TypedQueryBuffers<'data>>,
    {
        let field_name = field.as_ref();

        let input = {
            let schema = self.base().array().schema()?;
            let schema_field = schema.field(field_name)?;
            data.query_buffers(
                schema_field.cell_val_num()?,
                schema_field.nullability()?,
            )?
        };

        self.buffers(field, input.into())
    }

    pub fn data_typed<S, T>(
        self,
        field: S,
        data: &'data T,
    ) -> TileDBResult<Self>
    where
        S: AsRef<str>,
        T: TypedDataProvider,
    {
        let field_name = field.as_ref();

        let input = {
            let schema = self.base().array().schema()?;
            let schema_field = schema.field(field_name)?;
            data.typed_query_buffers(
                schema_field.cell_val_num()?,
                schema_field.nullability()?,
            )?
        };

        self.buffers(field, input)
    }

    pub fn records<R>(self, data: &'data R) -> TileDBResult<Self>
    where
        R: RecordProvider<'data>,
    {
        let schema = Rc::new(self.base().array().schema()?);

        let mut b = self;
        for try_input in data.tiledb_inputs(Rc::clone(&schema)) {
            let (field, input_data) = try_input?;
            b = b.buffers(field, input_data)?;
        }

        Ok(b)
    }
}

#[cfg(test)]
mod tests;
