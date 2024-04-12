use super::*;

use std::collections::HashMap;
use std::pin::Pin;

use crate::query::private::QueryCAPIInterface;
use crate::query::write::input::{DataProvider, InputData};

pub mod input;

struct RawWriteInput<'data> {
    data_size: Pin<Box<u64>>,
    offsets_size: Option<Pin<Box<u64>>>,
    input: InputData<'data>,
}

type InputMap<'data> = HashMap<String, RawWriteInput<'data>>;

#[derive(ContextBound)]
pub struct WriteQuery<'ctx, 'data> {
    #[base(ContextBound)]
    base: Query<'ctx>,

    /// Hold on to query inputs to ensure they live long enough
    _inputs: InputMap<'data>,
}

impl<'ctx, 'data> WriteQuery<'ctx, 'data> {
    pub fn submit(&self) -> TileDBResult<()> {
        self.base.do_submit()
    }
}

impl<'ctx, 'data> private::QueryCAPIInterface for WriteQuery<'ctx, 'data> {
    fn raw(&self) -> &RawQuery {
        self.base.raw()
    }
}

impl<'ctx, 'data> WriteQuery<'ctx, 'data> {
    pub fn finalize(self) -> TileDBResult<Array<'ctx>> {
        let c_context = self.context().capi();
        let c_query = **self.raw();
        self.capi_return(unsafe {
            ffi::tiledb_query_finalize(c_context, c_query)
        })?;

        Ok(self.base.array)
    }
}

#[derive(ContextBound)]
pub struct WriteBuilder<'ctx, 'data> {
    #[base(ContextBound)]
    base: BuilderBase<'ctx>,
    inputs: InputMap<'data>,
}

impl<'ctx, 'data> crate::query::private::QueryCAPIInterface
    for WriteBuilder<'ctx, 'data>
{
    fn raw(&self) -> &RawQuery {
        self.base.raw()
    }
}

impl<'ctx, 'data> QueryBuilder<'ctx> for WriteBuilder<'ctx, 'data> {
    type Query = WriteQuery<'ctx, 'data>;

    fn array(&self) -> &Array {
        self.base.array()
    }

    fn build(self) -> Self::Query {
        WriteQuery {
            base: self.base.build(),
            _inputs: self.inputs,
        }
    }
}

impl<'ctx, 'data> WriteBuilder<'ctx, 'data> {
    pub fn new(
        context: &'ctx Context,
        array: Array<'ctx>,
    ) -> TileDBResult<Self> {
        Ok(WriteBuilder {
            base: BuilderBase::new(context, array, QueryType::Write)?,
            inputs: HashMap::new(),
        })
    }

    pub fn data_typed<S, T>(
        mut self,
        field: S,
        data: &'data T,
    ) -> TileDBResult<Self>
    where
        S: AsRef<str>,
        T: DataProvider,
    {
        let field = field.as_ref();
        let input = data.as_tiledb_input();
        let mut raw_write_input = RawWriteInput {
            data_size: Box::pin(input.data.size() as u64),
            offsets_size: input
                .cell_offsets
                .as_ref()
                .map(|b| Box::pin(b.size() as u64)),
            input,
        };

        let c_context = self.context().capi();
        let c_query = **self.raw();
        let c_name = cstring!(field);

        self.capi_return(unsafe {
            let c_bufptr = raw_write_input.input.data.as_ref().as_ptr()
                as *mut std::ffi::c_void;
            let c_sizeptr =
                raw_write_input.data_size.as_mut().get_mut() as *mut u64;

            ffi::tiledb_query_set_data_buffer(
                c_context,
                c_query,
                c_name.as_ptr(),
                c_bufptr,
                c_sizeptr,
            )
        })?;

        if let Some(ref mut offsets_size) =
            raw_write_input.offsets_size.as_mut()
        {
            let c_offptr = raw_write_input
                .input
                .cell_offsets
                .as_ref()
                .unwrap()
                .as_ref()
                .as_ptr() as *mut u64;
            let c_sizeptr = offsets_size.as_mut().get_mut() as *mut u64;

            self.capi_return(unsafe {
                ffi::tiledb_query_set_offsets_buffer(
                    c_context,
                    c_query,
                    c_name.as_ptr(),
                    c_offptr,
                    c_sizeptr,
                )
            })?;
        }

        self.inputs.insert(String::from(field), raw_write_input);

        Ok(self)
    }
}
