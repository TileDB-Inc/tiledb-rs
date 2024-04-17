use super::*;

use std::collections::HashMap;
use std::pin::Pin;

use crate::query::buffer::QueryBuffers;
use crate::query::write::input::DataProvider;

pub mod input;

struct RawWriteInput<'data> {
    _data_size: Pin<Box<u64>>,
    _offsets_size: Option<Pin<Box<u64>>>,
    _validity_size: Option<Pin<Box<u64>>>,
    _input: TypedQueryBuffers<'data>,
}

pub enum TypedQueryBuffers<'data> {
    UInt8(QueryBuffers<'data, u8>),
    UInt16(QueryBuffers<'data, u16>),
    UInt32(QueryBuffers<'data, u32>),
    UInt64(QueryBuffers<'data, u64>),
    Int8(QueryBuffers<'data, i8>),
    Int16(QueryBuffers<'data, i16>),
    Int32(QueryBuffers<'data, i32>),
    Int64(QueryBuffers<'data, i64>),
}

macro_rules! typed_input_data {
    ($($V:ident : $U:ty),+) => {
        $(
            impl<'data> From<QueryBuffers<'data, $U>> for TypedQueryBuffers<'data> {
                fn from(value: QueryBuffers<'data, $U>) -> Self {
                    TypedQueryBuffers::$V(value)
                }
            }
        )+
    }
}

typed_input_data!(UInt8: u8, UInt16: u16, UInt32: u32, UInt64: u64);
typed_input_data!(Int8: i8, Int16: i16, Int32: i32, Int64: i64);

type InputMap<'data> = HashMap<String, RawWriteInput<'data>>;

#[derive(ContextBound, Query)]
pub struct WriteQuery<'ctx, 'data> {
    #[base(ContextBound, Query)]
    base: QueryBase<'ctx>,

    /// Hold on to query inputs to ensure they live long enough
    _inputs: InputMap<'data>,
}

impl<'ctx, 'data> WriteQuery<'ctx, 'data> {
    pub fn submit(&self) -> TileDBResult<()> {
        self.base.do_submit()
    }
}

impl<'ctx, 'data> WriteQuery<'ctx, 'data> {
    pub fn finalize(self) -> TileDBResult<Array<'ctx>> {
        let c_context = self.context().capi();
        let c_query = **self.base().cquery();
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

impl<'ctx, 'data> QueryBuilder<'ctx> for WriteBuilder<'ctx, 'data> {
    type Query = WriteQuery<'ctx, 'data>;

    fn base(&self) -> &BuilderBase<'ctx> {
        &self.base
    }

    fn build(self) -> Self::Query {
        WriteQuery {
            base: self.base.build(),
            _inputs: self.inputs,
        }
    }
}

impl<'ctx, 'data> WriteBuilder<'ctx, 'data> {
    pub fn new(array: Array<'ctx>) -> TileDBResult<Self> {
        Ok(WriteBuilder {
            base: BuilderBase::new(array, QueryType::Write)?,
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
        QueryBuffers<'data, <T as DataProvider>::Unit>:
            Into<TypedQueryBuffers<'data>>,
    {
        let field = field.as_ref();
        let input = data.as_tiledb_input();

        let c_context = self.context().capi();
        let c_query = **self.base().cquery();
        let c_name = cstring!(field);

        let mut data_size = Box::pin(input.data.size() as u64);

        self.capi_return(unsafe {
            let c_bufptr =
                input.data.as_ref().as_ptr() as *mut std::ffi::c_void;
            let c_sizeptr = data_size.as_mut().get_mut() as *mut u64;

            ffi::tiledb_query_set_data_buffer(
                c_context,
                c_query,
                c_name.as_ptr(),
                c_bufptr,
                c_sizeptr,
            )
        })?;

        let mut offsets_size = input
            .cell_offsets
            .as_ref()
            .map(|b| Box::pin(b.size() as u64));

        if let Some(ref mut offsets_size) = offsets_size.as_mut() {
            let c_offptr =
                input.cell_offsets.as_ref().unwrap().as_ref().as_ptr()
                    as *mut u64;
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

        let mut validity_size =
            input.validity.as_ref().map(|b| Box::pin(b.size() as u64));

        if let Some(ref mut validity_size) = validity_size.as_mut() {
            let c_validityptr =
                input.validity.as_ref().unwrap().as_ref().as_ptr() as *mut u8;
            let c_sizeptr = validity_size.as_mut().get_mut() as *mut u64;

            self.capi_return(unsafe {
                ffi::tiledb_query_set_validity_buffer(
                    c_context,
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
            _input: input.into(),
        };

        self.inputs.insert(String::from(field), raw_write_input);

        Ok(self)
    }
}
