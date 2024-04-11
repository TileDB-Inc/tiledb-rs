use std::collections::{HashMap, HashSet};
use std::ops::Deref;

use anyhow::anyhow;

use crate::context::{CApiInterface, Context, ContextBound};
use crate::convert::CAPIConverter;
use crate::error::Error;
use crate::{Array, Result as TileDBResult};

pub mod subarray;

pub use crate::query::subarray::{Builder as SubarrayBuilder, Subarray};

pub type QueryType = crate::array::Mode;
pub type QueryLayout = crate::array::CellOrder;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryStatus {
    Failed,
    Completed,
    InProgress,
    Incomplete,
    Uninitialized,
    Initialized,
}

impl TryFrom<ffi::tiledb_query_status_t> for QueryStatus {
    type Error = crate::error::Error;
    fn try_from(value: ffi::tiledb_query_status_t) -> TileDBResult<Self> {
        match value {
            ffi::tiledb_query_status_t_TILEDB_FAILED => Ok(QueryStatus::Failed),
            ffi::tiledb_query_status_t_TILEDB_COMPLETED => {
                Ok(QueryStatus::Completed)
            }
            ffi::tiledb_query_status_t_TILEDB_INPROGRESS => {
                Ok(QueryStatus::InProgress)
            }
            ffi::tiledb_query_status_t_TILEDB_INCOMPLETE => {
                Ok(QueryStatus::Incomplete)
            }
            ffi::tiledb_query_status_t_TILEDB_UNINITIALIZED => {
                Ok(QueryStatus::Uninitialized)
            }
            ffi::tiledb_query_status_t_TILEDB_INITIALIZED => {
                Ok(QueryStatus::Initialized)
            }
            _ => Err(Self::Error::LibTileDB(format!(
                "Invalid array type: {}",
                value
            ))),
        }
    }
}

pub(crate) enum RawQuery {
    Owned(*mut ffi::tiledb_query_t),
}

impl Deref for RawQuery {
    type Target = *mut ffi::tiledb_query_t;
    fn deref(&self) -> &Self::Target {
        match *self {
            RawQuery::Owned(ref ffi) => ffi,
        }
    }
}

impl Drop for RawQuery {
    fn drop(&mut self) {
        let RawQuery::Owned(ref mut ffi) = *self;
        unsafe { ffi::tiledb_query_free(ffi) };
    }
}

struct Buffer<'data> {
    pub(crate) _data: &'data [u8],
    pub(crate) data_size: Box<u64>,
    pub(crate) elem_size: u64,
}

pub struct Query<'ctx> {
    context: &'ctx Context,
    array: Array<'ctx>,
    subarrays: Vec<Subarray<'ctx>>,
    raw: RawQuery,
    req_data_buffers: HashSet<String>,
    req_offsets_buffers: HashSet<String>,
}

impl<'ctx> ContextBound<'ctx> for Query<'ctx> {
    fn context(&self) -> &'ctx Context {
        self.context
    }
}

impl<'ctx> Query<'ctx> {
    pub(crate) fn capi(&self) -> *mut ffi::tiledb_query_t {
        *self.raw
    }

    pub fn executor<'data>(&'ctx self) -> Executor<'ctx, 'data> {
        Executor {
            query: self,
            data_buffers: HashMap::new(),
            offset_buffers: HashMap::new(),
            rexec: false,
        }
    }
}

pub struct QueryResult {
    status: QueryStatus,
    sizes: HashMap<String, (u64, Option<u64>)>,
}

impl QueryResult {
    pub fn status(&self) -> QueryStatus {
        self.status
    }

    pub fn completed(&self) -> bool {
        self.status == QueryStatus::Completed
    }

    pub fn sizes(&self) -> &HashMap<String, (u64, Option<u64>)> {
        &self.sizes
    }
}

pub struct Executor<'ctx, 'data> {
    query: &'ctx Query<'ctx>,
    data_buffers: HashMap<String, Buffer<'data>>,
    offset_buffers: HashMap<String, Buffer<'data>>,
    rexec: bool,
}

impl<'ctx, 'data> ContextBound<'ctx> for Executor<'ctx, 'data> {
    fn context(&self) -> &'ctx Context {
        self.query.context()
    }
}

impl<'ctx, 'data> Executor<'ctx, 'data> {
    pub fn submit(&self) -> TileDBResult<QueryResult> {
        self.check_required_buffers()?;
        let c_context = self.query.context.capi();
        let c_query = self.query.capi();
        self.capi_return(unsafe {
            ffi::tiledb_query_submit(c_context, c_query)
        })?;

        let mut c_status: ffi::tiledb_query_status_t = 0;
        self.capi_return(unsafe {
            ffi::tiledb_query_get_status(c_context, c_query, &mut c_status)
        })?;

        let sizes = self.result_sizes();

        Ok(QueryResult {
            status: QueryStatus::try_from(c_status)?,
            sizes,
        })
    }

    pub fn finalize(self) -> TileDBResult<()> {
        self.capi_return(unsafe {
            ffi::tiledb_query_finalize(
                self.query.context.capi(),
                self.query.capi(),
            )
        })?;

        Ok(())
    }

    pub fn set_data_buffer<Conv: CAPIConverter>(
        mut self,
        name: &str,
        data: &'data mut [Conv],
    ) -> TileDBResult<Self> {
        self.check_buffer_name(
            name,
            &self.data_buffers,
            &self.query.req_data_buffers,
        )?;

        let c_name = cstring!(name);
        let mut size =
            Box::new(std::mem::size_of_val(data).try_into().map_err(
                |e: <u64 as std::convert::TryFrom<usize>>::Error| {
                    Error::InvalidArgument(anyhow!(e))
                },
            )?);

        let val = &mut *size;

        self.capi_return(unsafe {
            ffi::tiledb_query_set_data_buffer(
                self.query.context.capi(),
                self.query.capi(),
                c_name.as_ptr(),
                data.as_mut_ptr() as *mut std::ffi::c_void,
                val,
            )
        })?;

        // Create and store the data buffer
        let data = unsafe {
            std::slice::from_raw_parts(
                data.as_ptr() as *mut u8,
                std::mem::size_of_val(data),
            )
        };

        self.data_buffers.insert(
            name.to_owned(),
            Buffer {
                _data: data,
                data_size: size,
                elem_size: std::mem::size_of::<Conv>() as u64,
            },
        );

        Ok(self)
    }

    pub fn set_offsets_buffer(
        mut self,
        name: &str,
        data: &'data mut [u64],
    ) -> TileDBResult<Self> {
        self.check_buffer_name(
            name,
            &self.offset_buffers,
            &self.query.req_offsets_buffers,
        )?;

        let c_name = cstring!(name);
        let mut size =
            Box::new(std::mem::size_of_val(data).try_into().map_err(
                |e: <u64 as std::convert::TryFrom<usize>>::Error| {
                    Error::InvalidArgument(anyhow!(e))
                },
            )?);

        self.capi_return(unsafe {
            ffi::tiledb_query_set_offsets_buffer(
                self.query.context.capi(),
                self.query.capi(),
                c_name.as_ptr(),
                data.as_mut_ptr(),
                &mut *size,
            )
        })?;

        // Create and store the data buffer
        let data = unsafe {
            std::slice::from_raw_parts(
                data.as_ptr() as *mut u8,
                std::mem::size_of_val(data),
            )
        };

        self.offset_buffers.insert(
            name.to_owned(),
            Buffer {
                _data: data,
                data_size: size,
                elem_size: std::mem::size_of::<u64>() as u64,
            },
        );

        Ok(self)
    }

    fn result_sizes(&self) -> HashMap<String, (u64, Option<u64>)> {
        if self.data_buffers.is_empty() {
            return HashMap::new();
        }

        let mut ret = HashMap::new();

        for (name, data_buffer) in self.data_buffers.iter() {
            let offsets_buffer = self.offset_buffers.get(name);
            if offsets_buffer.is_some() {
                let offsets_buffer = offsets_buffer.unwrap();
                let num_elems =
                    *offsets_buffer.data_size / offsets_buffer.elem_size;
                let var_size = *data_buffer.data_size / data_buffer.elem_size;
                ret.insert(name.clone(), (num_elems, Some(var_size)));
            } else {
                let num_elems = *data_buffer.data_size / data_buffer.elem_size;
                ret.insert(name.clone(), (num_elems, None));
            }
        }

        ret
    }

    fn check_buffer_name(
        &self,
        name: &str,
        buffers: &HashMap<String, Buffer<'data>>,
        required: &HashSet<String>,
    ) -> TileDBResult<()> {
        if self.rexec && !required.contains(name) {
            return Err(Error::Other(format!(
                "Buffer '{}' was not set on previous submission.",
                name
            )));
        }

        if buffers.contains_key(name) {
            return Err(Error::Other(format!(
                "Buffer '{}' was already set.",
                name
            )));
        }

        Ok(())
    }

    fn check_required_buffers(&self) -> TileDBResult<()> {
        if !self.rexec {
            return Ok(());
        }

        let mut unset = Vec::new();

        for name in self.query.req_data_buffers.iter() {
            if !self.data_buffers.contains_key(name) {
                unset.push(name.clone());
            }
        }

        if !unset.is_empty() {
            let names = unset.join(", ");
            return Err(Error::Other(format!(
                "Missing required data buffers for resubmission: {}",
                names
            )));
        }

        for name in self.query.req_offsets_buffers.iter() {
            if !self.offset_buffers.contains_key(name) {
                unset.push(name.clone());
            }
        }

        if !unset.is_empty() {
            let names = unset.join(", ");
            return Err(Error::Other(format!(
                "Missing required offset buffers for resubmission: {}",
                names
            )));
        }

        Ok(())
    }
}

pub struct Builder<'ctx> {
    query: Query<'ctx>,
}

impl<'ctx> ContextBound<'ctx> for Builder<'ctx> {
    fn context(&self) -> &'ctx Context {
        self.query.context()
    }
}

impl<'ctx> Builder<'ctx> {
    pub fn new(
        context: &'ctx Context,
        array: Array<'ctx>,
        query_type: QueryType,
    ) -> TileDBResult<Self> {
        let c_context = context.capi();
        let c_array = array.capi();
        let c_query_type = query_type.capi_enum();
        let mut c_query: *mut ffi::tiledb_query_t = out_ptr!();
        context.capi_return(unsafe {
            ffi::tiledb_query_alloc(
                c_context,
                c_array,
                c_query_type,
                &mut c_query,
            )
        })?;
        Ok(Builder {
            query: Query {
                context,
                array,
                subarrays: vec![],
                raw: RawQuery::Owned(c_query),
                req_data_buffers: HashSet::new(),
                req_offsets_buffers: HashSet::new(),
            },
        })
    }

    pub fn layout(self, layout: QueryLayout) -> TileDBResult<Self> {
        let c_context = self.query.context.capi();
        let c_query = *self.query.raw;
        let c_layout = layout.capi_enum();
        self.capi_return(unsafe {
            ffi::tiledb_query_set_layout(c_context, c_query, c_layout)
        })?;
        Ok(self)
    }

    pub fn add_subarray(self) -> TileDBResult<SubarrayBuilder<'ctx>> {
        SubarrayBuilder::for_query(self)
    }

    pub fn read_all(self) -> TileDBResult<Self> {
        self.read_all_dimensions()?.read_all_attributes()
    }

    pub fn read_all_dimensions(mut self) -> TileDBResult<Self> {
        let schema = self.query.array.schema()?;
        let domain = schema.domain()?;
        for i in 0..domain.ndim()? {
            let dim = domain.dimension(i)?;
            let name = dim.name()?;
            self.query.req_data_buffers.insert(name.clone());
            if dim.is_var_sized()? {
                self.query.req_offsets_buffers.insert(name.clone());
            }
        }

        Ok(self)
    }

    pub fn read_all_attributes(mut self) -> TileDBResult<Self> {
        let schema = self.query.array.schema()?;
        for i in 0..schema.nattributes()? {
            let attr = schema.attribute(i)?;
            let name = attr.name()?;
            self.query.req_data_buffers.insert(name.clone());
            if attr.is_var_sized()? {
                self.query.req_offsets_buffers.insert(name.clone());
            }
        }

        Ok(self)
    }

    pub fn read_dimensions<T: AsRef<str>>(
        mut self,
        names: &[T],
    ) -> TileDBResult<Self> {
        let domain = self.query.array.schema()?.domain()?;
        for name in names {
            let dim = domain.dimension(name.as_ref())?;
            self.query.req_data_buffers.insert(name.as_ref().into());
            if dim.is_var_sized()? {
                self.query.req_offsets_buffers.insert(name.as_ref().into());
            }
        }

        Ok(self)
    }

    pub fn read_attributes<T: AsRef<str>>(
        mut self,
        names: &[T],
    ) -> TileDBResult<Self> {
        let schema = self.query.array.schema()?;
        for name in names {
            let attr = schema.attribute(name.as_ref())?;
            self.query.req_data_buffers.insert(name.as_ref().into());
            if attr.is_var_sized()? {
                self.query.req_offsets_buffers.insert(name.as_ref().into());
            }
        }

        Ok(self)
    }

    pub fn build(self) -> Query<'ctx> {
        self.query
    }
}
