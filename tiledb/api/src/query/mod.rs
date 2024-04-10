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
    query: Query<'ctx>,
    data_buffers: HashMap<String, Buffer<'data>>,
    req_data_buffers: HashSet<String>,
    offset_buffers: HashMap<String, Buffer<'data>>,
    req_offset_buffers: HashSet<String>,
    rexec: bool,
}

impl<'ctx, 'data> ContextBound<'ctx> for Executor<'ctx, 'data> {
    fn context(&self) -> &'ctx Context {
        self.query.context()
    }
}

impl<'ctx, 'data> Executor<'ctx, 'data> {
    pub fn submit(&self) -> TileDBResult<QueryResult> {
        self.check_resubmission()?;
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

    pub fn reset<'new_data>(self) -> Executor<'ctx, 'new_data> {
        let mut req_data = HashSet::new();
        let mut req_offsets = HashSet::new();
        for (key, info) in self.result_sizes().iter() {
            req_data.insert(key.clone());
            if info.1.is_some() {
                req_offsets.insert(key.clone());
            }
        }
        Executor::<'ctx, 'new_data> {
            query: self.query,
            data_buffers: HashMap::new(),
            req_data_buffers: req_data,
            offset_buffers: HashMap::new(),
            req_offset_buffers: req_offsets,
            rexec: true,
        }
    }

    pub fn set_data_buffer<Conv: CAPIConverter>(
        mut self,
        name: &str,
        data: &'data mut [Conv],
    ) -> TileDBResult<Self> {
        self.check_buffer_name(
            name,
            &self.data_buffers,
            &self.req_data_buffers,
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
            &self.req_offset_buffers,
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

    fn check_resubmission(&self) -> TileDBResult<()> {
        if !self.rexec {
            return Ok(());
        }

        let mut unset = Vec::new();

        for name in self.req_data_buffers.iter() {
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

        for name in self.req_offset_buffers.iter() {
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

    pub fn executor<'data>(self) -> Executor<'ctx, 'data> {
        Executor {
            query: self.query,
            data_buffers: HashMap::new(),
            req_data_buffers: HashSet::new(),
            offset_buffers: HashMap::new(),
            req_offset_buffers: HashSet::new(),
            rexec: false,
        }
    }
}

// impl<'ctx, 'data> From<Builder<'ctx, 'data>> for Query<'ctx> {
//     fn from(builder: Builder<'ctx, 'data>) -> Query<'ctx> {
//         builder.build()
//     }
// }
