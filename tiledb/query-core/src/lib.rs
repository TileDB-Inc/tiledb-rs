//! The TileDB Query interface and supporting utilities
extern crate tiledb_sys as ffi;

use std::collections::HashMap;
use std::ops::Deref;

use thiserror::Error;
use tiledb_common::{single_value_range_go, var_value_range_go};

use tiledb_api::array::Array;
use tiledb_api::config::Config;
use tiledb_api::context::{CApiInterface, Context, ContextBound};
use tiledb_api::error::Error as TileDBError;
use tiledb_api::key::LookupKey;
use tiledb_api::query::conditions::QueryConditionExpr;
use tiledb_common::range::{Range, SingleValueRange, VarValueRange};

use buffers::{Error as QueryBuffersError, QueryBuffers};
use fields::{QueryFields, QueryFieldsBuilderForQuery};
use subarray::{SubarrayBuilderForQuery, SubarrayData};

pub use buffers::SharedBuffers;

pub mod arrow;
pub mod buffers;
pub mod fields;
pub mod subarray;

pub type QueryType = tiledb_common::array::Mode;
pub type QueryLayout = tiledb_common::array::CellOrder;

macro_rules! out_ptr {
    () => {
        unsafe { std::mem::MaybeUninit::zeroed().assume_init() }
    };
}

/// Errors related to query creation and execution
#[derive(Debug, Error)]
pub enum Error {
    #[error("Incompatible buffer specification when replacing buffers.")]
    IncompatibleReplacementBuffers,
    #[error("Internal TileDB Error: {0}")]
    InternalError(String),
    #[error("Invalid string for C API calls: {0}")]
    NulError(#[from] std::ffi::NulError),
    #[error("Error building query buffers: {0}")]
    QueryBuffersError(#[from] QueryBuffersError),
    #[error("Dimension range error: {0}")]
    DimensionRangeError(
        #[from] tiledb_common::range::DimensionCompatibilityError,
    ),
    #[error("Encountered internal libtiledb error: {0}")]
    TileDBError(#[from] TileDBError),
}

impl From<Error> for TileDBError {
    fn from(err: Error) -> TileDBError {
        TileDBError::Other(format!("{err}"))
    }
}

type Result<T> = std::result::Result<T, Error>;

/// The status of a query submission
///
/// Note that BuffersTooSmall is a Rust invention. But given that we never
/// attempt to translate this status object back into a capi value its fine.
pub enum QueryStatus {
    Uninitialized,
    Initialized,
    InProgress,
    Incomplete,
    BuffersTooSmall,
    Completed,
    Failed,
}

impl QueryStatus {
    pub fn is_complete(&self) -> bool {
        matches!(self, QueryStatus::Completed)
    }

    pub fn has_data(&self) -> bool {
        !matches!(self, QueryStatus::BuffersTooSmall)
    }
}

impl TryFrom<ffi::tiledb_query_status_t> for QueryStatus {
    type Error = Error;
    fn try_from(status: ffi::tiledb_query_status_t) -> Result<Self> {
        match status {
            ffi::tiledb_query_status_t_TILEDB_UNINITIALIZED => {
                Ok(QueryStatus::Uninitialized)
            }
            ffi::tiledb_query_status_t_TILEDB_INITIALIZED => {
                Ok(QueryStatus::Initialized)
            }
            ffi::tiledb_query_status_t_TILEDB_INPROGRESS => {
                Ok(QueryStatus::InProgress)
            }
            ffi::tiledb_query_status_t_TILEDB_INCOMPLETE => {
                Ok(QueryStatus::Incomplete)
            }
            ffi::tiledb_query_status_t_TILEDB_COMPLETED => {
                Ok(QueryStatus::Completed)
            }
            ffi::tiledb_query_status_t_TILEDB_FAILED => Ok(QueryStatus::Failed),
            invalid => Err(Error::InternalError(format!(
                "Invaldi query status: {}",
                invalid
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
        let RawQuery::Owned(ref ffi) = self;
        ffi
    }
}

impl Drop for RawQuery {
    fn drop(&mut self) {
        let RawQuery::Owned(ref mut ffi) = *self;
        unsafe { ffi::tiledb_query_free(ffi) }
    }
}

pub(crate) enum RawSubarray {
    Owned(*mut ffi::tiledb_subarray_t),
}

impl Deref for RawSubarray {
    type Target = *mut ffi::tiledb_subarray_t;
    fn deref(&self) -> &Self::Target {
        match *self {
            RawSubarray::Owned(ref ffi) => ffi,
        }
    }
}

impl Drop for RawSubarray {
    fn drop(&mut self) {
        let RawSubarray::Owned(ref mut ffi) = *self;
        unsafe { ffi::tiledb_subarray_free(ffi) };
    }
}

/// The main Query interface
///
/// This struct is responsible for executing queries against TileDB arrays.
pub struct Query {
    context: Context,
    raw: RawQuery,
    query_type: QueryType,
    array: Array,
    buffers: QueryBuffers,
}

impl ContextBound for Query {
    fn context(&self) -> Context {
        self.array.context()
    }
}

impl Query {
    pub(crate) fn capi(&mut self) -> *mut ffi::tiledb_query_t {
        *self.raw
    }

    pub fn submit(&mut self) -> Result<QueryStatus> {
        self.buffers.make_mut()?;
        if matches!(self.query_type, QueryType::Read) {
            self.buffers.reset_lengths()?;
        }
        self.set_buffers()?;

        let c_query = self.capi();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_query_submit(ctx, c_query)
        })?;

        if matches!(self.query_type, QueryType::Read) {
            self.buffers.shrink_lengths()?;
        }

        match self.curr_status()? {
            QueryStatus::Uninitialized
            | QueryStatus::Initialized
            | QueryStatus::InProgress => Err(Error::InternalError(
                "Invalid query status after submit".to_string(),
            )),
            QueryStatus::Failed => Err(self.context.expect_last_error().into()),
            QueryStatus::Incomplete => {
                if self.buffers.iter().any(|(_, b)| b.len() > 0) {
                    Ok(QueryStatus::Incomplete)
                } else {
                    Ok(QueryStatus::BuffersTooSmall)
                }
            }
            QueryStatus::BuffersTooSmall => {
                panic!("TileDB does not generate this variant.")
            }
            QueryStatus::Completed => Ok(QueryStatus::Completed),
        }
    }

    pub fn finalize(mut self) -> Result<(Array, SharedBuffers)> {
        let c_query = self.capi();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_query_finalize(ctx, c_query)
        })?;

        self.buffers.make_shared()?;
        let mut ret = HashMap::with_capacity(self.buffers.len());
        for (field, buffer) in self.buffers.iter() {
            ret.insert(field.clone(), buffer.as_shared()?);
        }

        Ok((self.array, ret.into()))
    }

    pub fn buffers(&mut self) -> Result<SharedBuffers> {
        self.buffers.make_shared()?;
        let mut ret = HashMap::with_capacity(self.buffers.len());
        for (field, buffer) in self.buffers.iter() {
            ret.insert(field.clone(), buffer.as_shared()?);
        }

        Ok(ret.into())
    }

    /// Replace this queries buffers with a new set specified by fields
    ///
    /// This can be used to reallocate buffers with a larger capacity.
    pub fn replace_buffers(
        &mut self,
        fields: QueryFields,
    ) -> Result<QueryBuffers> {
        let mut tmp_buffers =
            QueryBuffers::from_fields(self.array.schema()?, fields)?;
        tmp_buffers.make_mut()?;
        if self.buffers.is_compatible(&tmp_buffers) {
            std::mem::swap(&mut self.buffers, &mut tmp_buffers);
            Ok(tmp_buffers)
        } else {
            Err(Error::IncompatibleReplacementBuffers)
        }
    }

    fn set_buffers(&mut self) -> Result<()> {
        let c_query = self.capi();
        for (field, buffer) in self.buffers.iter_mut() {
            let c_name = std::ffi::CString::new(field.as_bytes())?;

            {
                let data = buffer.data_mut()?;
                let c_data_ptr = data.data_ptr();
                let c_data_size_ptr = data.size_ptr();

                self.context.capi_call(|ctx| unsafe {
                    ffi::tiledb_query_set_data_buffer(
                        ctx,
                        c_query,
                        c_name.as_ptr(),
                        c_data_ptr,
                        c_data_size_ptr,
                    )
                })?;
            }

            {
                // NB: `if let` binding is longer than it looks
                if let Some(offsets) = buffer.offsets_mut()? {
                    let c_offsets_ptr = offsets.offsets_ptr();
                    let c_offsets_size_ptr = offsets.size_ptr();

                    self.context.capi_call(|ctx| unsafe {
                        ffi::tiledb_query_set_offsets_buffer(
                            ctx,
                            c_query,
                            c_name.as_ptr(),
                            c_offsets_ptr,
                            c_offsets_size_ptr,
                        )
                    })?;
                }
            }

            if let Some(validity) = buffer.validity_mut()? {
                let c_validity_ptr = validity.validity_ptr();
                let c_validity_size_ptr = validity.size_ptr();
                self.context.capi_call(|ctx| unsafe {
                    ffi::tiledb_query_set_validity_buffer(
                        ctx,
                        c_query,
                        c_name.as_ptr(),
                        c_validity_ptr,
                        c_validity_size_ptr,
                    )
                })?;
            }
        }
        Ok(())
    }

    fn curr_status(&mut self) -> Result<QueryStatus> {
        let c_query = self.capi();
        let mut c_status: ffi::tiledb_query_status_t = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_query_get_status(ctx, c_query, &mut c_status)
        })?;

        QueryStatus::try_from(c_status)
    }
}

/// The main interface to creating Query instances
pub struct QueryBuilder {
    context: Context,
    array: Array,
    query_type: QueryType,
    config: Option<Config>,
    layout: Option<QueryLayout>,
    subarray: Option<SubarrayData>,
    query_condition: Option<QueryConditionExpr>,
    fields: QueryFields,
}

impl ContextBound for QueryBuilder {
    fn context(&self) -> Context {
        self.context.clone()
    }
}

impl QueryBuilder {
    pub fn new(array: Array, query_type: QueryType) -> Self {
        Self {
            context: array.context(),
            array,
            query_type,
            config: None,
            layout: None,
            subarray: None,
            query_condition: None,
            fields: Default::default(),
        }
    }

    pub fn read(array: Array) -> Self {
        Self::new(array, QueryType::Read)
    }

    pub fn write(array: Array) -> Self {
        Self::new(array, QueryType::Write)
    }

    pub fn build(mut self) -> Result<Query> {
        let raw = self.alloc_query()?;

        let schema = self.array.schema()?;
        self.set_config(&raw)?;
        self.set_layout(&raw)?;
        self.set_subarray(&raw)?;
        self.set_query_condition(&raw)?;

        Ok(Query {
            context: self.array.context(),
            raw,
            query_type: self.query_type,
            array: self.array,
            buffers: QueryBuffers::from_fields(schema, self.fields)?,
        })
    }

    pub fn with_config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }

    pub fn with_layout(mut self, layout: QueryLayout) -> Self {
        self.layout = Some(layout);
        self
    }

    pub fn with_query_condition(
        mut self,
        query_condition: QueryConditionExpr,
    ) -> Self {
        self.query_condition = Some(query_condition);
        self
    }

    pub fn with_subarray_data(mut self, subarray: SubarrayData) -> Self {
        self.subarray = Some(subarray);
        self
    }

    pub fn start_subarray(self) -> SubarrayBuilderForQuery {
        SubarrayBuilderForQuery::new(self)
    }

    pub fn with_fields(mut self, fields: QueryFields) -> Self {
        self.fields = fields;
        self
    }

    pub fn start_fields(self) -> QueryFieldsBuilderForQuery {
        QueryFieldsBuilderForQuery::new(self)
    }

    // Internal builder methods below

    fn alloc_query(&self) -> Result<RawQuery> {
        let c_array = **self.array.capi();
        let c_query_type = ffi::tiledb_query_type_t::from(self.query_type);
        let mut c_query: *mut ffi::tiledb_query_t = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_query_alloc(ctx, c_array, c_query_type, &mut c_query)
        })?;

        let raw = RawQuery::Owned(c_query);

        // configure the query to use arrow-shaped offsets
        let mut qconf = Config::new()?;
        qconf.set("sm.var_offsets.bitsize", "64")?;
        qconf.set("sm.var_offsets.mode", "elements")?;
        qconf.set("sm.var_offsets.extra_element", "true")?;

        self.capi_call(|c_context| unsafe {
            ffi::tiledb_query_set_config(c_context, c_query, qconf.capi())
        })?;

        Ok(raw)
    }

    fn alloc_subarray(&self) -> Result<RawSubarray> {
        let c_array = **self.array.capi();
        let mut c_subarray: *mut ffi::tiledb_subarray_t = out_ptr!();

        self.capi_call(|ctx| unsafe {
            ffi::tiledb_subarray_alloc(ctx, c_array, &mut c_subarray)
        })?;

        Ok(RawSubarray::Owned(c_subarray))
    }

    fn set_config(&mut self, raw: &RawQuery) -> Result<()> {
        if self.config.is_none() {
            return Ok(());
        }

        // TODO: Reject configurations that will break out buffer management
        // logic. Specifically, the various sm.var_offsets.* keys.
        let c_query = **raw;
        let c_cfg = self.config.as_mut().unwrap().capi();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_query_set_config(ctx, c_query, c_cfg)
        })?;

        Ok(())
    }

    fn set_layout(&mut self, raw: &RawQuery) -> Result<()> {
        let Some(layout) = self.layout.as_ref() else {
            return Ok(());
        };

        let c_query = **raw;
        let c_layout = ffi::tiledb_layout_t::from(*layout);
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_query_set_layout(ctx, c_query, c_layout)
        })?;

        Ok(())
    }

    fn set_subarray(&self, raw: &RawQuery) -> Result<()> {
        let Some(subarray_data) = self.subarray.as_ref() else {
            return Ok(());
        };

        let raw_subarray = self.alloc_subarray()?;
        for (key, ranges) in subarray_data.iter() {
            for range in ranges {
                self.set_subarray_range(*raw_subarray, &key.into(), range)?;
            }
        }

        let c_query = **raw;
        let c_subarray = *raw_subarray;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_query_set_subarray_t(ctx, c_query, c_subarray)
        })?;

        Ok(())
    }

    fn set_subarray_range(
        &self,
        c_subarray: *mut ffi::tiledb_subarray_t,
        key: &LookupKey,
        range: &Range,
    ) -> Result<()> {
        let schema = self.array.schema()?;
        let dim = schema.domain()?.dimension(key.clone())?;

        range.check_dimension_compatibility(
            dim.datatype()?,
            dim.cell_val_num()?,
        )?;

        match range {
            Range::Single(range) => {
                single_value_range_go!(range, _DT, start, end, {
                    let start = start.to_le_bytes();
                    let end = end.to_le_bytes();
                    match key {
                        LookupKey::Index(idx) => {
                            self.capi_call(|ctx| unsafe {
                                ffi::tiledb_subarray_add_range(
                                    ctx,
                                    c_subarray,
                                    *idx as u32,
                                    start.as_ptr() as *const std::ffi::c_void,
                                    end.as_ptr() as *const std::ffi::c_void,
                                    std::ptr::null(),
                                )
                            })?;
                        }
                        LookupKey::Name(name) => {
                            let c_name = std::ffi::CString::new(name.clone())?;
                            self.capi_call(|ctx| unsafe {
                                ffi::tiledb_subarray_add_range_by_name(
                                    ctx,
                                    c_subarray,
                                    c_name.as_ptr(),
                                    start.as_ptr() as *const std::ffi::c_void,
                                    end.as_ptr() as *const std::ffi::c_void,
                                    std::ptr::null(),
                                )
                            })?;
                        }
                    }
                })
            }
            Range::Multi(_) => unreachable!(
                "This is rejected by range.check_dimension_compatibility"
            ),
            Range::Var(range) => {
                var_value_range_go!(range, _DT, start, end, {
                    match key {
                        LookupKey::Index(idx) => {
                            self.capi_call(|ctx| unsafe {
                                ffi::tiledb_subarray_add_range_var(
                                    ctx,
                                    c_subarray,
                                    *idx as u32,
                                    start.as_ptr() as *const std::ffi::c_void,
                                    start.len() as u64,
                                    end.as_ptr() as *const std::ffi::c_void,
                                    end.len() as u64,
                                )
                            })?;
                        }
                        LookupKey::Name(name) => {
                            let c_name = std::ffi::CString::new(name.clone())?;
                            self.capi_call(|ctx| unsafe {
                                ffi::tiledb_subarray_add_range_var_by_name(
                                    ctx,
                                    c_subarray,
                                    c_name.as_ptr(),
                                    start.as_ptr() as *const std::ffi::c_void,
                                    start.len() as u64,
                                    end.as_ptr() as *const std::ffi::c_void,
                                    end.len() as u64,
                                )
                            })?;
                        }
                    }
                })
            }
        }

        Ok(())
    }

    fn set_query_condition(&self, raw: &RawQuery) -> Result<()> {
        let Some(query_condition) = self.query_condition.as_ref() else {
            return Ok(());
        };

        let cq_raw = query_condition.build(&self.context)?;
        let c_query = **raw;
        let c_cond = *cq_raw;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_query_set_condition(ctx, c_query, c_cond)
        })?;

        Ok(())
    }
}
