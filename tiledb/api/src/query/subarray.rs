use std::ops::Deref;

use anyhow::anyhow;

use super::traits::{Query, QueryBuilder};
use crate::array::{CellValNum, Schema};
use crate::context::{CApiInterface, Context, ContextBound};
use crate::convert::CAPISameRepr;
use crate::error::Error;
use crate::key::LookupKey;
use crate::range::{Range, SingleValueRange, TypedRange, VarValueRange};
use crate::Result as TileDBResult;
use crate::{single_value_range_go, var_value_range_go};

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

pub struct Subarray {
    context: Context,
    raw: RawSubarray,
}

impl ContextBound for Subarray {
    fn context(&self) -> &Context {
        &self.context
    }
}

impl Subarray {
    pub(crate) fn capi(&self) -> *mut ffi::tiledb_subarray_t {
        *self.raw
    }

    pub(crate) fn new(context: &Context, raw: RawSubarray) -> Self {
        Subarray {
            context: context.clone(),
            raw,
        }
    }

    /// Return all dimension ranges set on the query.
    pub fn ranges(&self, schema: &Schema) -> TileDBResult<Vec<Vec<Range>>> {
        let ctx = self.context();
        let ndims = schema.domain()?.ndim()? as u32;

        let c_subarray = self.capi();

        let mut ranges: Vec<Vec<Range>> = Vec::new();
        for dim_idx in 0..ndims {
            let mut nranges: u64 = 0;
            ctx.capi_call(|ctx| unsafe {
                ffi::tiledb_subarray_get_range_num(
                    ctx,
                    c_subarray,
                    dim_idx,
                    &mut nranges,
                )
            })?;

            let dim = schema.domain()?.dimension(dim_idx)?;
            let var_sized_dim = dim.is_var_sized()?;

            let mut dim_ranges: Vec<Range> = Vec::new();
            for rng_idx in 0..nranges {
                if var_sized_dim {
                    let mut start_size: u64 = 0;
                    let mut end_size: u64 = 0;
                    ctx.capi_call(|ctx| unsafe {
                        ffi::tiledb_subarray_get_range_var_size(
                            ctx,
                            c_subarray,
                            dim_idx,
                            rng_idx,
                            &mut start_size,
                            &mut end_size,
                        )
                    })?;

                    let start =
                        vec![0u8; start_size as usize].into_boxed_slice();
                    let end = vec![0u8; end_size as usize].into_boxed_slice();

                    ctx.capi_call(|ctx| unsafe {
                        ffi::tiledb_subarray_get_range_var(
                            ctx,
                            c_subarray,
                            dim_idx,
                            rng_idx,
                            start.as_ptr() as *mut std::ffi::c_void,
                            end.as_ptr() as *mut std::ffi::c_void,
                        )
                    })?;

                    let dtype = dim.datatype()?;
                    let cvn = dim.cell_val_num()?;
                    let range =
                        TypedRange::from_slices(dtype, cvn, &start, &end)?
                            .range;
                    dim_ranges.push(range);
                } else {
                    let dtype = dim.datatype()?;
                    let cvn = dim.cell_val_num()?;
                    let size = match cvn {
                        CellValNum::Fixed(cvn) => {
                            cvn.get() as u64 * dtype.size()
                        }
                        // Unreachable becuase we're in !var_sized_dim
                        CellValNum::Var => unreachable!(),
                    };

                    let start = vec![0u8; size as usize].into_boxed_slice();
                    let end = vec![0u8; size as usize].into_boxed_slice();

                    // Apparently stride exists in the API but isn't used.
                    let mut stride: *const std::ffi::c_void = out_ptr!();

                    ctx.capi_call(|ctx| unsafe {
                        ffi::tiledb_subarray_get_range(
                            ctx,
                            c_subarray,
                            dim_idx,
                            rng_idx,
                            start.as_ptr() as *mut *const std::ffi::c_void,
                            end.as_ptr() as *mut *const std::ffi::c_void,
                            &mut stride,
                        )
                    })?;

                    let range =
                        TypedRange::from_slices(dtype, cvn, &start, &end)?
                            .range;
                    dim_ranges.push(range);
                }
            }

            ranges.push(dim_ranges);
        }

        Ok(Vec::new())
    }
}

pub struct Builder<Q> {
    query: Q,
    subarray: Subarray,
}

impl<Q> ContextBound for Builder<Q> {
    fn context(&self) -> &Context {
        self.subarray.context()
    }
}

impl<Q> Builder<Q>
where
    Q: QueryBuilder + Sized,
{
    pub(crate) fn for_query(query: Q) -> TileDBResult<Self> {
        let context = query.context().clone();
        let c_array = query.array().capi();
        let mut c_subarray: *mut ffi::tiledb_subarray_t = out_ptr!();

        context.capi_call(|ctx| unsafe {
            ffi::tiledb_subarray_alloc(ctx, c_array, &mut c_subarray)
        })?;

        Ok(Builder {
            query,
            subarray: Subarray::new(&context, RawSubarray::Owned(c_subarray)),
        })
    }

    /// Add a range on a dimension to the subarray. Adding a range restricts
    /// how much data TileDB has to read from disk to complete a query.
    pub fn add_range<Key: Into<LookupKey> + Clone, IntoRange: Into<Range>>(
        self,
        key: Key,
        range: IntoRange,
    ) -> TileDBResult<Self> {
        // Get the dimension so that we can assert the correct Range type.
        let schema = self.query.array().schema()?;
        let dim = schema.domain()?.dimension(key.clone())?;

        let range = range.into();
        range
            .check_dimension_compatibility(dim.datatype()?, dim.cell_val_num()?)
            .map_err(|e| {
                Error::InvalidArgument(
                    anyhow!("Invalid range variant for dimension").context(e),
                )
            })?;

        let ctx = self.context();
        let c_subarray = self.subarray.capi();

        match range {
            Range::Single(range) => {
                single_value_range_go!(range, _DT, start, end, {
                    let start = start.to_le_bytes();
                    let end = end.to_le_bytes();
                    match key.into() {
                        LookupKey::Index(idx) => {
                            ctx.capi_call(|ctx| unsafe {
                                ffi::tiledb_subarray_add_range(
                                    ctx,
                                    c_subarray,
                                    idx as u32,
                                    start.as_ptr() as *const std::ffi::c_void,
                                    end.as_ptr() as *const std::ffi::c_void,
                                    std::ptr::null(),
                                )
                            })?;
                        }
                        LookupKey::Name(name) => {
                            let c_name = cstring!(name);
                            ctx.capi_call(|ctx| unsafe {
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
                    match key.into() {
                        LookupKey::Index(idx) => {
                            ctx.capi_call(|ctx| unsafe {
                                ffi::tiledb_subarray_add_range_var(
                                    ctx,
                                    c_subarray,
                                    idx as u32,
                                    start.as_ptr() as *const std::ffi::c_void,
                                    start.len() as u64,
                                    end.as_ptr() as *const std::ffi::c_void,
                                    end.len() as u64,
                                )
                            })?;
                        }
                        LookupKey::Name(name) => {
                            let c_name = cstring!(name);
                            ctx.capi_call(|ctx| unsafe {
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

        Ok(self)
    }

    /// Add a list of point ranges to the query.
    pub fn add_point_ranges<Key: Into<LookupKey>, T: CAPISameRepr>(
        self,
        key: Key,
        points: &[T],
    ) -> TileDBResult<Self> {
        let schema = self.query.array().schema()?;
        let dim_idx = schema.domain()?.dimension_index(key)?;

        if points.is_empty() {
            return Err(Error::InvalidArgument(anyhow!(
                "No point ranges provided to set."
            )));
        }

        let c_subarray = self.subarray.capi();

        self.context().capi_call(|ctx| unsafe {
            ffi::tiledb_subarray_add_point_ranges(
                ctx,
                c_subarray,
                dim_idx as u32,
                points.as_ptr() as *const std::ffi::c_void,
                points.len() as u64,
            )
        })?;

        Ok(self)
    }

    /// Apply the subarray to the query, returning the query builder.
    pub fn finish_subarray(self) -> TileDBResult<Q> {
        let c_query = self.query.capi();
        let c_subarray = *self.subarray.raw;

        self.capi_call(|ctx| unsafe {
            ffi::tiledb_query_set_subarray_t(ctx, c_query, c_subarray)
        })?;
        Ok(self.query)
    }
}
