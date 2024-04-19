use std::ops::Deref;

use crate::context::{CApiInterface, Context, ContextBound};
use crate::convert::CAPIConverter;
use crate::key::LookupKey;
use crate::query::QueryBuilder;
use crate::Result as TileDBResult;

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

#[derive(ContextBound)]
pub struct Subarray<'ctx> {
    #[context]
    context: &'ctx Context,
    raw: RawSubarray,
}

#[derive(ContextBound)]
pub struct Builder<'ctx, Q> {
    query: Q,
    #[base(ContextBound)]
    subarray: Subarray<'ctx>,
}

impl<'ctx, Q> Builder<'ctx, Q>
where
    Q: QueryBuilder<'ctx> + Sized,
{
    pub(crate) fn for_query(query: Q) -> TileDBResult<Self> {
        let context = query.base().context();
        let c_context = context.capi();
        let c_array = **query.base().carray();
        let mut c_subarray: *mut ffi::tiledb_subarray_t = out_ptr!();

        context.capi_return(unsafe {
            ffi::tiledb_subarray_alloc(c_context, c_array, &mut c_subarray)
        })?;

        Ok(Builder {
            query,
            subarray: Subarray {
                context,
                raw: RawSubarray::Owned(c_subarray),
            },
        })
    }

    pub fn dimension_range_typed<Conv: CAPIConverter, K: Into<LookupKey>>(
        self,
        key: K,
        range: &[Conv; 2],
    ) -> TileDBResult<Self> {
        let c_context = self.subarray.context.capi();
        let c_subarray = *self.subarray.raw;

        let c_start = &range[0] as *const Conv as *const std::ffi::c_void;
        let c_end = &range[1] as *const Conv as *const std::ffi::c_void;

        match key.into() {
            LookupKey::Index(idx) => {
                let c_idx = idx.try_into().unwrap();
                self.capi_return(unsafe {
                    ffi::tiledb_subarray_add_range(
                        c_context,
                        c_subarray,
                        c_idx,
                        c_start,
                        c_end,
                        std::ptr::null(),
                    )
                })
            }
            LookupKey::Name(name) => {
                let c_name = cstring!(name);
                self.capi_return(unsafe {
                    ffi::tiledb_subarray_add_range_by_name(
                        c_context,
                        c_subarray,
                        c_name.as_ptr(),
                        c_start,
                        c_end,
                        std::ptr::null(),
                    )
                })
            }
        }?;
        Ok(self)
    }

    /// Apply the subarray to the query, returning the query builder.
    pub fn finish_subarray(self) -> TileDBResult<Q> {
        let c_context = self.subarray.context.capi();
        let c_query = **self.query.base().cquery();
        let c_subarray = *self.subarray.raw;

        self.capi_return(unsafe {
            ffi::tiledb_query_set_subarray_t(c_context, c_query, c_subarray)
        })?;
        Ok(self.query)
    }
}
