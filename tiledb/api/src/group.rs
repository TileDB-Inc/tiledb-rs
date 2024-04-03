use std::ops::Deref;

use crate::{array, Context, ContextBound, Query};

extern crate tiledb_sys as ffi;
use crate::Result as TileDBResult;
pub type QueryType = crate::array::Mode;

pub(crate) struct RawGroup {
    ffi: *mut ffi::tiledb_group_t,
}

impl RawGroup {
    pub fn new(ffi: *mut ffi::tiledb_group_t) -> Self {
        RawGroup { ffi }
    }
}

impl Deref for RawGroup {
    type Target = *mut ffi::tiledb_group_t;
    fn deref(&self) -> &Self::Target {
        &self.ffi
    }
}

impl Drop for RawGroup {
    fn drop(&mut self) {
        unsafe { ffi::tiledb_group_free(&mut self.ffi) }
    }
}

pub struct Group<'ctx> {
    context: &'ctx Context,
    raw: RawGroup,
}

impl<'ctx> ContextBound<'ctx> for Group<'ctx> {
    fn context(&self) -> &'ctx Context {
        self.context
    }
}

impl<'ctx> Group<'ctx> {
    pub(crate) fn capi(&self) -> *mut ffi::tiledb_group_t {
        *self.raw
    }

    pub(crate) fn new(context: &'ctx Context, raw: RawGroup) -> Self {
        Group { context, raw }
    }

    pub fn create<S>(
        context: &'ctx Context,
        name: S
    ) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let c_name = cstring!(name.as_ref());
        context.capi_return(unsafe {
            ffi::tiledb_group_create(
                context.capi(),
                c_name.as_ptr()
            )
        })
    }

    pub fn open<S>(
        context: &'ctx Context,
        uri: S,
        query_type : QueryType
    ) -> TileDBResult<Self>
    where
        S: AsRef<str>,
    {
        let ctx = context.capi();
        let mut group_raw: *mut ffi::tiledb_group_t = std::ptr::null_mut();

        let c_uri = cstring!(uri.as_ref());

        context.capi_return(unsafe {
            ffi::tiledb_group_alloc(ctx, c_uri.as_ptr(), &mut group_raw)
        })?;

        let query_type_raw = query_type.capi_enum();
        context.capi_return(unsafe {
            ffi::tiledb_group_open(ctx, group_raw, query_type_raw)
        })?;
        Ok(Group {
            context,
            raw: RawGroup::new(group_raw),
        })
    }

    pub fn uri(&self,
        context: &'ctx Context) -> TileDBResult<String> {
        let c_context = self.context.capi();
        let mut c_uri = std::ptr::null::<std::ffi::c_char>();
        context.capi_return(unsafe {
            ffi::tiledb_group_get_uri(c_context, *self.raw, &mut c_uri)
        })?;
        let uri = unsafe { std::ffi::CStr::from_ptr(c_uri) };
        Ok(String::from(uri.to_string_lossy()))
    }

    pub fn query_type(&self, context:  &'ctx Context) -> TileDBResult<QueryType> {
        
    }
}

impl Drop for Group<'_> {
    fn drop(&mut self) {
        let c_context = self.context.capi();
        let c_group = *self.raw;
        self.context.capi_return(unsafe {
            ffi::tiledb_group_close(c_context, c_group)
        })
        .expect("TileDB internal error when closing group");
    }
}