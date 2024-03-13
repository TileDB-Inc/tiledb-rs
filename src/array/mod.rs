use std::sync::Arc;

use crate::context::Context;
use crate::Result as TileDBResult;

mod attribute;
mod schema;

pub use attribute::Attribute;
pub use schema::{Builder as SchemaBuilder, Schema};

pub enum Mode {
    Read,
    Write,
    Delete,
    Update,
}

impl Mode {
    pub(crate) fn capi_enum(&self) -> ffi::tiledb_query_type_t {
        match *self {
            Mode::Read => ffi::tiledb_query_type_t_TILEDB_READ,
            Mode::Write => ffi::tiledb_query_type_t_TILEDB_WRITE,
            Mode::Delete => ffi::tiledb_query_type_t_TILEDB_DELETE,
            Mode::Update => ffi::tiledb_query_type_t_TILEDB_UPDATE,
        }
    }
}

pub struct Array {
    _wrapped: *mut ffi::tiledb_array_t,
}

impl Array {
    pub fn create(
        context: &Context,
        name: &str,
        schema: Schema,
    ) -> TileDBResult<()> {
        let c_name = cstring!(name);
        if unsafe {
            ffi::tiledb_array_create(
                context.as_mut_ptr(),
                c_name,
                schema.as_mut_ptr(),
            )
        } == ffi::TILEDB_OK
        {
            Ok(())
        } else {
            Err(context.expect_last_error())
        }
    }

    pub fn open(
        context: Arc<Context>,
        uri: &str,
        mode: Mode,
    ) -> TileDBResult<Self> {
        let ctx = context.as_mut_ptr();
        let mut array_raw: *mut ffi::tiledb_array_t = std::ptr::null_mut();

        let c_uri = cstring!(uri);

        if unsafe {
            ffi::tiledb_array_alloc(ctx, c_uri.as_ptr(), &mut array_raw)
        } != ffi::TILEDB_OK
        {
            return Err(context.expect_last_error());
        }

        let mode_raw = mode.capi_enum();
        if unsafe { ffi::tiledb_array_open(ctx, array_raw, mode_raw) }
            == ffi::TILEDB_OK
        {
            Ok(Array {
                _wrapped: array_raw,
            })
        } else {
            Err(context.expect_last_error())
        }
    }
}
