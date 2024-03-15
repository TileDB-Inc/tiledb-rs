use std::ops::Deref;
use std::sync::Arc;

use crate::context::Context;
use crate::Result as TileDBResult;

mod attribute;
mod dimension;
mod domain;
mod schema;

pub use attribute::Attribute;
pub use dimension::{Builder as DimensionBuilder, Dimension};
pub use domain::{Builder as DomainBuilder, Domain};
pub use schema::{ArrayType, Builder as SchemaBuilder, Schema};

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

pub(crate) struct RawArray {
    ffi: *mut ffi::tiledb_array_t,
}

impl RawArray {
    pub fn new(ffi: *mut ffi::tiledb_array_t) -> Self {
        RawArray { ffi }
    }
}

impl Deref for RawArray {
    type Target = *mut ffi::tiledb_array_t;
    fn deref(&self) -> &Self::Target {
        &self.ffi
    }
}

impl Drop for RawArray {
    fn drop(&mut self) {
        unsafe { ffi::tiledb_array_free(&mut self.ffi) }
    }
}

pub struct Array {
    raw: RawArray,
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
                c_name.as_ptr(),
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
                raw: RawArray::new(array_raw),
            })
        } else {
            Err(context.expect_last_error())
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate lazy_static;
    extern crate tempdir;

    use crate::array::*;
    use crate::context::Context;
    use crate::Datatype;
    use lazy_static::lazy_static;

    lazy_static! {
        static ref DIR: tempdir::TempDir =
            tempdir::TempDir::new("tiledb-rs.array").unwrap();
    }

    #[test]
    fn test_array_create() {
        let arr_path = DIR.path().join("test_array_create");

        let c: Context = Context::new().unwrap();

        let s: Schema = SchemaBuilder::new(&c, ArrayType::Sparse)
            .unwrap()
            .add_attribute(Attribute::new(&c, "a", Datatype::UInt64).unwrap())
            .unwrap()
            .into();

        // domain not set
        assert!(Array::create(&c, arr_path.to_str().unwrap(), s).is_err());
    }
}
