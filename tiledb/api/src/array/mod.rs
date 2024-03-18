use std::convert::TryFrom;
use std::ops::Deref;

use crate::context::Context;
use crate::Result as TileDBResult;

mod attribute;
mod dimension;
mod domain;
mod schema;

pub use attribute::{Attribute, Builder as AttributeBuilder};
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

#[derive(Debug, PartialEq)]
pub enum Layout {
    Unordered,
    RowMajor,
    ColumnMajor,
    Hilbert,
}

impl Layout {
    pub(crate) fn capi_enum(&self) -> ffi::tiledb_layout_t {
        match *self {
            Layout::Unordered => ffi::tiledb_layout_t_TILEDB_UNORDERED,
            Layout::RowMajor => ffi::tiledb_layout_t_TILEDB_ROW_MAJOR,
            Layout::ColumnMajor => ffi::tiledb_layout_t_TILEDB_COL_MAJOR,
            Layout::Hilbert => ffi::tiledb_layout_t_TILEDB_HILBERT,
        }
    }
}

impl TryFrom<ffi::tiledb_layout_t> for Layout {
    type Error = crate::error::Error;
    fn try_from(value: ffi::tiledb_layout_t) -> TileDBResult<Self> {
        match value {
            ffi::tiledb_layout_t_TILEDB_UNORDERED => Ok(Layout::Unordered),
            ffi::tiledb_layout_t_TILEDB_ROW_MAJOR => Ok(Layout::RowMajor),
            ffi::tiledb_layout_t_TILEDB_COL_MAJOR => Ok(Layout::ColumnMajor),
            ffi::tiledb_layout_t_TILEDB_HILBERT => Ok(Layout::Hilbert),
            _ => Err(Self::Error::from(format!("Invalid layout: {}", value))),
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

pub struct Array<'ctx> {
    context: &'ctx Context,
    raw: RawArray,
}

impl<'ctx> Array<'ctx> {
    pub fn create(
        context: &'ctx Context,
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
        context: &'ctx Context,
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
                context,
                raw: RawArray::new(array_raw),
            })
        } else {
            Err(context.expect_last_error())
        }
    }

    pub(crate) fn capi(&self) -> *mut ffi::tiledb_array_t {
        *self.raw
    }
}

impl Drop for Array<'_> {
    fn drop(&mut self) {
        let c_context = self.context.as_mut_ptr();
        let c_array = *self.raw;
        let c_ret = unsafe { ffi::tiledb_array_close(c_context, c_array) };
        if c_ret != ffi::TILEDB_OK {
            panic!(
                "TileDB internal error when closing array: {}",
                self.context.expect_last_error()
            )
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::io;
    use tempdir::TempDir;

    use crate::array::*;
    use crate::context::Context;
    use crate::Datatype;

    /// Create the array used in the "quickstart_dense" example
    pub fn create_quickstart_dense(
        dir: &TempDir,
        context: &Context,
    ) -> TileDBResult<String> {
        let arr_dir = dir.path().join("quickstart_dense");
        let d: Domain = {
            let rows: Dimension = DimensionBuilder::new::<i32>(
                context,
                "rows",
                Datatype::Int32,
                &[1, 4],
                &4,
            )
            .expect("Error constructing rows dimension")
            .build();
            let cols: Dimension = DimensionBuilder::new::<i32>(
                context,
                "cols",
                Datatype::Int32,
                &[1, 4],
                &4,
            )
            .expect("Error constructing cols dimension")
            .build();

            DomainBuilder::new(context)
                .unwrap()
                .add_dimension(rows)
                .unwrap()
                .add_dimension(cols)
                .unwrap()
                .build()
        };

        let s: Schema = SchemaBuilder::new(context, ArrayType::Sparse, d)
            .unwrap()
            .add_attribute(
                AttributeBuilder::new(context, "a", Datatype::UInt64)
                    .unwrap()
                    .build(),
            )
            .unwrap()
            .into();

        // domain not set
        // TODO
        Array::create(context, arr_dir.to_str().unwrap(), s)?;

        Ok(String::from(arr_dir.to_str().unwrap()))
    }

    #[test]
    fn test_array_create() -> io::Result<()> {
        let tmp_dir = TempDir::new("test_rs_bdelit")?;

        let c: Context = Context::new().unwrap();

        let r = create_quickstart_dense(&tmp_dir, &c);
        assert!(r.is_ok());

        // Make sure we can remove the array we created.
        tmp_dir.close()?;

        Ok(())
    }
}
