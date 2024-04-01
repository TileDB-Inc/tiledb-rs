use std::convert::TryFrom;
use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::context::{CApiInterface, Context, ContextBound};
use crate::Result as TileDBResult;

pub mod attribute;
pub mod dimension;
pub mod domain;
pub mod schema;

pub use attribute::{Attribute, AttributeData, Builder as AttributeBuilder};
pub use dimension::{Builder as DimensionBuilder, Dimension, DimensionData};
pub use domain::{Builder as DomainBuilder, DimensionKey, Domain, DomainData};
pub use schema::{ArrayType, Builder as SchemaBuilder, Schema, SchemaData};

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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
            _ => Err(Self::Error::LibTileDB(format!(
                "Invalid layout: {}",
                value
            ))),
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

impl<'ctx> ContextBound<'ctx> for Array<'ctx> {
    fn context(&self) -> &'ctx Context {
        self.context
    }
}

impl<'ctx> Array<'ctx> {
    pub(crate) fn capi(&self) -> *mut ffi::tiledb_array_t {
        *self.raw
    }

    pub fn create<S>(
        context: &'ctx Context,
        name: S,
        schema: Schema,
    ) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let c_name = cstring!(name.as_ref());
        context.capi_return(unsafe {
            ffi::tiledb_array_create(
                context.capi(),
                c_name.as_ptr(),
                schema.capi(),
            )
        })
    }

    pub fn exists<S>(context: &'ctx Context, uri: S) -> TileDBResult<bool>
    where
        S: AsRef<str>,
    {
        Ok(matches!(
            context.object_type(uri)?,
            Some(crate::context::ObjectType::Array)
        ))
    }

    pub fn open<S>(
        context: &'ctx Context,
        uri: S,
        mode: Mode,
    ) -> TileDBResult<Self>
    where
        S: AsRef<str>,
    {
        let ctx = context.capi();
        let mut array_raw: *mut ffi::tiledb_array_t = std::ptr::null_mut();

        let c_uri = cstring!(uri.as_ref());

        context.capi_return(unsafe {
            ffi::tiledb_array_alloc(ctx, c_uri.as_ptr(), &mut array_raw)
        })?;

        let mode_raw = mode.capi_enum();
        context.capi_return(unsafe {
            ffi::tiledb_array_open(ctx, array_raw, mode_raw)
        })?;
        Ok(Array {
            context,
            raw: RawArray::new(array_raw),
        })
    }
}

impl Drop for Array<'_> {
    fn drop(&mut self) {
        let c_context = self.context.capi();
        let c_array = *self.raw;
        self.capi_return(unsafe {
            ffi::tiledb_array_close(c_context, c_array)
        })
        .expect("TileDB internal error when closing array");
    }
}

#[cfg(feature = "proptest-strategies")]
pub mod strategy;

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
