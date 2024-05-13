use std::convert::TryFrom;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::ops::Deref;

use serde::{Deserialize, Serialize};
use util::option::OptionSubset;

use crate::array::schema::RawSchema;
use crate::context::{CApiInterface, Context, ContextBound};
use crate::error::ModeErrorKind;
use crate::Result as TileDBResult;

pub mod attribute;
pub mod dimension;
pub mod domain;
pub mod enumeration;
pub mod fragment_info;
pub mod schema;

pub use attribute::{Attribute, AttributeData, Builder as AttributeBuilder};
pub use dimension::{Builder as DimensionBuilder, Dimension, DimensionData};
pub use domain::{Builder as DomainBuilder, Domain, DomainData};
pub use enumeration::{
    Builder as EnumerationBuilder, Enumeration, EnumerationData,
};
pub use fragment_info::{Builder as FragmentInfoBuilder, FragmentInfo};
pub use schema::{
    ArrayType, Builder as SchemaBuilder, CellValNum, Field, Schema, SchemaData,
};

#[derive(Clone, Debug, PartialEq)]
pub enum Mode {
    Read,
    Write,
    Delete,
    Update,
    ModifyExclusive,
}

impl Mode {
    pub(crate) fn capi_enum(&self) -> ffi::tiledb_query_type_t {
        match *self {
            Mode::Read => ffi::tiledb_query_type_t_TILEDB_READ,
            Mode::Write => ffi::tiledb_query_type_t_TILEDB_WRITE,
            Mode::Delete => ffi::tiledb_query_type_t_TILEDB_DELETE,
            Mode::Update => ffi::tiledb_query_type_t_TILEDB_UPDATE,
            Mode::ModifyExclusive => {
                ffi::tiledb_query_type_t_TILEDB_MODIFY_EXCLUSIVE
            }
        }
    }
}

impl TryFrom<ffi::tiledb_query_type_t> for Mode {
    type Error = crate::error::Error;

    fn try_from(value: ffi::tiledb_query_type_t) -> TileDBResult<Self> {
        Ok(match value {
            ffi::tiledb_query_type_t_TILEDB_READ => Mode::Read,
            ffi::tiledb_query_type_t_TILEDB_WRITE => Mode::Write,
            ffi::tiledb_query_type_t_TILEDB_DELETE => Mode::Delete,
            ffi::tiledb_query_type_t_TILEDB_UPDATE => Mode::Update,
            ffi::tiledb_query_type_t_TILEDB_MODIFY_EXCLUSIVE => {
                Mode::ModifyExclusive
            }
            _ => {
                return Err(crate::error::Error::ModeType(
                    ModeErrorKind::InvalidDiscriminant(value as u64),
                ))
            }
        })
    }
}

impl Display for Mode {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        <Self as Debug>::fmt(self, f)
    }
}

#[derive(
    Clone, Copy, Debug, Deserialize, Eq, OptionSubset, PartialEq, Serialize,
)]
#[cfg_attr(
    any(test, feature = "proptest-strategies"),
    derive(proptest_derive::Arbitrary)
)]
pub enum TileOrder {
    RowMajor,
    ColumnMajor,
}

impl TileOrder {
    pub(crate) fn capi_enum(&self) -> ffi::tiledb_layout_t {
        match *self {
            TileOrder::RowMajor => ffi::tiledb_layout_t_TILEDB_ROW_MAJOR,
            TileOrder::ColumnMajor => ffi::tiledb_layout_t_TILEDB_COL_MAJOR,
        }
    }
}

impl TryFrom<ffi::tiledb_layout_t> for TileOrder {
    type Error = crate::error::Error;
    fn try_from(value: ffi::tiledb_layout_t) -> TileDBResult<Self> {
        match value {
            ffi::tiledb_layout_t_TILEDB_ROW_MAJOR => Ok(TileOrder::RowMajor),
            ffi::tiledb_layout_t_TILEDB_COL_MAJOR => Ok(TileOrder::ColumnMajor),
            _ => Err(Self::Error::LibTileDB(format!(
                "Invalid tile order: {}",
                value
            ))),
        }
    }
}

#[derive(
    Clone, Copy, Debug, Deserialize, Eq, OptionSubset, PartialEq, Serialize,
)]
pub enum CellOrder {
    Unordered,
    RowMajor,
    ColumnMajor,
    Global,
    Hilbert,
}

impl CellOrder {
    pub(crate) fn capi_enum(&self) -> ffi::tiledb_layout_t {
        match *self {
            CellOrder::Unordered => ffi::tiledb_layout_t_TILEDB_UNORDERED,
            CellOrder::RowMajor => ffi::tiledb_layout_t_TILEDB_ROW_MAJOR,
            CellOrder::ColumnMajor => ffi::tiledb_layout_t_TILEDB_COL_MAJOR,
            CellOrder::Global => ffi::tiledb_layout_t_TILEDB_GLOBAL_ORDER,
            CellOrder::Hilbert => ffi::tiledb_layout_t_TILEDB_HILBERT,
        }
    }
}

impl TryFrom<ffi::tiledb_layout_t> for CellOrder {
    type Error = crate::error::Error;
    fn try_from(value: ffi::tiledb_layout_t) -> TileDBResult<Self> {
        match value {
            ffi::tiledb_layout_t_TILEDB_UNORDERED => Ok(CellOrder::Unordered),
            ffi::tiledb_layout_t_TILEDB_ROW_MAJOR => Ok(CellOrder::RowMajor),
            ffi::tiledb_layout_t_TILEDB_COL_MAJOR => Ok(CellOrder::ColumnMajor),
            ffi::tiledb_layout_t_TILEDB_GLOBAL_ORDER => Ok(CellOrder::Global),
            ffi::tiledb_layout_t_TILEDB_HILBERT => Ok(CellOrder::Hilbert),
            _ => Err(Self::Error::LibTileDB(format!(
                "Invalid cell order: {}",
                value
            ))),
        }
    }
}

pub enum RawArray {
    Owned(*mut ffi::tiledb_array_t),
}

impl Deref for RawArray {
    type Target = *mut ffi::tiledb_array_t;
    fn deref(&self) -> &Self::Target {
        let RawArray::Owned(ffi) = self;
        ffi
    }
}

impl Drop for RawArray {
    fn drop(&mut self) {
        let RawArray::Owned(ref mut ffi) = *self;
        unsafe { ffi::tiledb_array_free(ffi) }
    }
}

#[derive(ContextBound)]
pub struct Array<'ctx> {
    #[context]
    context: &'ctx Context,
    pub(crate) raw: RawArray,
}

impl<'ctx> Array<'ctx> {
    pub(crate) fn capi(&self) -> &RawArray {
        &self.raw
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
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_array_create(ctx, c_name.as_ptr(), schema.capi())
        })?;

        Ok(())
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
        let mut array_raw: *mut ffi::tiledb_array_t = std::ptr::null_mut();
        let c_uri = cstring!(uri.as_ref());

        context.capi_call(|ctx| unsafe {
            ffi::tiledb_array_alloc(ctx, c_uri.as_ptr(), &mut array_raw)
        })?;

        let mode_raw = mode.capi_enum();
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_array_open(ctx, array_raw, mode_raw)
        })?;

        Ok(Array {
            context,
            raw: RawArray::Owned(array_raw),
        })
    }

    pub fn schema(&self) -> TileDBResult<Schema<'ctx>> {
        let c_array = *self.raw;
        let mut c_schema: *mut ffi::tiledb_array_schema_t = out_ptr!();

        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_get_schema(
                ctx,
                c_array,
                &mut c_schema as *mut *mut ffi::tiledb_array_schema_t,
            )
        })?;

        Ok(Schema::new(self.context, RawSchema::Owned(c_schema)))
    }
}

impl Drop for Array<'_> {
    fn drop(&mut self) {
        let c_array = *self.raw;
        self.capi_call(|ctx| unsafe { ffi::tiledb_array_close(ctx, c_array) })
            .expect("TileDB internal error when closing array");
    }
}

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;

#[cfg(test)]
pub mod tests {
    use std::io;
    use tempfile::TempDir;

    use crate::array::*;
    use crate::Datatype;

    /// Create the array used in the "quickstart_dense" example
    pub fn create_quickstart_dense(
        dir: &TempDir,
        context: &Context,
    ) -> TileDBResult<String> {
        let arr_dir = dir.path().join("quickstart_dense");
        let d: Domain = {
            let rows: Dimension = DimensionBuilder::new(
                context,
                "rows",
                Datatype::Int32,
                ([1, 4], 4),
            )
            .expect("Error constructing rows dimension")
            .build();
            let cols: Dimension = DimensionBuilder::new(
                context,
                "cols",
                Datatype::Int32,
                ([1, 4], 4),
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
            .build()
            .unwrap();

        // domain not set
        // TODO
        Array::create(context, arr_dir.to_str().unwrap(), s)?;

        Ok(String::from(arr_dir.to_str().unwrap()))
    }

    #[test]
    fn test_array_create() -> io::Result<()> {
        let tmp_dir = TempDir::new()?;

        let c: Context = Context::new().unwrap();

        let r = create_quickstart_dense(&tmp_dir, &c);
        assert!(r.is_ok());

        // Make sure we can remove the array we created.
        tmp_dir.close()?;

        Ok(())
    }
}
