use std::convert::TryFrom;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::ops::Deref;

use serde::{Deserialize, Serialize};
use util::option::OptionSubset;

use crate::array::schema::RawSchema;
use crate::context::{CApiInterface, Context, ContextBound};
use crate::error::ModeErrorKind;
use crate::key::LookupKey;
use crate::metadata::Metadata;
use crate::Datatype;
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

    pub fn put_metadata(&mut self, metadata: Metadata) -> TileDBResult<()> {
        let c_array = *self.raw;
        let (vec_size, vec_ptr, datatype) = metadata.c_data();
        let c_key = cstring!(metadata.key);
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_put_metadata(
                ctx,
                c_array,
                c_key.as_ptr(),
                datatype,
                vec_size as u32,
                vec_ptr,
            )
        })?;
        Ok(())
    }

    pub fn delete_metadata<S>(&mut self, name: S) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let c_array = *self.raw;
        let c_name = cstring!(name.as_ref());
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_delete_metadata(ctx, c_array, c_name.as_ptr())
        })?;
        Ok(())
    }

    pub fn num_metadata(&self) -> TileDBResult<u64> {
        let c_array = *self.raw;
        let mut num: u64 = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_get_metadata_num(ctx, c_array, &mut num)
        })?;
        Ok(num)
    }

    pub fn metadata(&self, key: LookupKey) -> TileDBResult<Metadata> {
        let c_array = *self.raw;
        let mut vec_size: u32 = out_ptr!();
        let mut c_datatype: ffi::tiledb_datatype_t = out_ptr!();
        let mut vec_ptr: *const std::ffi::c_void = out_ptr!();

        let name: String = match key {
            LookupKey::Index(index) => {
                let mut key_ptr: *const std::ffi::c_char = out_ptr!();
                let mut key_len: u32 = out_ptr!();
                self.capi_call(|ctx| unsafe {
                    ffi::tiledb_array_get_metadata_from_index(
                        ctx,
                        c_array,
                        index as u64,
                        &mut key_ptr,
                        &mut key_len,
                        &mut c_datatype,
                        &mut vec_size,
                        &mut vec_ptr,
                    )
                })?;
                let c_key = unsafe { std::ffi::CStr::from_ptr(key_ptr) };
                Ok(c_key.to_string_lossy().into_owned()) as TileDBResult<String>
            }
            LookupKey::Name(name) => {
                let c_name = cstring!(name.as_ref() as &str);
                self.capi_call(|ctx| unsafe {
                    ffi::tiledb_array_get_metadata(
                        ctx,
                        c_array,
                        c_name.as_ptr(),
                        &mut c_datatype,
                        &mut vec_size,
                        &mut vec_ptr,
                    )
                })?;
                Ok(name.to_owned())
            }
        }?;
        let datatype = Datatype::try_from(c_datatype)?;
        Ok(Metadata::new_raw(name, datatype, vec_ptr, vec_size))
    }

    pub fn has_metadata_key<S>(&self, name: S) -> TileDBResult<Option<Datatype>>
    where
        S: AsRef<str>,
    {
        let c_array = *self.raw;
        let c_name = cstring!(name.as_ref());
        let mut c_datatype: ffi::tiledb_datatype_t = out_ptr!();
        let mut exists: i32 = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_has_metadata_key(
                ctx,
                c_array,
                c_name.as_ptr(),
                &mut c_datatype,
                &mut exists,
            )
        })?;
        if exists == 0 {
            return Ok(None);
        }

        let datatype = Datatype::try_from(c_datatype)?;
        Ok(Some(datatype))
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
    use crate::error::Error;
    use std::io;
    use tempfile::TempDir;

    use crate::array::*;
    use crate::metadata::Value;
    use crate::query::QueryType;
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

    #[test]
    fn test_array_metadata() -> TileDBResult<()> {
        let tmp_dir =
            TempDir::new().map_err(|e| Error::Other(e.to_string()))?;

        let tdb = Context::new()?;
        let r = create_quickstart_dense(&tmp_dir, &tdb);
        assert!(r.is_ok());

        let arr_dir = tmp_dir.path().join("quickstart_dense");
        {
            let mut array =
                Array::open(&tdb, arr_dir.to_str().unwrap(), QueryType::Write)?;

            array.put_metadata(Metadata::new(
                "key".to_owned(),
                Datatype::Int32,
                vec![5],
            )?)?;
            array.put_metadata(Metadata::new(
                "aaa".to_owned(),
                Datatype::Int32,
                vec![5],
            )?)?;
            array.put_metadata(Metadata::new(
                "bb".to_owned(),
                Datatype::Float32,
                vec![1.1f32, 2.2f32],
            )?)?;
        }

        {
            let array =
                Array::open(&tdb, arr_dir.to_str().unwrap(), QueryType::Read)?;

            let metadata_aaa =
                array.metadata(LookupKey::Name("aaa".to_owned()))?;
            assert_eq!(metadata_aaa.datatype, Datatype::Int32);
            assert_eq!(metadata_aaa.value, Value::Int32Value(vec!(5)));
            assert_eq!(metadata_aaa.key, "aaa");

            let metadata_num = array.num_metadata()?;
            assert_eq!(metadata_num, 3);

            let metadata_bb = array.metadata(LookupKey::Index(1))?;
            assert_eq!(metadata_bb.datatype, Datatype::Float32);
            assert_eq!(metadata_bb.key, "bb");
            assert_eq!(
                metadata_bb.value,
                Value::Float32Value(vec!(1.1f32, 2.2f32))
            );

            let has_aaa = array.has_metadata_key("aaa")?;
            assert_eq!(has_aaa, Some(Datatype::Int32));
        }

        {
            let mut array =
                Array::open(&tdb, arr_dir.to_str().unwrap(), QueryType::Write)?;
            array.delete_metadata("aaa")?;
        }

        {
            let array =
                Array::open(&tdb, arr_dir.to_str().unwrap(), QueryType::Read)?;
            let has_aaa = array.has_metadata_key("aaa")?;
            assert_eq!(has_aaa, None);
        }

        tmp_dir.close().map_err(|e| Error::Other(e.to_string()))?;
        Ok(())
    }

    #[test]
    fn test_mode_metadata() -> TileDBResult<()> {
        let tmp_dir =
            TempDir::new().map_err(|e| Error::Other(e.to_string()))?;

        let tdb = Context::new()?;
        let r = create_quickstart_dense(&tmp_dir, &tdb);
        assert!(r.is_ok());

        let arr_dir = tmp_dir.path().join("quickstart_dense");
        // Calling put_metadada with the wrong mode.
        {
            let mut array =
                Array::open(&tdb, arr_dir.to_str().unwrap(), QueryType::Read)?;
            let res = array.put_metadata(Metadata::new(
                "key".to_owned(),
                Datatype::Int32,
                vec![5],
            )?);
            assert!(res.is_err());
        }

        // Successful put_metadata call.
        {
            let mut array =
                Array::open(&tdb, arr_dir.to_str().unwrap(), QueryType::Write)?;
            let res = array.put_metadata(Metadata::new(
                "key".to_owned(),
                Datatype::Int32,
                vec![5],
            )?);
            assert!(res.is_ok());
        }

        // Read metadata mode testing.
        {
            let array =
                Array::open(&tdb, arr_dir.to_str().unwrap(), QueryType::Write)?;

            let res = array.metadata(LookupKey::Name("aaa".to_owned()));
            assert!(res.is_err());

            let res = array.num_metadata();
            assert!(res.is_err());

            let res = array.metadata(LookupKey::Index(0));
            assert!(res.is_err());

            let res = array.has_metadata_key("key");
            assert!(res.is_err());
        }

        {
            let mut array =
                Array::open(&tdb, arr_dir.to_str().unwrap(), QueryType::Read)?;
            let res = array.delete_metadata("key");
            assert!(res.is_err());
        }

        tmp_dir.close().map_err(|e| Error::Other(e.to_string()))?;
        Ok(())
    }
}
