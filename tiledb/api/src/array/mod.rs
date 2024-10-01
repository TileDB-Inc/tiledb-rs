use std::convert::TryFrom;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::num::NonZeroU32;
use std::ops::Deref;
use std::str::FromStr;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use util::option::OptionSubset;

use crate::array::enumeration::RawEnumeration;
use crate::array::schema::RawSchema;
use crate::context::{CApiInterface, Context, ContextBound};
use crate::datatype::PhysicalType;
use crate::error::{DatatypeErrorKind, Error, ModeErrorKind};
use crate::key::LookupKey;
use crate::metadata::Metadata;
use crate::range::{
    Range, SingleValueRange, TypedNonEmptyDomain, TypedRange, VarValueRange,
};
use crate::Result as TileDBResult;
use crate::{physical_type_go, Datatype};

pub mod attribute;
pub mod dimension;
pub mod domain;
pub mod enumeration;
pub mod fragment_info;
pub mod schema;

use crate::config::Config;
pub use attribute::{Attribute, AttributeData, Builder as AttributeBuilder};
pub use dimension::{
    Builder as DimensionBuilder, Dimension, DimensionConstraints, DimensionData,
};
pub use domain::{Builder as DomainBuilder, Domain, DomainData};
pub use enumeration::{
    Builder as EnumerationBuilder, Enumeration, EnumerationData,
};
use ffi::tiledb_config_t;
pub use fragment_info::{
    Builder as FragmentInfoBuilder, FragmentInfo, FragmentInfoList,
};
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
                return Err(Error::ModeType(
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

/// Method of encryption.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Encryption {
    Unencrypted,
    Aes256Gcm,
}

impl Encryption {
    /// Returns the corresponding C API constant.
    pub(crate) fn capi_enum(&self) -> ffi::tiledb_encryption_type_t {
        match *self {
            Self::Unencrypted => {
                ffi::tiledb_encryption_type_t_TILEDB_NO_ENCRYPTION
            }
            Self::Aes256Gcm => ffi::tiledb_encryption_type_t_TILEDB_AES_256_GCM,
        }
    }
}

impl Display for Encryption {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let c_encryption = self.capi_enum();
        let mut c_str = out_ptr!();

        let c_ret = unsafe {
            ffi::tiledb_encryption_type_to_str(
                c_encryption,
                &mut c_str as *mut *const ::std::os::raw::c_char,
            )
        };
        if c_ret == ffi::TILEDB_OK {
            let s =
                unsafe { std::ffi::CStr::from_ptr(c_str) }.to_string_lossy();
            write!(f, "{}", s)
        } else {
            write!(f, "<Internal error>")
        }
    }
}

impl FromStr for Encryption {
    type Err = crate::error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let c_value = cstring!(s);
        let mut c_encryption = out_ptr!();

        let c_ret = unsafe {
            ffi::tiledb_encryption_type_from_str(
                c_value.as_ptr(),
                &mut c_encryption as *mut ffi::tiledb_encryption_type_t,
            )
        };
        if c_ret == ffi::TILEDB_OK {
            Self::try_from(c_encryption)
        } else {
            Err(Error::InvalidArgument(anyhow!(format!(
                "Invalid encryption type: {}",
                s
            ))))
        }
    }
}

impl TryFrom<ffi::tiledb_encryption_type_t> for Encryption {
    type Error = crate::error::Error;

    fn try_from(value: ffi::tiledb_encryption_type_t) -> TileDBResult<Self> {
        match value {
            ffi::tiledb_encryption_type_t_TILEDB_NO_ENCRYPTION => {
                Ok(Self::Unencrypted)
            }
            ffi::tiledb_encryption_type_t_TILEDB_AES_256_GCM => {
                Ok(Self::Aes256Gcm)
            }
            _ => Err(Self::Error::LibTileDB(format!(
                "Invalid encryption type: {}",
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

pub struct Array {
    context: Context,
    uri: String,
    pub(crate) raw: RawArray,
}

impl ContextBound for Array {
    fn context(&self) -> Context {
        self.context.clone()
    }
}

fn unwrap_config_to_ptr(context: Option<&Config>) -> *mut tiledb_config_t {
    context.map_or_else(
        || std::ptr::null::<Config>() as *mut tiledb_config_t,
        |ctx| ctx.capi(),
    )
}

impl Array {
    pub(crate) fn capi(&self) -> &RawArray {
        &self.raw
    }

    pub fn create<S>(
        context: &Context,
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

    pub fn exists<S>(context: &Context, uri: S) -> TileDBResult<bool>
    where
        S: AsRef<str>,
    {
        Ok(matches!(
            context.object_type(uri)?,
            Some(crate::context::ObjectType::Array)
        ))
    }

    /// Returns the manner in which the array located at `uri` is encrypted.
    pub fn encryption<S>(context: &Context, uri: S) -> TileDBResult<Encryption>
    where
        S: AsRef<str>,
    {
        let c_uri = cstring!(uri.as_ref());
        let mut c_encryption_type: ffi::tiledb_encryption_type_t = out_ptr!();

        context.capi_call(|c_ctx| unsafe {
            ffi::tiledb_array_encryption_type(
                c_ctx,
                c_uri.as_ptr(),
                &mut c_encryption_type as *mut ffi::tiledb_encryption_type_t,
            )
        })?;

        Encryption::try_from(c_encryption_type)
    }

    /// Opens the array located at `uri` for queries of type `mode` using default configurations.
    pub fn open<S>(context: &Context, uri: S, mode: Mode) -> TileDBResult<Self>
    where
        S: AsRef<str>,
    {
        ArrayOpener::new(context, uri, mode)?.open()
    }

    /// Prepares an array to be "re-opened". Re-opening the array will bring in any changes
    /// which occured since it was initially opened. This also allows changing configurations
    /// of an open array, such as the timestamp range.
    pub fn reopen(self) -> ArrayOpener {
        ArrayOpener {
            array: self,
            mode: None,
        }
    }

    /// Returns the URI that this array is located at
    pub fn uri(&self) -> &str {
        self.uri.as_ref()
    }

    pub fn schema(&self) -> TileDBResult<Schema> {
        let c_array = *self.raw;
        let mut c_schema: *mut ffi::tiledb_array_schema_t = out_ptr!();

        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_get_schema(
                ctx,
                c_array,
                &mut c_schema as *mut *mut ffi::tiledb_array_schema_t,
            )
        })?;

        Ok(Schema::new(&self.context, RawSchema::Owned(c_schema)))
    }

    pub fn fragment_info(&self) -> TileDBResult<FragmentInfoList> {
        FragmentInfoBuilder::new(&self.context, self.uri())?.build()
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

    pub fn metadata<K: Into<LookupKey>>(
        &self,
        key: K,
    ) -> TileDBResult<Metadata> {
        let c_array = *self.raw;
        let mut vec_size: u32 = out_ptr!();
        let mut c_datatype: ffi::tiledb_datatype_t = out_ptr!();
        let mut vec_ptr: *const std::ffi::c_void = out_ptr!();

        let name: String = match key.into() {
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

    /// Get the enumeration with the given name.
    ///
    /// Note that the name is not the name of the attribute, but of the
    /// enumeration as enumerations can be shared by multiple attributes.
    pub fn get_enumeration<S: AsRef<str>>(
        &self,
        name: S,
    ) -> TileDBResult<Enumeration> {
        let c_array = *self.raw;
        let mut c_enmr: *mut ffi::tiledb_enumeration_t = out_ptr!();
        let c_name = cstring!(name.as_ref());

        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_get_enumeration(
                ctx,
                c_array,
                c_name.as_c_str().as_ptr(),
                &mut c_enmr,
            )
        })?;

        Ok(Enumeration::new(
            self.context.clone(),
            RawEnumeration::Owned(c_enmr),
        ))
    }

    /// Cleans up the array, such as consolidated fragments and array metadata.
    pub fn vacuum<S>(
        ctx: &Context,
        array_uri: S,
        config: Option<&Config>,
    ) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let c_array_uri = cstring!(array_uri.as_ref());
        ctx.capi_call(|ctx| unsafe {
            ffi::tiledb_array_vacuum(
                ctx,
                c_array_uri.as_ptr(),
                unwrap_config_to_ptr(config),
            )
        })
    }

    /// Upgrades an array to the latest format version.
    pub fn upgrade_version<S>(
        ctx: &Context,
        array_uri: S,
        config: Option<&Config>,
    ) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let c_array_uri = cstring!(array_uri.as_ref());
        ctx.capi_call(|ctx| unsafe {
            ffi::tiledb_array_upgrade_version(
                ctx,
                c_array_uri.as_ptr(),
                unwrap_config_to_ptr(config),
            )
        })
    }

    /// Depending on the consolidation mode in the config, consolidates either the fragment files,
    /// fragment metadata files, or array metadata files into a single file.
    pub fn consolidate<S>(
        ctx: &Context,
        array_uri: S,
        config: Option<&Config>,
    ) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let c_array_uri = cstring!(array_uri.as_ref());
        ctx.capi_call(|ctx| unsafe {
            ffi::tiledb_array_consolidate(
                ctx,
                c_array_uri.as_ptr(),
                unwrap_config_to_ptr(config),
            )
        })
    }

    /// Consolidates the given fragment URIs into a single fragment.
    pub fn consolidate_fragments<S>(
        ctx: &Context,
        array_uri: S,
        fragment_names: &[S],
        config: Option<&Config>,
    ) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let c_array_uri = cstring!(array_uri.as_ref());

        // This array has to outlive the API call below.
        let fragment_names_cstr = fragment_names
            .iter()
            .map(|fragment_name| Ok(cstring!(fragment_name.as_ref())))
            .collect::<TileDBResult<Vec<_>>>()?;
        let mut fragment_names_ptr = fragment_names_cstr
            .iter()
            .map(|fragment_name| fragment_name.as_ptr())
            .collect::<Vec<_>>();

        ctx.capi_call(|ctx| unsafe {
            ffi::tiledb_array_consolidate_fragments(
                ctx,
                c_array_uri.as_ptr(),
                fragment_names_ptr.as_mut_ptr(),
                fragment_names_ptr.len() as u64,
                unwrap_config_to_ptr(config),
            )
        })
    }

    /// Removes the array located at [array_uri].
    ///
    /// All of the array contents are deleted, including the values
    /// in its cells, its metadata, and its schema.
    pub fn delete<S>(context: &Context, array_uri: S) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let c_array_uri = cstring!(array_uri.as_ref());

        context.capi_call(|ctx| unsafe {
            ffi::tiledb_array_delete(ctx, c_array_uri.as_ptr())
        })
    }

    // Implements `dimension_nonempty_domain` for dimensions with CellValNum::Fixed
    fn dimension_nonempty_domain_impl_fixed<DT>(
        &self,
        dimension_key: LookupKey,
        cell_val_num: NonZeroU32,
    ) -> TileDBResult<Option<Range>>
    where
        DT: PhysicalType,
        SingleValueRange: for<'a> From<&'a [DT; 2]>,
    {
        let num_values = cell_val_num.get() as usize;
        let mut domain = vec![DT::default(); 2 * num_values].into_boxed_slice();

        let c_array = *self.raw;
        let c_domain = domain.as_mut_ptr() as *mut std::ffi::c_void;
        let mut c_is_empty = out_ptr!();

        match dimension_key {
            LookupKey::Index(idx) => {
                let c_idx: u32 = idx.try_into().map_err(
                    |e: <usize as TryInto<u32>>::Error| {
                        Error::InvalidArgument(anyhow!(e))
                    },
                )?;

                self.capi_call(|ctx| unsafe {
                    ffi::tiledb_array_get_non_empty_domain_from_index(
                        ctx,
                        c_array,
                        c_idx,
                        c_domain,
                        &mut c_is_empty,
                    )
                })
            }
            LookupKey::Name(name) => {
                let c_name = cstring!(name);
                self.capi_call(|ctx| unsafe {
                    ffi::tiledb_array_get_non_empty_domain_from_name(
                        ctx,
                        c_array,
                        c_name.as_ptr(),
                        c_domain,
                        &mut c_is_empty,
                    )
                })
            }
        }?;

        // dimension either has cell val num var or 1 right now,
        // but this is written to be easy to upgrade if that ever changes
        assert_eq!(
            num_values, 1,
            "Unexpected cell val num for dimension: {:?}",
            cell_val_num
        );

        if c_is_empty == 1 {
            Ok(None)
        } else if num_values == 1 {
            Ok(Some(Range::Single(SingleValueRange::from(&[
                domain[0], domain[1],
            ]))))
        } else {
            unreachable!()
        }
    }

    // Implements `dimension_nonempty_domain` for dimensions with CellValNum::Var
    fn dimension_nonempty_domain_impl_var<DT>(
        &self,
        dimension_key: LookupKey,
        datatype: Datatype,
    ) -> TileDBResult<Option<VarValueRange>>
    where
        DT: PhysicalType,
        VarValueRange: From<(Box<[DT]>, Box<[DT]>)>,
    {
        let c_array = *self.raw;
        let mut c_is_empty: i32 = out_ptr!();

        match dimension_key {
            LookupKey::Index(idx) => {
                let c_idx: u32 = idx.try_into().map_err(
                    |e: <usize as TryInto<u32>>::Error| {
                        Error::InvalidArgument(anyhow!(e))
                    },
                )?;

                let mut start_size: u64 = out_ptr!();
                let mut end_size: u64 = out_ptr!();
                self.capi_call(|ctx| unsafe {
                    ffi::tiledb_array_get_non_empty_domain_var_size_from_index(
                        ctx,
                        c_array,
                        c_idx,
                        &mut start_size,
                        &mut end_size,
                        &mut c_is_empty,
                    )
                })?;

                if c_is_empty == 1 {
                    return Ok(None);
                }

                if start_size % std::mem::size_of::<DT>() as u64 != 0 {
                    return Err(Error::Datatype(
                        DatatypeErrorKind::TypeMismatch {
                            user_type: std::any::type_name::<DT>().to_owned(),
                            tiledb_type: datatype,
                        },
                    ));
                }

                if end_size % std::mem::size_of::<DT>() as u64 != 0 {
                    return Err(Error::Datatype(
                        DatatypeErrorKind::TypeMismatch {
                            user_type: std::any::type_name::<DT>().to_owned(),
                            tiledb_type: datatype,
                        },
                    ));
                }

                let start_nelems =
                    start_size / std::mem::size_of::<DT>() as u64;
                let end_nelems = end_size / std::mem::size_of::<DT>() as u64;

                let mut start = vec![DT::default(); start_nelems as usize]
                    .into_boxed_slice();
                let mut end =
                    vec![DT::default(); end_nelems as usize].into_boxed_slice();

                self.capi_call(|ctx| unsafe {
                    ffi::tiledb_array_get_non_empty_domain_var_from_index(
                        ctx,
                        c_array,
                        c_idx,
                        start.as_mut_ptr() as *mut std::ffi::c_void,
                        end.as_mut_ptr() as *mut std::ffi::c_void,
                        &mut c_is_empty,
                    )
                })?;

                if c_is_empty == 1 {
                    unreachable!("Non-empty domain was non-empty for size check but empty when retrieving data: dimension = {:?}, start_size = {}, end_size = {}",
                            dimension_key, start_size, end_size)
                } else {
                    Ok(Some(VarValueRange::from((start, end))))
                }
            }
            LookupKey::Name(name) => {
                let c_name = cstring!(name);

                let mut start_size: u64 = out_ptr!();
                let mut end_size: u64 = out_ptr!();
                self.capi_call(|ctx| unsafe {
                    ffi::tiledb_array_get_non_empty_domain_var_size_from_name(
                        ctx,
                        c_array,
                        c_name.as_ptr(),
                        &mut start_size,
                        &mut end_size,
                        &mut c_is_empty,
                    )
                })?;

                if c_is_empty == 1 {
                    return Ok(None);
                }

                if start_size % std::mem::size_of::<DT>() as u64 != 0 {
                    return Err(Error::Datatype(
                        DatatypeErrorKind::TypeMismatch {
                            user_type: std::any::type_name::<DT>().to_owned(),
                            tiledb_type: datatype,
                        },
                    ));
                }

                if end_size % std::mem::size_of::<DT>() as u64 != 0 {
                    return Err(Error::Datatype(
                        DatatypeErrorKind::TypeMismatch {
                            user_type: std::any::type_name::<DT>().to_owned(),
                            tiledb_type: datatype,
                        },
                    ));
                }

                let start_nelems =
                    start_size / std::mem::size_of::<DT>() as u64;
                let end_nelems = end_size / std::mem::size_of::<DT>() as u64;

                let mut start = vec![DT::default(); start_nelems as usize]
                    .into_boxed_slice();
                let mut end =
                    vec![DT::default(); end_nelems as usize].into_boxed_slice();

                self.capi_call(|ctx| unsafe {
                    ffi::tiledb_array_get_non_empty_domain_var_from_name(
                        ctx,
                        c_array,
                        c_name.as_ptr(),
                        start.as_mut_ptr() as *mut std::ffi::c_void,
                        end.as_mut_ptr() as *mut std::ffi::c_void,
                        &mut c_is_empty,
                    )
                })?;

                if c_is_empty == 1 {
                    unreachable!("Non-empty domain was non-empty for size check but empty when retrieving data: dimension = {:?}, start_size = {}, end_size = {}",
                            c_name, start_size, end_size)
                } else {
                    Ok(Some(VarValueRange::from((start, end))))
                }
            }
        }
    }

    fn dimension_nonempty_domain_impl(
        &self,
        dimension_key: LookupKey,
        datatype: Datatype,
        cell_val_num: CellValNum,
    ) -> TileDBResult<Option<TypedRange>> {
        match cell_val_num {
            CellValNum::Fixed(nz) => {
                physical_type_go!(datatype, DT, {
                    Ok(self
                        .dimension_nonempty_domain_impl_fixed::<DT>(
                            dimension_key,
                            nz,
                        )?
                        .map(|range| TypedRange { datatype, range }))
                })
            }
            CellValNum::Var => {
                physical_type_go!(datatype, DT, {
                    let var_range = self
                        .dimension_nonempty_domain_impl_var::<DT>(
                            dimension_key,
                            datatype,
                        )?;
                    Ok(var_range.map(|var_range| TypedRange {
                        datatype,
                        range: Range::Var(var_range),
                    }))
                })
            }
        }
    }

    /// Returns the non-empty domain of a dimension from this array, if any.
    ///
    /// The domain of a dimension is the range of allowed coordinate values.
    /// It is an aspect of the array schema and is determined when the array is created.
    /// The *non-empty* domain of a dimension is the minimum and maximum
    /// coordinate values which have been populated.
    ///
    /// This is the union of the non-empty domains of all array fragments.
    pub fn dimension_nonempty_domain(
        &self,
        dimension_key: impl Into<LookupKey>,
    ) -> TileDBResult<Option<TypedRange>> {
        let key = dimension_key.into();
        let (datatype, cell_val_num) = {
            let dim = self.schema()?.domain()?.dimension(key.clone())?;
            (dim.datatype()?, dim.cell_val_num()?)
        };
        self.dimension_nonempty_domain_impl(key, datatype, cell_val_num)
    }

    /// Returns the non-empty domain of all dimensions from this array, if any.
    ///
    /// The domain of an array is the range of allowed coordinate values
    /// for each dimension. It is an aspect of the array schema and is
    /// determined when the array is created.
    /// The *non-empty* domain of an array is the minimum and maximum
    /// coordinate values of each dimension which have been populated.
    ///
    /// This is the union of all the non-empty domains of all array fragments.
    pub fn nonempty_domain(&self) -> TileDBResult<Option<TypedNonEmptyDomain>> {
        // note to devs: calling `tiledb_array_get_non_empty_domain`
        // looks like a huge pain, if this is ever a performance bottleneck
        // then maybe we can look into it
        (0..self.schema()?.domain()?.num_dimensions()?)
            .map(|d| self.dimension_nonempty_domain(d))
            .collect::<TileDBResult<Option<TypedNonEmptyDomain>>>()
    }
}

impl Drop for Array {
    fn drop(&mut self) {
        let c_array = *self.raw;
        let mut c_is_open: i32 = out_ptr!();

        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_is_open(ctx, c_array, &mut c_is_open)
        })
        .expect("TileDB internal error when closing array");

        // the array will not be open if the user constructs Opener and drops it without calling
        // `Opener::open`.  This bit of al dente buccatini is mitigated by the fact that the
        // user still never sees a non-open Array object. Maybe worth refactoring at some point
        // nonetheless.
        if c_is_open == 1 {
            self.capi_call(|ctx| unsafe {
                ffi::tiledb_array_close(ctx, c_array)
            })
            .expect("TileDB internal error when closing array");
        }
    }
}

/// Holds configuration options for opening an array located at a particular URI.
pub struct ArrayOpener {
    array: Array,
    /// Mode for opening the array, or `None` if re-opening.
    mode: Option<Mode>,
}

impl ArrayOpener {
    /// Prepares to open the array located at `uri` for operations indicated by `mode`.
    pub fn new<S>(context: &Context, uri: S, mode: Mode) -> TileDBResult<Self>
    where
        S: AsRef<str>,
    {
        let mut array_raw: *mut ffi::tiledb_array_t = out_ptr!();
        let c_uri = cstring!(uri.as_ref());

        context.capi_call(|ctx| unsafe {
            ffi::tiledb_array_alloc(ctx, c_uri.as_ptr(), &mut array_raw)
        })?;

        Ok(ArrayOpener {
            array: Array {
                context: context.clone(),
                uri: uri.as_ref().to_owned(),
                raw: RawArray::Owned(array_raw),
            },
            mode: Some(mode),
        })
    }

    /// Sets configuration options for this array.
    pub fn config(self, config: &Config) -> TileDBResult<Self> {
        let c_array = **self.array.capi();
        let c_config = config.capi();

        self.array.capi_call(|c_context| unsafe {
            ffi::tiledb_array_set_config(c_context, c_array, c_config)
        })?;
        Ok(self)
    }

    /// Configures the start timestamp for an array.
    /// The start and end timestamps determine the set of fragments
    /// which will be loaded and used for queries.
    /// Use `start_timestamp` to avoid reading data from older fragments,
    /// such as to see only recently-written data.
    pub fn start_timestamp(self, timestamp: u64) -> TileDBResult<Self> {
        let c_array = *self.array.raw;
        self.array.capi_call(|ctx| unsafe {
            ffi::tiledb_array_set_open_timestamp_start(ctx, c_array, timestamp)
        })?;
        Ok(self)
    }

    /// Configures the end timestamp for an array.
    /// The start and end timestamps determine the set of fragments
    /// which will be loaded and used for queries.
    /// Use `end_timestamp` to avoid reading data from newer fragments,
    /// such as for historical queries.
    pub fn end_timestamp(self, timestamp: u64) -> TileDBResult<Self> {
        let c_array = *self.array.raw;
        self.array.capi_call(|ctx| unsafe {
            ffi::tiledb_array_set_open_timestamp_end(ctx, c_array, timestamp)
        })?;
        Ok(self)
    }

    /// Opens the array and returns a handle to it, consuming `self`.
    pub fn open(self) -> TileDBResult<Array> {
        let c_array = *self.array.raw;

        if let Some(mode) = self.mode {
            let c_mode = mode.capi_enum();
            self.array.capi_call(|ctx| unsafe {
                ffi::tiledb_array_open(ctx, c_array, c_mode)
            })?;
        } else {
            self.array.capi_call(|ctx| unsafe {
                ffi::tiledb_array_reopen(ctx, c_array)
            })?;
        }

        Ok(self.array)
    }
}

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;

#[cfg(test)]
pub mod tests {
    use tiledb_test_utils::{self, TestArrayUri};

    use super::*;
    use crate::array::dimension::DimensionConstraints;
    use crate::config::CommonOption;
    use crate::metadata::Value;
    use crate::query::{
        Query, QueryBuilder, QueryLayout, QueryType, WriteBuilder,
    };
    use crate::{Datatype, Factory};

    /// Create the array used in the "quickstart_dense" example
    pub fn create_quickstart_dense(
        test_uri: &dyn TestArrayUri,
        context: &Context,
    ) -> TileDBResult<String> {
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

        let s: Schema = SchemaBuilder::new(context, ArrayType::Dense, d)
            .unwrap()
            .add_attribute(
                AttributeBuilder::new(context, "a", Datatype::UInt64)
                    .unwrap()
                    .build(),
            )
            .unwrap()
            .build()
            .unwrap();

        let uri = test_uri
            .with_path("quickstart_dense")
            .map_err(|e| Error::Other(e.to_string()))?;
        Array::create(context, &uri, s)?;
        Ok(uri)
    }

    /// Creates an array whose schema is used in the
    /// `quickstart_sparse_string` example and returns the URI.
    pub fn create_quickstart_sparse_string(
        test_uri: &dyn TestArrayUri,
        ctx: &Context,
    ) -> TileDBResult<String> {
        let schema =
            crate::tests::examples::quickstart::Builder::new(ArrayType::Sparse)
                .with_rows(DimensionConstraints::StringAscii)
                .build();

        let schema = schema.create(ctx)?;

        let uri = test_uri
            .with_path("quickstart_dense")
            .map_err(|e| Error::Other(e.to_string()))?;
        Array::create(ctx, &uri, schema)?;
        Ok(uri)
    }

    #[test]
    fn test_array_create() -> TileDBResult<()> {
        let test_uri = tiledb_test_utils::get_uri_generator()
            .map_err(|e| Error::Other(e.to_string()))?;

        let c: Context = Context::new().unwrap();

        let r = create_quickstart_dense(&test_uri, &c);
        assert!(r.is_ok());

        // Make sure we can remove the array we created.
        test_uri.close().map_err(|e| Error::Other(e.to_string()))?;

        Ok(())
    }

    #[test]
    fn test_array_metadata() -> TileDBResult<()> {
        let test_uri = tiledb_test_utils::get_uri_generator()
            .map_err(|e| Error::Other(e.to_string()))?;

        let tdb = Context::new()?;
        let r = create_quickstart_dense(&test_uri, &tdb);
        assert!(r.is_ok());
        let uri = r.ok().unwrap();

        {
            let mut array = Array::open(&tdb, &uri, QueryType::Write)?;

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
            let array = Array::open(&tdb, &uri, QueryType::Read)?;

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
            let mut array = Array::open(&tdb, &uri, QueryType::Write)?;
            array.delete_metadata("aaa")?;
        }

        {
            let array = Array::open(&tdb, &uri, QueryType::Read)?;
            let has_aaa = array.has_metadata_key("aaa")?;
            assert_eq!(has_aaa, None);
        }

        test_uri.close().map_err(|e| Error::Other(e.to_string()))
    }

    #[test]
    fn test_mode_metadata() -> TileDBResult<()> {
        let test_uri = tiledb_test_utils::get_uri_generator()
            .map_err(|e| Error::Other(e.to_string()))?;

        let tdb = Context::new()?;
        let r = create_quickstart_dense(&test_uri, &tdb);
        assert!(r.is_ok());
        let uri = r.ok().unwrap();

        // Calling put_metadada with the wrong mode.
        {
            let mut array = Array::open(&tdb, &uri, QueryType::Read)?;
            let res = array.put_metadata(Metadata::new(
                "key".to_owned(),
                Datatype::Int32,
                vec![5],
            )?);
            assert!(res.is_err());
        }

        // Successful put_metadata call.
        {
            let mut array = Array::open(&tdb, &uri, QueryType::Write)?;
            let res = array.put_metadata(Metadata::new(
                "key".to_owned(),
                Datatype::Int32,
                vec![5],
            )?);
            assert!(res.is_ok());
        }

        // Read metadata mode testing.
        {
            let array = Array::open(&tdb, &uri, QueryType::Write)?;

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
            let mut array = Array::open(&tdb, &uri, QueryType::Read)?;
            let res = array.delete_metadata("key");
            assert!(res.is_err());
        }

        test_uri.close().map_err(|e| Error::Other(e.to_string()))
    }

    fn create_simple_dense(
        test_uri: &dyn TestArrayUri,
        ctx: &Context,
    ) -> TileDBResult<String> {
        let domain = {
            let rows = DimensionBuilder::new(
                ctx,
                "id",
                Datatype::Int32,
                ([1, 410], 10),
            )?
            .build();

            DomainBuilder::new(ctx)?.add_dimension(rows)?.build()
        };

        let schema = SchemaBuilder::new(ctx, ArrayType::Dense, domain)?
            .add_attribute(
                AttributeBuilder::new(ctx, "a", Datatype::Int32)?.build(),
            )?
            .build()?;

        let array_uri = test_uri
            .with_path("quickstart_dense")
            .map_err(|e| Error::Other(e.to_string()))?;
        Array::create(ctx, &array_uri, schema)?;
        Ok(array_uri)
    }

    fn write_dense_vector_4_fragments(
        ctx: &Context,
        array_uri: &str,
        timestamp: u64,
    ) -> TileDBResult<()> {
        // Subarray boundaries.
        let boundaries = [0, 200, 250, 310, 410];

        for i in 0..4 {
            // Prepare cell buffer.
            let low_bound = boundaries[i];
            let high_bound = boundaries[i + 1];

            let data = (low_bound..high_bound).collect::<Vec<i32>>();

            let opener = ArrayOpener::new(ctx, array_uri, Mode::Write)?
                .end_timestamp(timestamp + i as u64 + 1)?;
            let array = opener.open()?;

            let q1 = WriteBuilder::new(array)?
                .layout(QueryLayout::RowMajor)?
                .start_subarray()?
                .add_range(0, &[low_bound + 1, boundaries[i + 1]])?
                .finish_subarray()?
                .data_typed("a", &data)?
                .build();
            q1.submit().and_then(|_| q1.finalize())?;
        }

        Ok(())
    }

    #[test]
    fn test_array_consolidation() -> TileDBResult<()> {
        // Test advanced consolidation. Based on unit-capi-consolidation.cc.

        let ctx: Context = Context::new().unwrap();
        let array_uri = tiledb_test_utils::get_uri_generator().unwrap();
        let array_uri = create_simple_dense(&array_uri, &ctx)?;
        write_dense_vector_4_fragments(&ctx, &array_uri, 0).unwrap();

        let mut config = Config::new()?;
        config.set("sm.consolidation.steps", "1").unwrap();
        config.set("sm.consolidation.step_min_frags", "2").unwrap();
        config.set("sm.consolidation.step_max_frags", "2").unwrap();
        config
            .set("sm.consolidation.step_size_ratio", "0.0")
            .unwrap();
        config.set("sm.consolidation.buffer_size", "10000").unwrap();

        let get_fragments_fn =
            || FragmentInfoBuilder::new(&ctx, array_uri.clone())?.build();
        let count_fragments_fn = || get_fragments_fn()?.num_fragments();
        assert_eq!(4, count_fragments_fn()?);

        // Consolidate and Vacuum.
        Array::consolidate(&ctx, &array_uri, Some(&config)).unwrap();
        Array::vacuum(&ctx, &array_uri, Some(&config)).unwrap();
        // We have consolidated first two fragments.
        assert_eq!(3, count_fragments_fn()?);

        let fragment_names = get_fragments_fn()?
            .iter()?
            .map(|f| f.name())
            .collect::<TileDBResult<Vec<_>>>()?;
        // Consolidate second and third remaining fragments.
        Array::consolidate_fragments(
            &ctx,
            array_uri.clone(),
            &fragment_names[1..3],
            Some(&config),
        )?;
        Array::vacuum(&ctx, &array_uri, Some(&config)).unwrap();
        assert_eq!(2, count_fragments_fn().unwrap());

        Ok(())
    }

    #[test]
    fn delete() -> TileDBResult<()> {
        let test_uri = tiledb_test_utils::get_uri_generator()
            .map_err(|e| Error::Other(e.to_string()))?;

        let c: Context = Context::new().unwrap();

        let r = create_quickstart_dense(&test_uri, &c);
        assert!(r.is_ok());
        let uri = r.unwrap();

        assert!(matches!(Array::exists(&c, &uri), Ok(true)));

        let r = Array::delete(&c, &uri);
        assert!(r.is_ok());

        assert!(matches!(Array::exists(&c, &uri), Ok(false)));

        Ok(())
    }

    #[test]
    fn create_enumeration() -> TileDBResult<()> {
        let test_uri = tiledb_test_utils::get_uri_generator()
            .map_err(|e| Error::Other(e.to_string()))?;

        let uri = test_uri
            .with_path("enumeration_test")
            .map_err(|e| Error::Other(e.to_string()))?;

        let ctx = Context::new().unwrap();
        let domain = {
            let dim = DimensionBuilder::new(
                &ctx,
                "dim",
                Datatype::Int32,
                ([0, 16], 4),
            )?
            .build();

            DomainBuilder::new(&ctx)?.add_dimension(dim)?.build()
        };

        let enmr = EnumerationBuilder::new(
            &ctx,
            "flintstones",
            Datatype::StringUtf8,
            "fredwilmageorgebetty".as_bytes(),
            Some(&[0u64, 4, 8, 14]),
        )
        .var_sized()
        .build()?;

        let enmr_before_write = EnumerationData::try_from(&enmr)?;

        let attr1 = AttributeBuilder::new(&ctx, "attr1", Datatype::Int32)?
            .nullability(true)?
            .enumeration_name("flintstones")?
            .build();

        let attr2 =
            AttributeBuilder::new(&ctx, "attr2", Datatype::Int32)?.build();

        let schema = SchemaBuilder::new(&ctx, ArrayType::Sparse, domain)?
            .add_enumeration(enmr)?
            .add_attribute(attr1)?
            .add_attribute(attr2)?
            .build()?;

        Array::create(&ctx, &uri, schema)?;

        let array = Array::open(&ctx, &uri, Mode::Read)?;

        let enmr = array.get_enumeration("flintstones")?;
        let enmr_after_write = EnumerationData::try_from(&enmr)?;

        assert!(enmr_before_write.option_subset(&enmr_after_write));

        assert_eq!(
            enmr_after_write.data,
            "fredwilmageorgebetty"
                .as_bytes()
                .to_vec()
                .into_boxed_slice()
        );

        assert_eq!(
            enmr_after_write.offsets,
            Some(vec![0u64, 4, 8, 14].into_boxed_slice())
        );

        assert!(
            array
                .schema()?
                .attribute("attr1")?
                .enumeration_name()?
                .unwrap()
                == "flintstones"
        );

        assert!(array
            .schema()?
            .attribute("attr2")?
            .enumeration_name()?
            .is_none());

        Ok(())
    }

    #[test]
    fn encryption_type_str() {
        assert_eq!(
            Encryption::Unencrypted,
            Encryption::from_str(&Encryption::Unencrypted.to_string()).unwrap()
        );
        assert_eq!(
            Encryption::Aes256Gcm,
            Encryption::from_str(&Encryption::Aes256Gcm.to_string()).unwrap()
        );
    }

    #[test]
    fn encryption_type_capi() {
        assert_eq!(
            Encryption::Unencrypted,
            Encryption::try_from(Encryption::Unencrypted.capi_enum()).unwrap()
        );
        assert_eq!(
            Encryption::Aes256Gcm,
            Encryption::try_from(Encryption::Aes256Gcm.capi_enum()).unwrap()
        );
    }

    #[test]
    fn encrypted_array() -> TileDBResult<()> {
        let test_uri = tiledb_test_utils::get_uri_generator()
            .map_err(|e| Error::Other(e.to_string()))?;

        let key = "0123456789abcdeF0123456789abcdeF";
        let key_config =
            CommonOption::Aes256GcmEncryptionKey(key.as_bytes().to_vec());

        // create array and try opening array using the same configured context
        let uri = {
            let context = {
                let mut config = Config::new()?;
                config.set_common_option(&key_config)?;

                Context::from_config(&config)
            }?;

            let uri = create_quickstart_dense(&test_uri, &context)?;

            assert_eq!(
                Encryption::Aes256Gcm,
                Array::encryption(&context, &uri)?
            );

            // re-using the configured context should be fine
            let _ = ArrayOpener::new(&context, &uri, Mode::Read)?.open()?;
            let _ = ArrayOpener::new(&context, &uri, Mode::Write)?.open()?;

            uri
        };

        // try opening from an un-configured context and it should fail
        {
            let context = Context::new()?;

            let open_read = Array::open(&context, &uri, Mode::Read);
            assert!(matches!(open_read, Err(Error::LibTileDB(_))));

            let open_write = Array::open(&context, &uri, Mode::Read);
            assert!(matches!(open_write, Err(Error::LibTileDB(_))));
        }

        // try opening from an un-configured context with the right array config should succeed
        {
            let context = Context::new()?;
            let array_config =
                Config::new()?.with_common_option(&key_config)?;

            let _ = ArrayOpener::new(&context, &uri, Mode::Read)?
                .config(&array_config)?
                .open()?;
            let _ = ArrayOpener::new(&context, &uri, Mode::Write)?
                .config(&array_config)?
                .open()?;
        }

        Ok(())
    }
}
