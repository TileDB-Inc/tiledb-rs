use std::ops::Deref;

use crate::config::{Config, RawConfig};
use crate::context::ObjectType;
use crate::{Context, ContextBound, Datatype};

extern crate tiledb_sys as ffi;
use crate::string::{RawTDBString, TDBString};
use crate::Result as TileDBResult;
pub type QueryType = crate::array::Mode;
use crate::metadata::Metadata;

pub struct GroupInfo {
    pub uri: String,
    pub group_type: ObjectType,
    pub name: String,
}

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

    pub fn create<S>(context: &'ctx Context, name: S) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let c_name = cstring!(name.as_ref());
        context.capi_return(unsafe {
            ffi::tiledb_group_create(context.capi(), c_name.as_ptr())
        })
    }

    pub fn open<S>(
        context: &'ctx Context,
        uri: S,
        query_type: QueryType,
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
        Ok(Self::new(context, RawGroup::new(group_raw)))
    }

    pub fn uri(&self, context: &'ctx Context) -> TileDBResult<String> {
        let c_context = self.context.capi();
        let mut c_uri = std::ptr::null::<std::ffi::c_char>();
        context.capi_return(unsafe {
            ffi::tiledb_group_get_uri(c_context, Self::capi(self), &mut c_uri)
        })?;
        let uri = unsafe { std::ffi::CStr::from_ptr(c_uri) };
        Ok(String::from(uri.to_string_lossy()))
    }

    pub fn query_type(
        &self,
        context: &'ctx Context,
    ) -> TileDBResult<QueryType> {
        let c_context = self.context.capi();
        let mut c_type: ffi::tiledb_query_type_t = out_ptr!();
        context.capi_return(unsafe {
            ffi::tiledb_group_get_query_type(
                c_context,
                Self::capi(self),
                &mut c_type,
            )
        })?;
        QueryType::try_from(c_type)
    }

    pub fn delete_group<S>(
        &self,
        context: &'ctx Context,
        uri: S,
        recursive: bool,
    ) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let ctx = context.capi();
        let c_uri = cstring!(uri.as_ref());
        let c_recursive: u8 = if recursive { 1 } else { 0 };
        context.capi_return(unsafe {
            ffi::tiledb_group_delete_group(
                ctx,
                Self::capi(self),
                c_uri.as_ptr(),
                c_recursive,
            )
        })?;
        Ok(())
    }

    pub fn add_member<S, T>(
        &self,
        context: &'ctx Context,
        uri: S,
        relative: bool,
        name: T,
    ) -> TileDBResult<()>
    where
        S: AsRef<str>,
        T: AsRef<str>,
    {
        let ctx = context.capi();
        let c_uri = cstring!(uri.as_ref());
        let c_name = cstring!(name.as_ref());
        let c_relative: u8 = if relative { 1 } else { 0 };
        context.capi_return(unsafe {
            ffi::tiledb_group_add_member(
                ctx,
                Self::capi(self),
                c_uri.as_ptr(),
                c_relative,
                c_name.as_ptr(),
            )
        })?;
        Ok(())
    }

    pub fn delete_member<S>(
        &self,
        context: &'ctx Context,
        name_or_uri: S,
    ) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let ctx = context.capi();
        let c_name_or_uri = cstring!(name_or_uri.as_ref());
        context.capi_return(unsafe {
            ffi::tiledb_group_remove_member(
                ctx,
                Self::capi(self),
                c_name_or_uri.as_ptr(),
            )
        })?;
        Ok(())
    }

    pub fn get_member_count(
        &self,
        context: &'ctx Context,
    ) -> TileDBResult<u64> {
        let ctx = context.capi();
        let mut c_count: u64 = out_ptr!();
        context.capi_return(unsafe {
            ffi::tiledb_group_get_member_count(
                ctx,
                Self::capi(self),
                &mut c_count,
            )
        })?;
        Ok(c_count)
    }

    pub fn get_member_by_index(
        &self,
        context: &'ctx Context,
        index: u64,
    ) -> TileDBResult<GroupInfo> {
        let ctx = context.capi();
        let mut tiledb_uri = std::ptr::null_mut::<ffi::tiledb_string_t>();
        let mut tiledb_name = std::ptr::null_mut::<ffi::tiledb_string_t>();
        let mut tiledb_type: ffi::tiledb_object_t = out_ptr!();
        context.capi_return(unsafe {
            ffi::tiledb_group_get_member_by_index_v2(
                ctx,
                Self::capi(self),
                index,
                &mut tiledb_uri as *mut *mut ffi::tiledb_string_t,
                &mut tiledb_type,
                &mut tiledb_name as *mut *mut ffi::tiledb_string_t,
            )
        })?;
        let uri = TDBString {
            raw: RawTDBString::Owned(tiledb_uri),
        }
        .to_string()?;

        let name = TDBString {
            raw: RawTDBString::Owned(tiledb_name),
        }
        .to_string()?;

        let object_type = ObjectType::try_from(tiledb_type)?;
        Ok(GroupInfo {
            uri,
            group_type: object_type,
            name,
        })
    }

    pub fn get_member_by_name<S>(
        &self,
        context: &'ctx Context,
        name: S,
    ) -> TileDBResult<GroupInfo>
    where
        S: AsRef<str>,
    {
        let ctx = context.capi();
        let mut tiledb_uri = std::ptr::null_mut::<ffi::tiledb_string_t>();
        let mut tiledb_type: ffi::tiledb_object_t = out_ptr!();
        let c_name = cstring!(name.as_ref());
        context.capi_return(unsafe {
            ffi::tiledb_group_get_member_by_name_v2(
                ctx,
                Self::capi(self),
                c_name.as_ptr(),
                &mut tiledb_uri as *mut *mut ffi::tiledb_string_t,
                &mut tiledb_type,
            )
        })?;

        let uri = TDBString {
            raw: RawTDBString::Owned(tiledb_uri),
        }
        .to_string()?;

        let object_type = ObjectType::try_from(tiledb_type)?;
        Ok(GroupInfo {
            uri,
            group_type: object_type,
            name: name.as_ref().to_string(),
        })
    }

    pub fn is_relative_uri<S>(
        &self,
        context: &'ctx Context,
        name: S,
    ) -> TileDBResult<bool>
    where
        S: AsRef<str>,
    {
        let ctx = context.capi();
        let mut c_relative: u8 = out_ptr!();
        let c_name = cstring!(name.as_ref());
        context.capi_return(unsafe {
            ffi::tiledb_group_get_is_relative_uri_by_name(
                ctx,
                Self::capi(self),
                c_name.as_ptr(),
                &mut c_relative,
            )
        })?;
        Ok(c_relative > 0)
    }

    pub fn is_open(&self, context: &'ctx Context) -> TileDBResult<bool> {
        let ctx = context.capi();
        let mut c_open: i32 = out_ptr!();
        context.capi_return(unsafe {
            ffi::tiledb_group_is_open(ctx, Self::capi(self), &mut c_open)
        })?;
        Ok(c_open > 0)
    }

    pub fn dump(
        &self,
        context: &'ctx Context,
        recursive: bool,
    ) -> TileDBResult<Option<String>> {
        let ctx = context.capi();
        let mut c_str = std::ptr::null_mut::<std::ffi::c_char>();
        let c_recursive = if recursive { 1 } else { 0 };
        context.capi_return(unsafe {
            ffi::tiledb_group_dump_str(
                ctx,
                Self::capi(self),
                &mut c_str as *mut *mut std::ffi::c_char,
                c_recursive,
            )
        })?;
        let group_dump = unsafe { std::ffi::CStr::from_ptr(c_str) };
        let group_dump_rust_str = group_dump.to_string_lossy().into_owned();

        // ABI TODO: free string here?
        Ok(Some(group_dump_rust_str))
    }

    pub fn set_config(
        &self,
        context: &'ctx Context,
        config: &Config,
    ) -> TileDBResult<()> {
        let ctx = context.capi();
        let cfg = config.capi();
        context.capi_return(unsafe {
            ffi::tiledb_group_set_config(ctx, Self::capi(self), cfg)
        })?;
        Ok(())
    }

    pub fn get_config(&self, context: &'ctx Context) -> TileDBResult<Config> {
        let ctx = context.capi();
        let mut c_cfg: *mut ffi::tiledb_config_t = out_ptr!();
        context.capi_return(unsafe {
            ffi::tiledb_group_get_config(ctx, Self::capi(self), &mut c_cfg)
        })?;

        Ok(Config {
            raw: RawConfig::Owned(c_cfg),
        })
    }

    pub fn put_metadata(
        &self,
        context: &'ctx Context,
        metadata: Metadata,
    ) -> TileDBResult<()> {
        let ctx = context.capi();
        let c_key = cstring!(metadata.key);
        let (vec_size, vec_ptr, datatype) = metadata.value.c_vec();
        // Convert value to pointer with value in it by casing on type/using fn_typed!
        // Then run the C function
        // Only supporting numeric types right now.
        context.capi_return(unsafe {
            ffi::tiledb_group_put_metadata(
                ctx,
                Self::capi(self),
                c_key.as_ptr(),
                datatype,
                vec_size.try_into().unwrap(),
                vec_ptr,
            )
        })?;
        Ok(())
    }

    pub fn delete_metadata<S>(
        &self,
        context: &'ctx Context,
        name: S,
    ) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let ctx = context.capi();
        let c_name = cstring!(name.as_ref());
        context.capi_return(unsafe {
            ffi::tiledb_group_delete_metadata(
                ctx,
                Self::capi(self),
                c_name.as_ptr(),
            )
        })?;
        Ok(())
    }

    pub fn get_metadata<S>(
        &self,
        context: &'ctx Context,
        name: S,
    ) -> TileDBResult<Metadata>
    where
        S: AsRef<str>,
    {
        let ctx = context.capi();
        let c_name = cstring!(name.as_ref());
        let mut vec_size: u32 = out_ptr!();
        let mut c_datatype: ffi::tiledb_datatype_t = out_ptr!();
        let mut vec_ptr = std::ptr::null::<std::ffi::c_void>();
        context.capi_return(unsafe {
            ffi::tiledb_group_get_metadata(
                ctx,
                Self::capi(self),
                c_name.as_ptr(),
                &mut c_datatype,
                &mut vec_size,
                &mut vec_ptr,
            )
        })?;
        let datatype = Datatype::try_from(c_datatype)?;
        Ok(Metadata::new(
            String::from(name.as_ref()),
            datatype,
            vec_ptr,
            vec_size,
        ))
    }

    pub fn get_metadata_num(
        &self,
        context: &'ctx Context,
    ) -> TileDBResult<u64> {
        let ctx = context.capi();
        let mut num: u64 = out_ptr!();
        context.capi_return(unsafe {
            ffi::tiledb_group_get_metadata_num(ctx, Self::capi(self), &mut num)
        })?;
        Ok(num)
    }

    pub fn get_metadata_from_index(
        &self,
        context: &'ctx Context,
        index: u64,
    ) -> TileDBResult<Metadata> {
        let ctx = context.capi();
        let mut key_ptr = std::ptr::null::<std::ffi::c_char>();
        let mut key_len: u32 = out_ptr!();
        let mut c_datatype: ffi::tiledb_datatype_t = out_ptr!();
        let mut vec_ptr = std::ptr::null::<std::ffi::c_void>();
        let mut vec_size: u32 = out_ptr!();
        context.capi_return(unsafe {
            ffi::tiledb_group_get_metadata_from_index(
                ctx,
                Self::capi(self),
                index,
                &mut key_ptr,
                &mut key_len,
                &mut c_datatype,
                &mut vec_size,
                &mut vec_ptr,
            )
        })?;

        let c_key = unsafe { std::ffi::CStr::from_ptr(key_ptr) };
        let key = c_key.to_string_lossy().into_owned();
        let datatype = Datatype::try_from(c_datatype)?;
        Ok(Metadata::new(key, datatype, vec_ptr, vec_size))
    }

    pub fn has_metadata_key<S>(
        &self,
        context: &'ctx Context,
        name: S,
    ) -> TileDBResult<Option<Datatype>>
    where
        S: AsRef<str>,
    {
        let ctx = context.capi();
        let c_name = cstring!(name.as_ref());
        let mut c_datatype: ffi::tiledb_datatype_t = out_ptr!();
        let mut exists: i32 = out_ptr!();
        context.capi_return(unsafe {
            ffi::tiledb_group_has_metadata_key(
                ctx,
                Self::capi(self),
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

    pub fn consolidate_metadata<S>(
        &self,
        context: &'ctx Context,
        config: &Config,
        group_uri: S,
    ) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let ctx = context.capi();
        let c_group_uri = cstring!(group_uri.as_ref());
        let cfg = config.capi();
        context.capi_return(unsafe {
            ffi::tiledb_group_consolidate_metadata(
                ctx,
                c_group_uri.as_ptr(),
                cfg,
            )
        })?;
        Ok(())
    }

    pub fn vacuum_metadata<S>(
        &self,
        context: &'ctx Context,
        config: &Config,
        group_uri: S,
    ) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let ctx = context.capi();
        let c_group_uri = cstring!(group_uri.as_ref());
        let cfg = config.capi();
        context.capi_return(unsafe {
            ffi::tiledb_group_vacuum_metadata(ctx, c_group_uri.as_ptr(), cfg)
        })?;
        Ok(())
    }
}

impl Drop for Group<'_> {
    fn drop(&mut self) {
        let c_context = self.context.capi();
        let c_group = Self::capi(self);
        self.context
            .capi_return(unsafe { ffi::tiledb_group_close(c_context, c_group) })
            .expect("TileDB internal error when closing group");
    }
}
