use std::ops::Deref;

use crate::config::{Config, RawConfig};
use crate::context::{ObjectType, ContextBound};
use crate::key::LookupKey;
use crate::{Context, Datatype};

extern crate tiledb_sys as ffi;
use crate::string::{RawTDBString, TDBString};
use crate::Result as TileDBResult;
pub type QueryType = crate::array::Mode;
use crate::metadata::Metadata;

#[derive(Clone, Debug, PartialEq)]
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

#[derive(ContextBound)]
pub struct Group<'ctx> {
    #[context]
    context: &'ctx Context,
    raw: RawGroup,
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
        let c_context = context.capi();
        let mut group_raw: *mut ffi::tiledb_group_t = out_ptr!();

        let c_uri = cstring!(uri.as_ref());

        context.capi_return(unsafe {
            ffi::tiledb_group_alloc(c_context, c_uri.as_ptr(), &mut group_raw)
        })?;

        let raw_group = RawGroup::new(group_raw);

        let query_type_raw = query_type.capi_enum();
        context.capi_return(unsafe {
            ffi::tiledb_group_open(c_context, group_raw, query_type_raw)
        })?;
        Ok(Self::new(context, raw_group))
    }

    pub fn uri(&self) -> TileDBResult<String> {
        let c_context = self.context.capi();
        let mut c_uri: *const std::ffi::c_char = out_ptr!();
        self.context.capi_return(unsafe {
            ffi::tiledb_group_get_uri(c_context, Self::capi(self), &mut c_uri)
        })?;
        let uri = unsafe { std::ffi::CStr::from_ptr(c_uri) };
        Ok(String::from(uri.to_string_lossy()))
    }

    pub fn query_type(
        &self
    ) -> TileDBResult<QueryType> {
        let c_context = self.context.capi();
        let mut c_type: ffi::tiledb_query_type_t = out_ptr!();
        self.context.capi_return(unsafe {
            ffi::tiledb_group_get_query_type(
                c_context,
                Self::capi(self),
                &mut c_type,
            )
        })?;
        QueryType::try_from(c_type)
    }

    pub fn delete_group<S>(
        self,
        uri: S,
        recursive: bool,
    ) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let c_context = self.context.capi();
        let c_uri = cstring!(uri.as_ref());
        let c_recursive: u8 = if recursive { 1 } else { 0 };
        self.context.capi_return(unsafe {
            ffi::tiledb_group_delete_group(
                c_context,
                Self::capi(&self),
                c_uri.as_ptr(),
                c_recursive,
            )
        })?;
        Ok(())
    }

    pub fn add_member<S, T>(
        &mut self,
        uri: S,
        relative: bool,
        name: Option<T>,
    ) -> TileDBResult<()>
    where
        S: AsRef<str>,
        T: AsRef<str>,
    {
        println!("{}", uri.as_ref());
        let c_context = self.context.capi();
        let c_uri = cstring!(uri.as_ref());
        let c_name = match name.as_ref() {   
            None => None,
            Some(s) => Some(cstring!(s.as_ref()))
        };
        let c_ptr = match c_name.as_ref() {
            None => out_ptr!(),
            Some(s) => s.as_ptr()
        };
        let c_relative: u8 = if relative { 1 } else { 0 };
        self.context.capi_return(unsafe {
            ffi::tiledb_group_add_member(
                c_context,
                Self::capi(self),
                c_uri.as_ptr(),
                c_relative,
                c_ptr,
            )
        })?;
        Ok(())
    }

    pub fn delete_member<S>(
        &mut self,
        name_or_uri: S,
    ) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let c_context = self.context.capi();
        let c_name_or_uri = cstring!(name_or_uri.as_ref());
        self.context.capi_return(unsafe {
            ffi::tiledb_group_remove_member(
                c_context,
                Self::capi(self),
                c_name_or_uri.as_ptr(),
            )
        })?;
        Ok(())
    }

    pub fn num_members(
        &self,
    ) -> TileDBResult<u64> {
        let c_context = self.context.capi();
        let mut c_count: u64 = out_ptr!();
        self.context.capi_return(unsafe {
            ffi::tiledb_group_get_member_count(
                c_context,
                Self::capi(self),
                &mut c_count,
            )
        })?;
        Ok(c_count)
    }

    pub fn member(&self, key : LookupKey) -> TileDBResult<GroupInfo> {
        let c_context = self.context.capi();
        let mut tiledb_uri: *mut ffi::tiledb_string_t = out_ptr!();
        let mut tiledb_type: ffi::tiledb_object_t = out_ptr!();
        let name : String = match key {
            LookupKey::Index(index) => {
                let mut tiledb_name: *mut ffi::tiledb_string_t = out_ptr!();
                self.context.capi_return(unsafe {
                    ffi::tiledb_group_get_member_by_index_v2(
                        c_context,
                        Self::capi(self),
                        index.try_into().unwrap(),
                        &mut tiledb_uri as *mut *mut ffi::tiledb_string_t,
                        &mut tiledb_type,
                        &mut tiledb_name as *mut *mut ffi::tiledb_string_t,
                    )
                })?;
                let name : String = TDBString {
                    raw: RawTDBString::Owned(tiledb_name),
                }
                .to_string()?;
                Ok(name) as TileDBResult<String>
            },
            LookupKey::Name(name) => {
                let c_name = cstring!(name.as_ref() as &str);
                self.context.capi_return(unsafe {
                    ffi::tiledb_group_get_member_by_name_v2(
                        c_context,
                        Self::capi(self),
                        c_name.as_ptr(),
                        &mut tiledb_uri as *mut *mut ffi::tiledb_string_t,
                        &mut tiledb_type,
                    )
                })?;
                Ok(name.to_owned())
            }
        }?;

        let uri = TDBString {
            raw: RawTDBString::Owned(tiledb_uri),
        }
        .to_string()?;

        let object_type = ObjectType::try_from(tiledb_type)?;
        Ok(GroupInfo {
            uri,
            group_type: object_type,
            name
        })
    }

    pub fn is_relative_uri<S>(
        &self,
        name: S,
    ) -> TileDBResult<bool>
    where
        S: AsRef<str>,
    {
        let c_context = self.context.capi();
        let mut c_relative: u8 = out_ptr!();
        let c_name = cstring!(name.as_ref());
        self.context.capi_return(unsafe {
            ffi::tiledb_group_get_is_relative_uri_by_name(
                c_context,
                Self::capi(self),
                c_name.as_ptr(),
                &mut c_relative,
            )
        })?;
        Ok(c_relative > 0)
    }

    pub fn is_open(&self) -> TileDBResult<bool> {
        let c_context = self.context.capi();
        let mut c_open: i32 = out_ptr!();
        self.context.capi_return(unsafe {
            ffi::tiledb_group_is_open(c_context, Self::capi(self), &mut c_open)
        })?;
        Ok(c_open > 0)
    }

    pub fn dump(
        &self,
        recursive: bool,
    ) -> TileDBResult<Option<String>> {
        let c_context = self.context.capi();
        let mut c_str: *mut std::ffi::c_char = out_ptr!();
        let c_recursive = if recursive { 1 } else { 0 };
        self.context.capi_return(unsafe {
            ffi::tiledb_group_dump_str(
                c_context,
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
        &mut self,
        config: &Config,
    ) -> TileDBResult<()> {
        let c_context = self.context.capi();
        let cfg = config.capi();
        self.context.capi_return(unsafe {
            ffi::tiledb_group_set_config(c_context, Self::capi(self), cfg)
        })?;
        Ok(())
    }

    pub fn config(&self) -> TileDBResult<Config> {
        let c_context = self.context.capi();
        let mut c_cfg: *mut ffi::tiledb_config_t = out_ptr!();
        self.context.capi_return(unsafe {
            ffi::tiledb_group_get_config(c_context, Self::capi(self), &mut c_cfg)
        })?;

        Ok(Config {
            raw: RawConfig::Owned(c_cfg),
        })
    }

    pub fn put_metadata(
        &mut self,
        metadata: Metadata,
    ) -> TileDBResult<()> {
        println!("{:?}", metadata);
        let c_context = self.context.capi();
        let (vec_size, vec_ptr, datatype) = metadata.c_data();
        println!("{:?}", metadata);
        let c_key = cstring!(metadata.key.clone()); // we're partially moving metadata
        self.context.capi_return(unsafe {
            ffi::tiledb_group_put_metadata(
                c_context,
                Self::capi(self),
                c_key.as_ptr(),
                datatype,
                vec_size.try_into().unwrap(),
                vec_ptr,
            )
        })?;
        println!("{:?}", metadata);
        Ok(())
    }

    pub fn delete_metadata<S>(
        &mut self,
        name: S,
    ) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let c_context = self.context.capi();
        let c_name = cstring!(name.as_ref());
        self.context.capi_return(unsafe {
            ffi::tiledb_group_delete_metadata(
                c_context,
                Self::capi(self),
                c_name.as_ptr(),
            )
        })?;
        Ok(())
    }

    pub fn num_metadata(
        &self,
    ) -> TileDBResult<u64> {
        let c_context = self.context.capi();
        let mut num: u64 = out_ptr!();
        self.context.capi_return(unsafe {
            ffi::tiledb_group_get_metadata_num(c_context, Self::capi(self), &mut num)
        })?;
        Ok(num)
    }

    pub fn metadata(&self, key : LookupKey) -> TileDBResult<Metadata> {
        let c_context = self.context.capi();
        let mut vec_size: u32 = out_ptr!();
        let mut c_datatype: ffi::tiledb_datatype_t = out_ptr!();
        let mut vec_ptr: *const std::ffi::c_void = out_ptr!();

        let name : String = match key {
            LookupKey::Index(index) => {
                let mut key_ptr: *const std::ffi::c_char = out_ptr!();
                let mut key_len: u32 = out_ptr!();
                self.context.capi_return(unsafe {
                    ffi::tiledb_group_get_metadata_from_index(
                        c_context,
                        Self::capi(self),
                        index.try_into().unwrap(),
                        &mut key_ptr,
                        &mut key_len,
                        &mut c_datatype,
                        &mut vec_size,
                        &mut vec_ptr,
                    )
                })?;
                let c_key = unsafe { std::ffi::CStr::from_ptr(key_ptr) };
                Ok(c_key.to_string_lossy().into_owned()) as TileDBResult<String>
            },
            LookupKey::Name(name) => {
                let c_name = cstring!(name.as_ref() as &str);
                self.context.capi_return(unsafe {
                    ffi::tiledb_group_get_metadata(
                        c_context,
                        Self::capi(self),
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
        Ok(Metadata::new(name, datatype, vec_ptr, vec_size))
    }

    pub fn has_metadata_key<S>(
        &self,
        name: S,
    ) -> TileDBResult<Option<Datatype>>
    where
        S: AsRef<str>,
    {
        let c_context = self.context.capi();
        let c_name = cstring!(name.as_ref());
        let mut c_datatype: ffi::tiledb_datatype_t = out_ptr!();
        let mut exists: i32 = out_ptr!();
        self.context.capi_return(unsafe {
            ffi::tiledb_group_has_metadata_key(
                c_context,
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
        config: &Config,
        group_uri: S,
    ) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let c_context = self.context.capi();
        let c_group_uri = cstring!(group_uri.as_ref());
        let cfg = config.capi();
        self.context.capi_return(unsafe {
            ffi::tiledb_group_consolidate_metadata(
                c_context,
                c_group_uri.as_ptr(),
                cfg,
            )
        })?;
        Ok(())
    }

    pub fn vacuum_metadata<S>(
        &self,
        config: &Config,
        group_uri: S,
    ) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let c_context = self.context.capi();
        let c_group_uri = cstring!(group_uri.as_ref());
        let cfg = config.capi();
        self.context.capi_return(unsafe {
            ffi::tiledb_group_vacuum_metadata(c_context, c_group_uri.as_ptr(), cfg)
        })?;
        Ok(())
    }
}

impl Drop for Group<'_> {
    fn drop(&mut self) {
        let c_context = self.context.capi();
        let c_group = Self::capi(self);

        let mut c_open: i32 = out_ptr!();
        self.context
            .capi_return(unsafe {
                ffi::tiledb_group_is_open(c_context, c_group, &mut c_open)
            })
            .expect("TileDB internal error when checking for open group.");
        if c_open > 0 {
            self.context
                .capi_return(unsafe {
                    ffi::tiledb_group_close(c_context, c_group)
                })
                .expect("TileDB internal error when closing group");
        }
    }
}