use std::ops::Deref;

use crate::config::{Config, RawConfig};
use crate::context::{CApiInterface, ContextBound, ObjectType};
use crate::error::Error;
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
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_group_create(ctx, c_name.as_ptr())
        })
    }

    pub fn open<S>(
        context: &'ctx Context,
        uri: S,
        query_type: QueryType,
        config: Option<&Config>,
    ) -> TileDBResult<Self>
    where
        S: AsRef<str>,
    {
        let mut group_raw: *mut ffi::tiledb_group_t = out_ptr!();

        let c_uri = cstring!(uri.as_ref());

        context.capi_call(|ctx| unsafe {
            ffi::tiledb_group_alloc(ctx, c_uri.as_ptr(), &mut group_raw)
        })?;

        if let Some(cfg) = config {
            let c_cfg = cfg.capi();
            context.capi_call(|ctx| unsafe {
                ffi::tiledb_group_set_config(ctx, group_raw, c_cfg)
            })?;
        }

        let raw_group = RawGroup::new(group_raw);
        let query_type_raw = query_type.capi_enum();
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_group_open(ctx, group_raw, query_type_raw)
        })?;

        let mut c_open: i32 = out_ptr!();
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_group_is_open(ctx, raw_group.ffi, &mut c_open)
        })?;

        if c_open < 0 {
            return Err(Error::LibTileDB(
                "tiledb_group_open call does not successfully open group."
                    .to_string(),
            ));
        }

        Ok(Self::new(context, raw_group))
    }

    pub fn uri(&self) -> TileDBResult<String> {
        let mut c_uri: *const std::ffi::c_char = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_group_get_uri(ctx, Self::capi(self), &mut c_uri)
        })?;
        let uri = unsafe { std::ffi::CStr::from_ptr(c_uri) };
        Ok(String::from(uri.to_string_lossy()))
    }

    pub fn query_type(&self) -> TileDBResult<QueryType> {
        let c_group = self.capi();
        let mut c_type: ffi::tiledb_query_type_t = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_group_get_query_type(ctx, c_group, &mut c_type)
        })?;
        QueryType::try_from(c_type)
    }

    // Deletes the group itself. Can only be called once.
    pub fn delete_group<S>(self, uri: S, recursive: bool) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let c_group = self.capi();
        let c_uri = cstring!(uri.as_ref());
        let c_recursive: u8 = if recursive { 1 } else { 0 };
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_group_delete_group(
                ctx,
                c_group,
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
        let c_group = self.capi();
        let c_uri = cstring!(uri.as_ref());
        let c_name = match name.as_ref() {
            None => None,
            Some(s) => Some(cstring!(s.as_ref())),
        };
        let c_ptr = match c_name.as_ref() {
            None => out_ptr!(),
            Some(s) => s.as_ptr(),
        };
        let c_relative: u8 = if relative { 1 } else { 0 };
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_group_add_member(
                ctx,
                c_group,
                c_uri.as_ptr(),
                c_relative,
                c_ptr,
            )
        })?;
        Ok(())
    }

    // Deletes a member of the group.
    pub fn delete_member<S>(&mut self, name_or_uri: S) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let c_group = self.capi();
        let c_name_or_uri = cstring!(name_or_uri.as_ref());
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_group_remove_member(
                ctx,
                c_group,
                c_name_or_uri.as_ptr(),
            )
        })?;
        Ok(())
    }

    pub fn num_members(&self) -> TileDBResult<u64> {
        let c_group = self.capi();
        let mut c_count: u64 = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_group_get_member_count(ctx, c_group, &mut c_count)
        })?;
        Ok(c_count)
    }

    pub fn member(&self, key: LookupKey) -> TileDBResult<GroupInfo> {
        let c_group = self.capi();
        let mut tiledb_uri: *mut ffi::tiledb_string_t = out_ptr!();
        let mut tiledb_type: ffi::tiledb_object_t = out_ptr!();
        let name: String = match key {
            LookupKey::Index(index) => {
                let mut tiledb_name: *mut ffi::tiledb_string_t = out_ptr!();
                self.capi_call(|ctx| unsafe {
                    ffi::tiledb_group_get_member_by_index_v2(
                        ctx,
                        c_group,
                        index as u64,
                        &mut tiledb_uri as *mut *mut ffi::tiledb_string_t,
                        &mut tiledb_type,
                        &mut tiledb_name as *mut *mut ffi::tiledb_string_t,
                    )
                })?;
                let name = TDBString {
                    raw: RawTDBString::Owned(tiledb_name),
                }
                .to_string();
                let name = name?;
                Ok(name) as TileDBResult<String>
            }
            LookupKey::Name(name) => {
                let c_name = cstring!(name.as_ref() as &str);
                self.capi_call(|ctx| unsafe {
                    ffi::tiledb_group_get_member_by_name_v2(
                        ctx,
                        c_group,
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
        .to_string();
        let uri = uri?;

        let object_type = ObjectType::try_from(tiledb_type)?;
        Ok(GroupInfo {
            uri,
            group_type: object_type,
            name,
        })
    }

    pub fn is_relative_uri<S>(&self, name: S) -> TileDBResult<bool>
    where
        S: AsRef<str>,
    {
        let c_group = self.capi();
        let mut c_relative: u8 = out_ptr!();
        let c_name = cstring!(name.as_ref());
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_group_get_is_relative_uri_by_name(
                ctx,
                c_group,
                c_name.as_ptr(),
                &mut c_relative,
            )
        })?;
        Ok(c_relative > 0)
    }

    pub fn is_open(&self) -> TileDBResult<bool> {
        let c_group = self.capi();
        let mut c_open: i32 = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_group_is_open(ctx, c_group, &mut c_open)
        })?;
        Ok(c_open > 0)
    }

    pub fn dump(&self, recursive: bool) -> TileDBResult<String> {
        let c_group = self.capi();
        let mut c_str: *mut std::ffi::c_char = out_ptr!();
        let c_recursive = if recursive { 1 } else { 0 };
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_group_dump_str(
                ctx,
                c_group,
                &mut c_str as *mut *mut std::ffi::c_char,
                c_recursive,
            )
        })?;
        let group_dump = unsafe { std::ffi::CStr::from_ptr(c_str) };
        let group_dump_rust_str = group_dump.to_string_lossy().into_owned();

        Ok(group_dump_rust_str)
    }

    pub fn config(&self) -> TileDBResult<Config> {
        let c_group = self.capi();
        let mut c_cfg: *mut ffi::tiledb_config_t = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_group_get_config(ctx, c_group, &mut c_cfg)
        })?;

        Ok(Config {
            raw: RawConfig::Owned(c_cfg),
        })
    }

    pub fn put_metadata(&mut self, metadata: Metadata) -> TileDBResult<()> {
        let c_group = self.capi();
        let (vec_size, vec_ptr, datatype) = metadata.c_data();
        let c_key = cstring!(metadata.key);
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_group_put_metadata(
                ctx,
                c_group,
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
        let c_group = self.capi();
        let c_name = cstring!(name.as_ref());
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_group_delete_metadata(ctx, c_group, c_name.as_ptr())
        })?;
        Ok(())
    }

    pub fn num_metadata(&self) -> TileDBResult<u64> {
        let c_group = self.capi();
        let mut num: u64 = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_group_get_metadata_num(ctx, c_group, &mut num)
        })?;
        Ok(num)
    }

    pub fn metadata(&self, key: LookupKey) -> TileDBResult<Metadata> {
        let c_group = self.capi();
        let mut vec_size: u32 = out_ptr!();
        let mut c_datatype: ffi::tiledb_datatype_t = out_ptr!();
        let mut vec_ptr: *const std::ffi::c_void = out_ptr!();

        let name: String = match key {
            LookupKey::Index(index) => {
                let mut key_ptr: *const std::ffi::c_char = out_ptr!();
                let mut key_len: u32 = out_ptr!();
                self.capi_call(|ctx| unsafe {
                    ffi::tiledb_group_get_metadata_from_index(
                        ctx,
                        c_group,
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
                    ffi::tiledb_group_get_metadata(
                        ctx,
                        c_group,
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
        let c_group = self.capi();
        let c_name = cstring!(name.as_ref());
        let mut c_datatype: ffi::tiledb_datatype_t = out_ptr!();
        let mut exists: i32 = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_group_has_metadata_key(
                ctx,
                c_group,
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
        &mut self,
        config: &Config,
        group_uri: S,
    ) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let c_group_uri = cstring!(group_uri.as_ref());
        let cfg = config.capi();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_group_consolidate_metadata(
                ctx,
                c_group_uri.as_ptr(),
                cfg,
            )
        })?;
        Ok(())
    }

    pub fn vacuum_metadata<S>(
        &mut self,
        config: &Config,
        group_uri: S,
    ) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let c_group_uri = cstring!(group_uri.as_ref());
        let cfg = config.capi();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_group_vacuum_metadata(ctx, c_group_uri.as_ptr(), cfg)
        })?;
        Ok(())
    }
}

impl Drop for Group<'_> {
    fn drop(&mut self) {
        let c_group = self.capi();
        let mut c_open: i32 = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_group_is_open(ctx, c_group, &mut c_open)
        })
        .expect("TileDB internal error when checking for open group.");

        // We check if the group is open, and only delete when the group is open, because
        // if delete_group is called, then we should not close the group.
        if c_open > 0 {
            self.capi_call(|ctx| unsafe {
                ffi::tiledb_group_close(ctx, c_group)
            })
            .expect("TileDB internal error when closing group");
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        array::{Array, ArrayType},
        config::Config,
        context::Context,
        datatype::Datatype,
        error::Error,
        group::{Group, QueryType},
        key::LookupKey,
        metadata::{self, Metadata},
    };
    use crate::{
        array::{
            AttributeBuilder, CellOrder, Dimension, DimensionBuilder,
            DomainBuilder, SchemaBuilder, TileOrder,
        },
        context::ObjectType,
        Result as TileDBResult,
    };
    use tempfile::TempDir;
    #[test]
    fn test_group_metadata() -> TileDBResult<()> {
        let tmp_dir =
            TempDir::new().map_err(|e| Error::Other(e.to_string()))?;

        let tdb = Context::new()?;
        let group1_path = tmp_dir.path().join("group1");
        let group1_uri = group1_path.to_str().unwrap();
        Group::create(&tdb, group1_uri)?;
        {
            let mut group1_err =
                Group::open(&tdb, group1_uri, QueryType::Read, None)?;
            let res = group1_err.put_metadata(Metadata::new(
                "key".to_owned(),
                Datatype::Int32,
                vec![5],
            )?);
            assert!(res.is_err());
        }

        {
            let mut group1_write =
                Group::open(&tdb, group1_uri, QueryType::Write, None)?;

            group1_write.put_metadata(Metadata::new(
                "key".to_owned(),
                Datatype::Int32,
                vec![5],
            )?)?;
            group1_write.put_metadata(Metadata::new(
                "aaa".to_owned(),
                Datatype::Int32,
                vec![5],
            )?)?;
            group1_write.put_metadata(Metadata::new(
                "bb".to_owned(),
                Datatype::Float32,
                vec![1.1f32, 2.2f32],
            )?)?;
        }

        {
            let group1_read =
                Group::open(&tdb, group1_uri, QueryType::Read, None)?;
            let metadata_aaa =
                group1_read.metadata(LookupKey::Name("aaa".to_owned()))?;
            assert_eq!(metadata_aaa.datatype, Datatype::Int32);
            assert_eq!(metadata_aaa.value, metadata::Value::Int32(vec!(5)));
            assert_eq!(metadata_aaa.key, "aaa");

            let metadata_num = group1_read.num_metadata()?;
            assert_eq!(metadata_num, 3);

            let metadata_bb = group1_read.metadata(LookupKey::Index(1))?;
            assert_eq!(metadata_bb.datatype, Datatype::Float32);
            assert_eq!(metadata_bb.key, "bb");
            assert_eq!(
                metadata_bb.value,
                metadata::Value::Float32(vec!(1.1f32, 2.2f32))
            );

            let has_aaa = group1_read.has_metadata_key("aaa")?;
            assert_eq!(has_aaa, Some(Datatype::Int32));
        }

        {
            let mut group1_write =
                Group::open(&tdb, group1_uri, QueryType::Write, None)?;
            group1_write.delete_metadata("aaa")?;
        }

        {
            let group1_read =
                Group::open(&tdb, group1_uri, QueryType::Read, None)?;
            let has_aaa = group1_read.has_metadata_key("aaa")?;
            assert_eq!(has_aaa, None);
        }

        // Cleanup
        let group1_write =
            Group::open(&tdb, group1_uri, QueryType::ModifyExclusive, None)?;
        group1_write.delete_group(group1_uri, true)?;

        tmp_dir.close().map_err(|e| Error::Other(e.to_string()))?;
        Ok(())
    }

    fn create_array<S>(array_uri: S, array_type: ArrayType) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let tdb = Context::new()?;

        // The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
        let domain = {
            let rows: Dimension = DimensionBuilder::new::<i32>(
                &tdb,
                "rows",
                Datatype::Int32,
                &[1, 4],
                &4,
            )?
            .build();
            let cols: Dimension = DimensionBuilder::new::<i32>(
                &tdb,
                "cols",
                Datatype::Int32,
                &[1, 4],
                &4,
            )?
            .build();

            DomainBuilder::new(&tdb)?
                .add_dimension(rows)?
                .add_dimension(cols)?
                .build()
        };

        // Create a single attribute "a" so each (i,j) cell can store an integer
        let attribute_a =
            AttributeBuilder::new(&tdb, "a", Datatype::Int32)?.build();

        // Create array schema
        let schema = SchemaBuilder::new(&tdb, array_type, domain)?
            .tile_order(TileOrder::RowMajor)?
            .cell_order(CellOrder::RowMajor)?
            .add_attribute(attribute_a)?
            .build()?;

        // Create array
        Array::create(&tdb, array_uri, schema)?;
        Ok(())
    }

    #[test]
    fn test_group_functionality() -> TileDBResult<()> {
        let tmp_dir =
            TempDir::new().map_err(|e| Error::Other(e.to_string()))?;

        let tdb = Context::new()?;
        let group_path = tmp_dir.path().join("group2");
        let group_uri = group_path.to_str().unwrap();
        Group::create(&tdb, group_uri)?;

        {
            let group_read =
                Group::open(&tdb, group_uri, QueryType::Read, None)?;
            let group_uri_copy = group_read.uri()?;
            assert_eq!(
                group_uri_copy,
                "file://".to_owned() + group_path.to_str().unwrap()
            );
            let group_type = group_read.query_type()?;
            assert_eq!(group_type, QueryType::Read);

            let open = group_read.is_open()?;
            assert!(open);
        }

        create_array(group_uri.to_owned() + "/aa", ArrayType::Dense)?;
        create_array(group_uri.to_owned() + "/bb", ArrayType::Dense)?;
        create_array(group_uri.to_owned() + "/cc", ArrayType::Sparse)?;

        {
            let mut group_write =
                Group::open(&tdb, group_uri, QueryType::Write, None)?;
            group_write.add_member("aa", true, Some("aa".to_owned()))?;
            group_write.add_member("bb", true, Some("bb".to_owned()))?;
            group_write.add_member("cc", true, Some("cc".to_owned()))?;
        }

        {
            let group_read =
                Group::open(&tdb, group_uri, QueryType::Read, None)?;
            let opt_string = group_read.dump(true)?;
            let expected_str =
                "group2 GROUP\n|-- aa ARRAY\n|-- bb ARRAY\n|-- cc ARRAY\n";
            assert_eq!(opt_string, expected_str.to_string());
        }

        {
            let mut group_write =
                Group::open(&tdb, group_uri, QueryType::Write, None)?;
            group_write.delete_member("bb")?;
        }

        {
            let group_read =
                Group::open(&tdb, group_uri, QueryType::Read, None)?;
            let opt_string = group_read.dump(true)?;
            let expected_str = "group2 GROUP\n|-- aa ARRAY\n|-- cc ARRAY\n";
            assert_eq!(opt_string, expected_str.to_string());

            let group_read =
                Group::open(&tdb, group_uri, QueryType::Read, None)?;
            let count = group_read.num_members()?;
            assert_eq!(count, 2);

            let member_aa =
                group_read.member(LookupKey::Name("aa".to_owned()))?;
            assert_eq!(member_aa.name, "aa".to_owned());
            assert_eq!(member_aa.group_type, ObjectType::Array);
            assert_eq!("file://".to_owned() + group_uri + "/aa", member_aa.uri);

            let member_cc = group_read.member(LookupKey::Index(1))?;
            assert_eq!(member_cc.name, "cc".to_owned());
            assert_eq!(member_cc.group_type, ObjectType::Array);
            assert_eq!("file://".to_owned() + group_uri + "/cc", member_cc.uri);

            let is_aa_relative = group_read.is_relative_uri("aa")?;
            assert!(is_aa_relative);
        }

        Ok(())
    }

    #[test]
    fn test_group_config() -> TileDBResult<()> {
        let tmp_dir =
            TempDir::new().map_err(|e| Error::Other(e.to_string()))?;

        let tdb = Context::new()?;
        let group_path = tmp_dir.path().join("group");
        let group_uri = group_path.to_str().unwrap();
        Group::create(&tdb, group_uri)?;

        let mut cfg = Config::new()?;
        cfg.set("foo", "bar")?;

        let group_read: Group<'_> =
            Group::open(&tdb, group_uri, QueryType::Read, Some(&cfg))?;
        let cfg_copy = group_read.config()?;
        assert!(cfg.eq(&cfg_copy));

        Ok(())
    }
}
