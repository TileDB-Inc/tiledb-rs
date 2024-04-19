use std::ops::Deref;

use crate::config::{Config, RawConfig};
use crate::context::ObjectType;
use crate::{Context, ContextBound, Datatype};

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
        let mut group_raw: *mut ffi::tiledb_group_t = out_ptr!();

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
        let mut c_uri: *const std::ffi::c_char = out_ptr!();
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
        name: Option<T>,
    ) -> TileDBResult<()>
    where
        S: AsRef<str>,
        T: AsRef<str>,
    {
        let ctx = context.capi();
        let c_uri = cstring!(uri.as_ref());
        let c_ptr = match name {
            None => out_ptr!(),
            Some(s) => cstring!(s.as_ref()).as_ptr(),
        };
        let c_relative: u8 = if relative { 1 } else { 0 };
        context.capi_return(unsafe {
            ffi::tiledb_group_add_member(
                ctx,
                Self::capi(self),
                c_uri.as_ptr(),
                c_relative,
                c_ptr,
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
        let mut tiledb_uri: *mut ffi::tiledb_string_t = out_ptr!();
        let mut tiledb_name: *mut ffi::tiledb_string_t = out_ptr!();
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
        let mut tiledb_uri: *mut ffi::tiledb_string_t = out_ptr!();
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
        let mut c_str: *mut std::ffi::c_char = out_ptr!();
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
        println!("{:?}", metadata);
        let ctx = context.capi();
        let (vec_size, vec_ptr, datatype) = metadata.c_data();
        println!("{:?}", metadata);
        let c_key = cstring!(metadata.key.clone()); // we're partially moving metadata
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
        println!("{:?}", metadata);
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
        // TODO: figure out if you need to copy metadata in ::new
        let ctx = context.capi();
        let c_name = cstring!(name.as_ref());
        let mut vec_size: u32 = out_ptr!();
        let mut c_datatype: ffi::tiledb_datatype_t = out_ptr!();
        let mut vec_ptr: *const std::ffi::c_void = out_ptr!();
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
        let mut key_ptr: *const std::ffi::c_char = out_ptr!();
        let mut key_len: u32 = out_ptr!();
        let mut c_datatype: ffi::tiledb_datatype_t = out_ptr!();
        let mut vec_ptr: *const std::ffi::c_void = out_ptr!();
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

#[cfg(test)]
mod tests {
    use crate::{array::{AttributeBuilder, CellOrder, Dimension, DimensionBuilder, DomainBuilder, SchemaBuilder, TileOrder}, Result as TileDBResult};
    use tempfile::TempDir;
    use crate::{
        array::Array,
        array::ArrayType,
        context::Context,
        datatype::Datatype,
        error::Error,
        group::{Group, QueryType},
        metadata::{self, Metadata}
    };
    #[test]
    fn test_group_metadata() -> TileDBResult<()> {
        let tmp_dir = TempDir::new().map_err(|e| {
            Error::Other(
                e.to_string())
        })?;

        let tdb = Context::new()?;        
        let group1_path = tmp_dir.path().join("group1");
        let group1_uri = group1_path.to_str().unwrap();
        Group::create(&tdb, group1_uri)?;

        let group1_err =
            Group::open(&tdb, group1_uri.to_owned(), QueryType::Read)?;
        let res = group1_err.put_metadata(
            &tdb,
            Metadata::create("key".to_owned(), Datatype::Int32, vec![5]),
        );
        assert!(res.is_err());

        std::mem::drop(group1_err);
        let group1_write =
            Group::open(&tdb, group1_uri.to_owned(), QueryType::Write)?;
        let res1 = group1_write.put_metadata(
            &tdb,
            Metadata::create("key".to_owned(), Datatype::Any, vec![5]),
        );
        assert!(res1.is_err());

        group1_write.put_metadata(
            &tdb,
            Metadata::create("key".to_owned(), Datatype::Int32, vec![5]),
        )?;
        group1_write.put_metadata(
            &tdb,
            Metadata::create("aaa".to_owned(), Datatype::Int32, vec![5]),
        )?;
        group1_write.put_metadata(
            &tdb,
            Metadata::create(
                "bb".to_owned(),
                Datatype::Float32,
                vec![1.1f32, 2.2f32],
            ),
        )?;

        std::mem::drop(group1_write);

        let group1_read =
            Group::open(&tdb, group1_uri.to_owned(), QueryType::Read)?;
        let metadata_aaa = group1_read.get_metadata(&tdb, "aaa".to_owned())?;
        assert_eq!(metadata_aaa.datatype, Datatype::Int32);
        assert_eq!(metadata_aaa.value, metadata::Value::Int32Value(vec!(5)));
        assert_eq!(metadata_aaa.key, "aaa");

        let metadata_num = group1_read.get_metadata_num(&tdb)?;
        assert_eq!(metadata_num, 3);

        let metadata_bb = group1_read.get_metadata_from_index(&tdb, 1)?;
        assert_eq!(metadata_bb.datatype, Datatype::Float32);
        assert_eq!(metadata_bb.key, "bb");
        assert_eq!(
            metadata_bb.value,
            metadata::Value::Float32Value(vec!(1.1f32, 2.2f32))
        );

        let has_aaa = group1_read.has_metadata_key(&tdb, "aaa")?;
        assert_eq!(has_aaa, Some(Datatype::Int32));
        std::mem::drop(group1_read);

        let group1_write =
            Group::open(&tdb, group1_uri.to_owned(), QueryType::Write)?;
        group1_write.delete_metadata(&tdb, "aaa")?;
        std::mem::drop(group1_write);

        let group1_read =
            Group::open(&tdb, group1_uri.to_owned(), QueryType::Read)?;
        let has_aaa = group1_read.has_metadata_key(&tdb, "aaa")?;
        assert_eq!(has_aaa, None);
        std::mem::drop(group1_read);

        // Cleanup
        let group1_write = Group::open(
            &tdb,
            group1_uri.to_owned(),
            QueryType::ModifyExclusive,
        )?;
        group1_write.delete_group(&tdb, group1_uri, true)?;

        tmp_dir.close().map_err(|e| {
            Error::Other(
                e.to_string())
        })?;
        Ok(())
    }

    fn create_array<S>(
        array_uri: S,
        array_type: ArrayType,
    ) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let tdb = Context::new()?;
    
        // The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
        let domain = {
            let rows: Dimension =
                DimensionBuilder::new::<i32>(
                    &tdb,
                    "rows",
                    Datatype::Int32,
                    &[1, 4],
                    &4,
                )?
                .build();
            let cols: Dimension =
                DimensionBuilder::new::<i32>(
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
        let attribute_a = AttributeBuilder::new(
            &tdb,
            "a",
            Datatype::Int32,
        )?
        .build();
    
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
        let tmp_dir = TempDir::new().map_err(|e| {
            Error::Other(
                e.to_string())
        })?;

        let tdb = Context::new()?;        
        let group_path = tmp_dir.path().join("group2");
        let group_uri = group_path.to_str().unwrap();
        Group::create(&tdb, group_uri)?;

        let group_read = Group::open(&tdb, group_uri.to_owned(), QueryType::Read)?;
        let group_uri_copy = group_read.uri(&tdb)?;
        assert_eq!(group_uri_copy, "file://".to_owned() + group_path.to_str().unwrap());
        let group_type = group_read.query_type(&tdb)?;
        assert_eq!(group_type, QueryType::Read);

        let open = group_read.is_open(&tdb)?;
        assert!(open);

        std::mem::drop(group_read);
        create_array(group_uri.to_owned() + "/aa", ArrayType::Dense)?;
        create_array(group_uri.to_owned() + "/bb", ArrayType::Dense)?;
        create_array(group_uri.to_owned() + "/cc", ArrayType::Sparse)?;

        let group_write = Group::open(&tdb, group_uri.to_owned(), QueryType::Write)?;
        group_write.add_member(&tdb, "aa", true, None as Option<String>)?;
        group_write.add_member(&tdb, "bb", true, None as Option<String>)?;
        group_write.add_member(&tdb, "cc", true, None as Option<String>)?;
        std::mem::drop(group_write);

        let group_read = Group::open(&tdb, group_uri.to_owned(), QueryType::Read)?;
        let opt_string = group_read.dump(&tdb, true)?;
        match opt_string {
            Some(s) => println!("{}", s),
            None => println!("Empty group")
        }
        std::mem::drop(group_read);

        let group_write =  Group::open(&tdb, group_uri.to_owned(), QueryType::Write)?;
        group_write.delete_member(&tdb, group_uri.to_owned() + "/bb")?;
        std::mem::drop(group_write);

        let group_read = Group::open(&tdb, group_uri.to_owned(), QueryType::Read)?;
        let opt_string = group_read.dump(&tdb, true)?;
        match opt_string {
            Some(s) => println!("{}", s),
            None => println!("Empty group")
        }

        //let group_read = Group::open(&tdb, group_uri.to_owned(), QueryType::Read)?;
        //let count = group_read.get_member_count(&tdb)?;
        //assert_eq!(count, 2);

        //let member_aa = group_read.get_member_by_name(&tdb, "aa")?;
        //println!("{:?}", member_aa);

        Ok(())
    }
}
