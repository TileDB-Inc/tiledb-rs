use std::iter::FusedIterator;
use std::ops::Deref;

use anyhow::anyhow;

use crate::array::schema::{RawSchema, Schema};
use crate::config::{Config, RawConfig};
use crate::context::{CApiInterface, Context, ContextBound};
use crate::datatype::{Datatype, LogicalType};
use crate::error::{DatatypeErrorKind, Error};
use crate::fn_typed;
use crate::range::{
    MinimumBoundingRectangle, NonEmptyDomain, Range, TypedRange,
};
use crate::string::{RawTDBString, TDBString};
use crate::Result as TileDBResult;

pub use crate::array::schema::ArrayType as FragmentType;

pub(crate) enum RawFragmentInfo {
    Owned(*mut ffi::tiledb_fragment_info_t),
}

impl Deref for RawFragmentInfo {
    type Target = *mut ffi::tiledb_fragment_info_t;
    fn deref(&self) -> &Self::Target {
        let RawFragmentInfo::Owned(ref ffi) = *self;
        ffi
    }
}

impl Drop for RawFragmentInfo {
    fn drop(&mut self) {
        let RawFragmentInfo::Owned(ref mut ffi) = *self;
        unsafe {
            ffi::tiledb_fragment_info_free(ffi);
        }
    }
}

struct FragmentInfoInternal {
    context: Context,
    raw: RawFragmentInfo,
}

// impl<'ctx> ContextBoundBase<'ctx> for FragmentInfoInternal<'ctx> {}

impl ContextBound for FragmentInfoInternal {
    fn context(&self) -> Context {
        self.context.clone()
    }
}

impl FragmentInfoInternal {
    pub(crate) fn capi(&self) -> *mut ffi::tiledb_fragment_info_t {
        *self.raw
    }

    pub fn config(&self) -> TileDBResult<Config> {
        let c_frag = self.capi();
        let mut c_config: *mut ffi::tiledb_config_t = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_fragment_info_get_config(ctx, c_frag, &mut c_config)
        })?;

        Ok(Config::from_raw(RawConfig::Owned(c_config)))
    }

    pub fn load(&self) -> TileDBResult<()> {
        let c_frag = self.capi();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_fragment_info_load(ctx, c_frag)
        })?;

        Ok(())
    }

    pub fn unconsolidated_metadata_num(&self) -> TileDBResult<u32> {
        let c_frag = self.capi();
        let mut result: u32 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_fragment_info_get_unconsolidated_metadata_num(
                ctx,
                c_frag,
                &mut result,
            )
        })?;

        Ok(result)
    }

    pub fn num_to_vacuum(&self) -> TileDBResult<u32> {
        let c_frag = self.capi();
        let mut result: u32 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_fragment_info_get_to_vacuum_num(
                ctx,
                c_frag,
                &mut result,
            )
        })?;

        Ok(result)
    }

    pub fn total_cell_count(&self) -> TileDBResult<u64> {
        let c_frag = self.capi();
        let mut count: u64 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_fragment_info_get_total_cell_num(
                ctx, c_frag, &mut count,
            )
        })?;

        Ok(count)
    }

    pub fn num_fragments(&self) -> TileDBResult<u32> {
        let c_frag = self.capi();
        let mut ret: u32 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_fragment_info_get_fragment_num(ctx, c_frag, &mut ret)
        })?;

        Ok(ret)
    }

    pub fn num_cells(&self, frag_idx: u32) -> TileDBResult<u64> {
        let c_frag = self.capi();
        let mut cells: u64 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_fragment_info_get_cell_num(
                ctx, c_frag, frag_idx, &mut cells,
            )
        })?;

        Ok(cells)
    }

    pub fn version(&self, frag_idx: u32) -> TileDBResult<u32> {
        let c_frag = self.capi();
        let mut version: u32 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_fragment_info_get_version(
                ctx,
                c_frag,
                frag_idx,
                &mut version,
            )
        })?;

        Ok(version)
    }

    pub fn schema(&self, frag_idx: u32) -> TileDBResult<Schema> {
        let c_frag = self.capi();
        let mut c_schema: *mut ffi::tiledb_array_schema_t = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_fragment_info_get_array_schema(
                ctx,
                c_frag,
                frag_idx,
                &mut c_schema,
            )
        })?;

        Ok(Schema::new(&self.context, RawSchema::Owned(c_schema)))
    }

    pub fn schema_name(&self, frag_idx: u32) -> TileDBResult<String> {
        let c_frag = self.capi();
        let mut c_str: *const std::ffi::c_char = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_fragment_info_get_array_schema_name(
                ctx, c_frag, frag_idx, &mut c_str,
            )
        })?;

        // N.B. This API only lends a pointer to an internally managed
        // std::string, thus we do *not* want to free it.
        let name = unsafe { std::ffi::CStr::from_ptr(c_str) };
        Ok(String::from(name.to_string_lossy()))
    }

    pub fn fragment_name(&self, frag_idx: u32) -> TileDBResult<String> {
        let c_frag = self.capi();
        let mut c_str: *mut ffi::tiledb_string_t = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_fragment_info_get_fragment_name_v2(
                ctx, c_frag, frag_idx, &mut c_str,
            )
        })?;

        let str = TDBString::from_raw(RawTDBString::Owned(c_str));
        str.to_string()
    }

    pub fn fragment_uri(&self, frag_idx: u32) -> TileDBResult<String> {
        let c_frag = self.capi();
        let mut c_str: *const std::ffi::c_char = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_fragment_info_get_fragment_uri(
                ctx, c_frag, frag_idx, &mut c_str,
            )
        })?;

        // N.B. This API only lends a pointer to an internally managed
        // std::string, thus we do *not* want to free it.
        let name = unsafe { std::ffi::CStr::from_ptr(c_str) };
        Ok(String::from(name.to_string_lossy()))
    }

    pub fn fragment_size(&self, frag_idx: u32) -> TileDBResult<u64> {
        let c_frag = self.capi();
        let mut size: u64 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_fragment_info_get_fragment_size(
                ctx, c_frag, frag_idx, &mut size,
            )
        })?;

        Ok(size)
    }

    pub fn fragment_type(&self, frag_idx: u32) -> TileDBResult<FragmentType> {
        let c_frag = self.capi();
        let mut dense: i32 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_fragment_info_get_dense(
                ctx, c_frag, frag_idx, &mut dense,
            )
        })?;

        if dense == 1 {
            Ok(FragmentType::Dense)
        } else {
            Ok(FragmentType::Sparse)
        }
    }

    pub fn timestamp_range(&self, frag_idx: u32) -> TileDBResult<[u64; 2]> {
        let c_frag = self.capi();
        let mut start: u64 = 0;
        let mut end: u64 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_fragment_info_get_timestamp_range(
                ctx, c_frag, frag_idx, &mut start, &mut end,
            )
        })?;

        Ok([start, end])
    }

    pub fn has_consolidated_metadata(
        &self,
        frag_idx: u32,
    ) -> TileDBResult<bool> {
        let c_frag = self.capi();
        let mut result: i32 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_fragment_info_has_consolidated_metadata(
                ctx,
                c_frag,
                frag_idx,
                &mut result,
            )
        })?;

        Ok(result != 0)
    }

    pub fn to_vacuum_uri(&self, frag_idx: u32) -> TileDBResult<String> {
        let c_frag = self.capi();
        let mut c_str: *const std::ffi::c_char = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_fragment_info_get_to_vacuum_uri(
                ctx, c_frag, frag_idx, &mut c_str,
            )
        })?;

        // N.B. This API only lends a pointer to an internally managed
        // std::string, thus we do *not* want to free it.
        let name = unsafe { std::ffi::CStr::from_ptr(c_str) };
        Ok(String::from(name.to_string_lossy()))
    }

    pub fn non_empty_domain(
        &self,
        frag_idx: u32,
        dim_idx: u32,
    ) -> TileDBResult<TypedRange> {
        let dim = self.schema(frag_idx)?.domain()?.dimension(dim_idx)?;
        let dtype = dim.datatype()?;
        if dim.is_var_sized()? {
            self.var_sized_non_empty_domain(dtype, frag_idx, dim_idx)
        } else {
            self.fixed_size_non_empty_domain(dtype, frag_idx, dim_idx)
        }
    }

    fn fixed_size_non_empty_domain(
        &self,
        datatype: Datatype,
        frag_idx: u32,
        dim_idx: u32,
    ) -> TileDBResult<TypedRange> {
        fn_typed!(datatype, LT, {
            type DT = <LT as LogicalType>::PhysicalType;
            let c_frag = self.capi();
            let mut range = [DT::default(), DT::default()];
            let c_range = range.as_mut_ptr();
            self.capi_call(|ctx| unsafe {
                ffi::tiledb_fragment_info_get_non_empty_domain_from_index(
                    ctx,
                    c_frag,
                    frag_idx,
                    dim_idx,
                    c_range as *mut std::ffi::c_void,
                )
            })?;

            let range = Range::from(&range);

            Ok(TypedRange::new(datatype, range))
        })
    }

    fn var_sized_non_empty_domain(
        &self,
        datatype: Datatype,
        frag_idx: u32,
        dim_idx: u32,
    ) -> TileDBResult<TypedRange> {
        let c_frag = self.capi();
        let mut start_size: u64 = 0;
        let mut end_size: u64 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_fragment_info_get_non_empty_domain_var_size_from_index(
                ctx,
                c_frag,
                frag_idx,
                dim_idx,
                &mut start_size,
                &mut end_size,
            )
        })?;

        fn_typed!(datatype, LT, {
            type DT = <LT as LogicalType>::PhysicalType;
            if start_size % std::mem::size_of::<DT>() as u64 != 0 {
                return Err(Error::Datatype(DatatypeErrorKind::TypeMismatch {
                    user_type: std::any::type_name::<DT>().to_owned(),
                    tiledb_type: datatype,
                }));
            }

            if end_size % std::mem::size_of::<DT>() as u64 != 0 {
                return Err(Error::Datatype(DatatypeErrorKind::TypeMismatch {
                    user_type: std::any::type_name::<DT>().to_owned(),
                    tiledb_type: datatype,
                }));
            }

            let start_elems = start_size / std::mem::size_of::<DT>() as u64;
            let end_elems = end_size / std::mem::size_of::<DT>() as u64;

            let mut start: Box<[DT]> =
                vec![Default::default(); start_elems as usize]
                    .into_boxed_slice();
            let mut end: Box<[DT]> =
                vec![Default::default(); end_elems as usize].into_boxed_slice();

            self.capi_call(|ctx| unsafe {
                ffi::tiledb_fragment_info_get_non_empty_domain_var_from_index(
                    ctx,
                    c_frag,
                    frag_idx,
                    dim_idx,
                    start.as_mut_ptr() as *mut std::ffi::c_void,
                    end.as_mut_ptr() as *mut std::ffi::c_void,
                )
            })?;

            Ok(TypedRange::new(datatype, Range::from((start, end))))
        })
    }

    pub fn num_mbrs(&self, frag_idx: u32) -> TileDBResult<u64> {
        let c_frag = self.capi();
        let mut num: u64 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_fragment_info_get_mbr_num(
                ctx, c_frag, frag_idx, &mut num,
            )
        })?;

        Ok(num)
    }

    pub fn mbr(
        &self,
        frag_idx: u32,
        mbr_idx: u32,
        dim_idx: u32,
    ) -> TileDBResult<TypedRange> {
        let dim = self.schema(frag_idx)?.domain()?.dimension(dim_idx)?;
        let dtype = dim.datatype()?;
        if !dim.is_var_sized()? {
            self.fixed_size_mbr(dtype, frag_idx, mbr_idx, dim_idx)
        } else {
            self.var_size_mbr(dtype, frag_idx, mbr_idx, dim_idx)
        }
    }

    fn fixed_size_mbr(
        &self,
        datatype: Datatype,
        frag_idx: u32,
        mbr_idx: u32,
        dim_idx: u32,
    ) -> TileDBResult<TypedRange> {
        let c_frag = self.capi();
        fn_typed!(datatype, LT, {
            type DT = <LT as LogicalType>::PhysicalType;
            let mut range = [DT::default(), DT::default()];
            let c_range = range.as_mut_ptr();
            self.capi_call(|ctx| unsafe {
                ffi::tiledb_fragment_info_get_mbr_from_index(
                    ctx,
                    c_frag,
                    frag_idx,
                    mbr_idx,
                    dim_idx,
                    c_range as *mut std::ffi::c_void,
                )
            })?;

            Ok(TypedRange {
                datatype,
                range: Range::from(&range),
            })
        })
    }

    fn var_size_mbr(
        &self,
        datatype: Datatype,
        frag_idx: u32,
        mbr_idx: u32,
        dim_idx: u32,
    ) -> TileDBResult<TypedRange> {
        let c_frag = self.capi();
        let mut start_size: u64 = 0;
        let mut end_size: u64 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_fragment_info_get_mbr_var_size_from_index(
                ctx,
                c_frag,
                frag_idx,
                mbr_idx,
                dim_idx,
                &mut start_size,
                &mut end_size,
            )
        })?;

        fn_typed!(datatype, LT, {
            type DT = <LT as LogicalType>::PhysicalType;
            if start_size % std::mem::size_of::<DT>() as u64 != 0 {
                return Err(Error::Datatype(DatatypeErrorKind::TypeMismatch {
                    user_type: std::any::type_name::<DT>().to_owned(),
                    tiledb_type: datatype,
                }));
            }

            if end_size % std::mem::size_of::<DT>() as u64 != 0 {
                return Err(Error::Datatype(DatatypeErrorKind::TypeMismatch {
                    user_type: std::any::type_name::<DT>().to_owned(),
                    tiledb_type: datatype,
                }));
            }

            let start_elems = start_size / std::mem::size_of::<DT>() as u64;
            let end_elems = end_size / std::mem::size_of::<DT>() as u64;

            let mut start: Box<[DT]> =
                vec![Default::default(); start_elems as usize]
                    .into_boxed_slice();
            let mut end: Box<[DT]> =
                vec![Default::default(); end_elems as usize].into_boxed_slice();

            self.capi_call(|ctx| unsafe {
                ffi::tiledb_fragment_info_get_mbr_var_from_index(
                    ctx,
                    c_frag,
                    frag_idx,
                    mbr_idx,
                    dim_idx,
                    start.as_mut_ptr() as *mut std::ffi::c_void,
                    end.as_mut_ptr() as *mut std::ffi::c_void,
                )
            })?;

            Ok(TypedRange {
                datatype,
                range: Range::from((start, end)),
            })
        })
    }
}

pub struct FragmentInfo<'info> {
    info: &'info FragmentInfoInternal,
    index: u32,
}

impl<'info> FragmentInfo<'info> {
    pub fn name(&self) -> TileDBResult<String> {
        self.info.fragment_name(self.index)
    }

    pub fn uri(&self) -> TileDBResult<String> {
        self.info.fragment_uri(self.index)
    }

    pub fn size(&self) -> TileDBResult<u64> {
        self.info.fragment_size(self.index)
    }

    pub fn fragment_type(&self) -> TileDBResult<FragmentType> {
        self.info.fragment_type(self.index)
    }

    pub fn num_cells(&self) -> TileDBResult<u64> {
        self.info.num_cells(self.index)
    }

    pub fn version(&self) -> TileDBResult<u32> {
        self.info.version(self.index)
    }

    pub fn schema_name(&self) -> TileDBResult<String> {
        self.info.schema_name(self.index)
    }

    pub fn schema(&self) -> TileDBResult<Schema> {
        self.info.schema(self.index)
    }

    pub fn non_empty_domain(&self) -> TileDBResult<NonEmptyDomain> {
        let schema = self.info.schema(self.index)?;
        let num_dims = schema.domain()?.ndim()? as u32;

        let mut ret = Vec::<TypedRange>::new();
        for dimension in 0..num_dims {
            let range = self.info.non_empty_domain(self.index, dimension)?;
            ret.push(range)
        }

        Ok(ret)
    }

    pub fn num_mbrs(&self) -> TileDBResult<u64> {
        self.info.num_mbrs(self.index)
    }

    pub fn mbr(&self, mbr_idx: u32) -> TileDBResult<MinimumBoundingRectangle> {
        let schema = self.info.schema(self.index)?;
        let num_dims = schema.domain()?.ndim()? as u32;

        let mut ret = Vec::<TypedRange>::new();
        for dimension in 0..num_dims {
            let range = self.info.mbr(self.index, mbr_idx, dimension)?;
            ret.push(range)
        }

        Ok(ret)
    }

    pub fn timestamp_range(&self) -> TileDBResult<[u64; 2]> {
        self.info.timestamp_range(self.index)
    }

    pub fn has_consolidated_metadata(&self) -> TileDBResult<bool> {
        self.info.has_consolidated_metadata(self.index)
    }

    pub fn to_vacuum_uri(&self) -> TileDBResult<String> {
        self.info.to_vacuum_uri(self.index)
    }
}

pub struct FragmentInfoList {
    info: FragmentInfoInternal,
}

impl ContextBound for FragmentInfoList {
    fn context(&self) -> Context {
        self.info.context()
    }
}

impl FragmentInfoList {
    pub fn config(&self) -> TileDBResult<Config> {
        self.info.config()
    }

    pub fn load(&self) -> TileDBResult<()> {
        self.info.load()
    }

    pub fn unconsolidated_metadata_num(&self) -> TileDBResult<u32> {
        self.info.unconsolidated_metadata_num()
    }

    pub fn num_to_vacuum(&self) -> TileDBResult<u32> {
        self.info.num_to_vacuum()
    }

    pub fn total_cell_count(&self) -> TileDBResult<u64> {
        self.info.total_cell_count()
    }

    pub fn num_fragments(&self) -> TileDBResult<u32> {
        self.info.num_fragments()
    }

    pub fn get_fragment(&self, index: u32) -> TileDBResult<FragmentInfo> {
        if index >= self.num_fragments()? {
            return Err(Error::InvalidIndex(index as usize));
        }

        Ok(FragmentInfo {
            info: &self.info,
            index,
        })
    }

    pub fn iter(&self) -> TileDBResult<FragmentInfoListIterator> {
        FragmentInfoListIterator::try_from(self)
    }
}

pub struct FragmentInfoListIterator<'info> {
    info: &'info FragmentInfoList,
    num_fragments: u32,
    index: u32,
}

impl<'data> ContextBound for FragmentInfoListIterator<'data> {
    fn context(&self) -> Context {
        self.info.context()
    }
}

impl<'info> Iterator for FragmentInfoListIterator<'info> {
    type Item = FragmentInfo<'info>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.num_fragments {
            return None;
        }

        let ret = self
            .info
            .get_fragment(self.index)
            .expect("Error getting fragment.");
        self.index += 1;

        Some(ret)
    }
}

impl<'info> FusedIterator for FragmentInfoListIterator<'info> {}

impl<'info> TryFrom<&'info FragmentInfoList>
    for FragmentInfoListIterator<'info>
{
    type Error = crate::error::Error;

    fn try_from(info: &'info FragmentInfoList) -> TileDBResult<Self> {
        Ok(FragmentInfoListIterator {
            info,
            num_fragments: info.num_fragments()?,
            index: 0u32,
        })
    }
}

pub struct Builder {
    info: FragmentInfoInternal,
}

impl ContextBound for Builder {
    fn context(&self) -> Context {
        self.info.context()
    }
}

impl Builder {
    pub fn new<T>(context: &Context, uri: T) -> TileDBResult<Self>
    where
        T: AsRef<str>,
    {
        let c_uri = std::ffi::CString::new(uri.as_ref())
            .map_err(|e| Error::InvalidArgument(anyhow!(e)))?;
        let mut c_frag_info: *mut ffi::tiledb_fragment_info_t = out_ptr!();
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_fragment_info_alloc(
                ctx,
                c_uri.as_c_str().as_ptr(),
                &mut c_frag_info,
            )
        })?;

        Ok(Builder {
            info: FragmentInfoInternal {
                context: context.clone(),
                raw: RawFragmentInfo::Owned(c_frag_info),
            },
        })
    }

    pub fn config(self, config: &Config) -> TileDBResult<Self> {
        let c_frag = self.info.capi();
        let c_cfg = config.capi();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_fragment_info_set_config(ctx, c_frag, c_cfg)
        })?;
        Ok(self)
    }

    pub fn build(self) -> TileDBResult<FragmentInfoList> {
        let ret = FragmentInfoList { info: self.info };
        ret.load()?;
        Ok(ret)
    }

    pub fn build_without_loading(self) -> FragmentInfoList {
        FragmentInfoList { info: self.info }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use tempfile::TempDir;

    use crate::array::*;
    use crate::config::Config;
    use crate::query::{QueryBuilder, WriteBuilder};
    use crate::Datatype;

    #[test]
    fn test_set_config() -> TileDBResult<()> {
        let ctx = Context::new().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let array_uri = create_dense_array(&ctx, &tmp_dir).unwrap();

        let config = Config::new()?;
        let frag_infos =
            Builder::new(&ctx, array_uri)?.config(&config)?.build();

        assert!(frag_infos.is_ok());

        Ok(())
    }

    #[test]
    fn test_get_config() -> TileDBResult<()> {
        let ctx = Context::new().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let array_uri = create_dense_array(&ctx, &tmp_dir).unwrap();
        let frag_infos = Builder::new(&ctx, array_uri)?.build()?;

        assert!(frag_infos.config().is_ok());

        Ok(())
    }

    #[test]
    fn test_load_infos() -> TileDBResult<()> {
        let ctx = Context::new().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let array_uri = create_dense_array(&ctx, &tmp_dir).unwrap();
        let frag_infos = Builder::new(&ctx, array_uri)?.build_without_loading();

        assert!(frag_infos.load().is_ok());

        Ok(())
    }

    #[test]
    fn test_unconsolidated_metadata_num() -> TileDBResult<()> {
        let ctx = Context::new().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let array_uri = create_dense_array(&ctx, &tmp_dir).unwrap();
        let frag_infos = Builder::new(&ctx, array_uri)?.build()?;

        assert!(frag_infos.unconsolidated_metadata_num().is_ok());

        Ok(())
    }

    #[test]
    fn test_num_to_vacuum() -> TileDBResult<()> {
        let ctx = Context::new().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let array_uri = create_dense_array(&ctx, &tmp_dir).unwrap();
        let frag_infos = Builder::new(&ctx, array_uri)?.build()?;

        assert!(frag_infos.num_to_vacuum().is_ok());

        Ok(())
    }

    #[test]
    fn test_total_cell_count() -> TileDBResult<()> {
        let ctx = Context::new().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let array_uri = create_dense_array(&ctx, &tmp_dir).unwrap();
        let frag_infos = Builder::new(&ctx, array_uri)?.build()?;

        let cell_count = frag_infos.total_cell_count()?;
        assert!(cell_count > 0);

        Ok(())
    }

    #[test]
    fn test_num_fragments() -> TileDBResult<()> {
        let ctx = Context::new().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let array_uri = create_dense_array(&ctx, &tmp_dir).unwrap();
        let frag_infos = Builder::new(&ctx, array_uri)?.build()?;

        let num_frags = frag_infos.num_fragments()?;
        assert_eq!(num_frags, 2);

        Ok(())
    }

    #[test]
    fn test_get_fragment() -> TileDBResult<()> {
        let ctx = Context::new().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let array_uri = create_dense_array(&ctx, &tmp_dir).unwrap();
        let frag_infos = Builder::new(&ctx, array_uri)?.build()?;

        assert!(frag_infos.get_fragment(0).is_ok());

        Ok(())
    }

    #[test]
    fn test_get_fragment_failure() -> TileDBResult<()> {
        let ctx = Context::new().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let array_uri = create_dense_array(&ctx, &tmp_dir).unwrap();
        let frag_infos = Builder::new(&ctx, array_uri)?.build()?;

        assert!(frag_infos.get_fragment(3).is_err());

        Ok(())
    }

    #[test]
    fn test_iter_fragments() -> TileDBResult<()> {
        let ctx = Context::new().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let array_uri = create_dense_array(&ctx, &tmp_dir).unwrap();
        let frag_infos = Builder::new(&ctx, array_uri)?.build()?;

        let mut num_frags = 0;
        for _ in frag_infos.iter()? {
            num_frags += 1;
        }

        assert_eq!(num_frags, 2);

        Ok(())
    }

    #[test]
    fn test_fragment_info_apis() -> TileDBResult<()> {
        let ctx = Context::new().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let dense_array_uri = create_dense_array(&ctx, &tmp_dir).unwrap();
        let sparse_array_uri = create_sparse_array(&ctx, &tmp_dir).unwrap();

        check_fragment_info_apis(&ctx, &dense_array_uri, FragmentType::Dense)?;
        check_fragment_info_apis(
            &ctx,
            &sparse_array_uri,
            FragmentType::Sparse,
        )?;

        Ok(())
    }

    fn check_fragment_info_apis(
        ctx: &Context,
        array_uri: &str,
        ftype: FragmentType,
    ) -> TileDBResult<()> {
        let frag_infos = Builder::new(ctx, array_uri)?.build()?;
        for frag in frag_infos.iter()? {
            assert!(frag.name().is_ok());
            assert!(frag.uri().is_ok());
            assert!(frag.size().is_ok());
            assert_eq!(frag.fragment_type()?, ftype);
            assert!(frag.schema().is_ok());
            assert!(frag.schema_name().is_ok());
            assert!(frag.non_empty_domain().is_ok());
            for i in 0..(frag.num_mbrs()? as u32) {
                assert!(frag.mbr(i).is_ok());
            }
            assert!(matches!(frag.timestamp_range()?, [_, _]));
            assert!(frag.has_consolidated_metadata().is_ok());
            assert!(frag.to_vacuum_uri().is_err());
            assert!(frag.num_cells()? > 0);
            assert!(frag.version()? > 0);
        }

        Ok(())
    }

    /// Create a simple dense test array with a couple fragments to inspect.
    pub fn create_dense_array(
        ctx: &Context,
        dir: &TempDir,
    ) -> TileDBResult<String> {
        let array_dir = dir.path().join("fragment_info_test_dense");
        let array_uri = String::from(array_dir.to_str().unwrap());

        let domain = {
            let rows = DimensionBuilder::new(
                ctx,
                "id",
                Datatype::Int32,
                ([1, 10], 4),
            )?
            .build();

            DomainBuilder::new(ctx)?.add_dimension(rows)?.build()
        };

        let schema = SchemaBuilder::new(ctx, ArrayType::Dense, domain)?
            .add_attribute(
                AttributeBuilder::new(ctx, "attr", Datatype::UInt64)?.build(),
            )?
            .build()?;

        Array::create(ctx, &array_uri, schema)?;

        // Two writes for multiple fragments
        write_dense_array(ctx, &array_uri)?;
        write_dense_array(ctx, &array_uri)?;
        Ok(array_uri)
    }

    /// Write another fragment to the test array.
    fn write_dense_array(ctx: &Context, array_uri: &str) -> TileDBResult<()> {
        let data = vec![1u64, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let array = Array::open(ctx, array_uri, Mode::Write)?;
        let query =
            WriteBuilder::new(array)?.data_typed("attr", &data)?.build();
        query.submit()?;
        Ok(())
    }

    /// Create a simple sparse test array with a couple fragments to inspect.
    pub fn create_sparse_array(
        ctx: &Context,
        dir: &TempDir,
    ) -> TileDBResult<String> {
        let array_dir = dir.path().join("fragment_info_test_sparse");
        let array_uri = String::from(array_dir.to_str().unwrap());

        let domain = {
            let rows = DimensionBuilder::new(
                ctx,
                "id",
                Datatype::Int32,
                ([1, 10], 4),
            )?
            .build();

            DomainBuilder::new(ctx)?.add_dimension(rows)?.build()
        };

        let schema = SchemaBuilder::new(ctx, ArrayType::Sparse, domain)?
            .add_attribute(
                AttributeBuilder::new(ctx, "attr", Datatype::UInt64)?.build(),
            )?
            .build()?;

        Array::create(ctx, &array_uri, schema)?;

        // Two writes for multiple fragments
        write_sparse_array(ctx, &array_uri)?;
        write_sparse_array(ctx, &array_uri)?;
        Ok(array_uri)
    }

    /// Write another fragment to the test array.
    fn write_sparse_array(ctx: &Context, array_uri: &str) -> TileDBResult<()> {
        let id_data = vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let attr_data = vec![1u64, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let array = Array::open(ctx, array_uri, Mode::Write)?;
        let query = WriteBuilder::new(array)?
            .data_typed("id", &id_data)?
            .data_typed("attr", &attr_data)?
            .build();
        query.submit()?;
        Ok(())
    }
}
