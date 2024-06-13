use std::convert::TryFrom;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::num::NonZeroU32;
use std::ops::Deref;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use util::option::OptionSubset;

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

pub use attribute::{Attribute, AttributeData, Builder as AttributeBuilder};
pub use dimension::{Builder as DimensionBuilder, Dimension, DimensionData};
pub use domain::{Builder as DomainBuilder, Domain, DomainData};
pub use enumeration::{
    Builder as EnumerationBuilder, Enumeration, EnumerationData,
};
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
        (0..self.schema()?.domain()?.ndim()?)
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

    use crate::array::*;
    use crate::metadata::Value;
    use crate::query::QueryType;
    use crate::Datatype;

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

        let uri = test_uri
            .with_path("quickstart_dense")
            .map_err(|e| Error::Other(e.to_string()))?;
        Array::create(context, &uri, s)?;
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
}
