extern crate tiledb_sys as ffi;

use std::borrow::Borrow;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::num::NonZeroU32;
use std::ops::Deref;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use serde_json::json;
use util::option::OptionSubset;

use crate::array::CellValNum;
use crate::context::{CApiInterface, Context, ContextBound};
use crate::datatype::physical::BitsEq;
use crate::datatype::{LogicalType, PhysicalType};
use crate::error::{DatatypeErrorKind, Error};
use crate::filter::list::{FilterList, FilterListData, RawFilterList};
use crate::fn_typed;
use crate::{Datatype, Factory, Result as TileDBResult};

pub(crate) enum RawAttribute {
    Owned(*mut ffi::tiledb_attribute_t),
}

impl Deref for RawAttribute {
    type Target = *mut ffi::tiledb_attribute_t;
    fn deref(&self) -> &Self::Target {
        let RawAttribute::Owned(ref ffi) = *self;
        ffi
    }
}

impl Drop for RawAttribute {
    fn drop(&mut self) {
        let RawAttribute::Owned(ref mut ffi) = *self;
        unsafe {
            ffi::tiledb_attribute_free(ffi);
        }
    }
}

pub struct Attribute {
    context: Context,
    raw: RawAttribute,
}

impl ContextBound for Attribute {
    fn context(&self) -> Context {
        self.context.clone()
    }
}

impl Attribute {
    pub(crate) fn capi(&self) -> *mut ffi::tiledb_attribute_t {
        *self.raw
    }

    pub(crate) fn new(context: &Context, raw: RawAttribute) -> Self {
        Attribute {
            context: context.clone(),
            raw,
        }
    }

    pub fn name(&self) -> TileDBResult<String> {
        let mut c_name = std::ptr::null::<std::ffi::c_char>();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_attribute_get_name(ctx, *self.raw, &mut c_name)
        })?;
        let c_name = unsafe { std::ffi::CStr::from_ptr(c_name) };
        Ok(String::from(c_name.to_string_lossy()))
    }

    pub fn datatype(&self) -> TileDBResult<Datatype> {
        let mut c_dtype: std::ffi::c_uint = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_attribute_get_type(ctx, *self.raw, &mut c_dtype)
        })?;
        Datatype::try_from(c_dtype)
    }

    pub fn is_nullable(&self) -> TileDBResult<bool> {
        let mut c_nullable: std::ffi::c_uchar = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_attribute_get_nullable(ctx, *self.raw, &mut c_nullable)
        })?;

        Ok(c_nullable == 1)
    }

    pub fn filter_list(&self) -> TileDBResult<FilterList> {
        let mut c_flist: *mut ffi::tiledb_filter_list_t = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_attribute_get_filter_list(ctx, *self.raw, &mut c_flist)
        })?;
        Ok(FilterList {
            context: self.context.clone(),
            raw: RawFilterList::Owned(c_flist),
        })
    }

    pub fn cell_val_num(&self) -> TileDBResult<CellValNum> {
        let mut c_num: std::ffi::c_uint = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_attribute_get_cell_val_num(ctx, *self.raw, &mut c_num)
        })?;
        CellValNum::try_from(c_num)
    }

    pub fn is_var_sized(&self) -> TileDBResult<bool> {
        Ok(self.cell_val_num()?.is_var_sized())
    }

    pub fn cell_size(&self) -> TileDBResult<u64> {
        let mut c_size: std::ffi::c_ulonglong = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_attribute_get_cell_size(ctx, *self.raw, &mut c_size)
        })?;
        Ok(c_size as u64)
    }

    pub fn fill_value<'a, F: FromFillValue<'a>>(&'a self) -> TileDBResult<F> {
        let c_attr = *self.raw;
        let mut c_ptr: *const std::ffi::c_void = out_ptr!();
        let mut c_size: u64 = 0;

        if !self.datatype()?.is_compatible_type::<F::PhysicalType>() {
            return Err(Error::Datatype(DatatypeErrorKind::TypeMismatch {
                user_type: std::any::type_name::<F::PhysicalType>().to_owned(),
                tiledb_type: self.datatype()?,
            }));
        }

        self.capi_call(|ctx| unsafe {
            ffi::tiledb_attribute_get_fill_value(
                ctx,
                c_attr,
                &mut c_ptr,
                &mut c_size,
            )
        })?;

        assert!(c_size % std::mem::size_of::<F::PhysicalType>() as u64 == 0,
            "Unexpected fill value size for compatible type {}: expected multiple of {}, found {}",
            std::any::type_name::<F::PhysicalType>(),
            std::mem::size_of::<F::PhysicalType>(), c_size);

        let len = c_size as usize / std::mem::size_of::<F::PhysicalType>();
        let slice: &[F::PhysicalType] = unsafe {
            std::slice::from_raw_parts(c_ptr as *const F::PhysicalType, len)
        };
        F::from_raw(slice)
    }

    pub fn fill_value_nullable<'a, F: FromFillValue<'a>>(
        &'a self,
    ) -> TileDBResult<(F, bool)> {
        if !self.datatype()?.is_compatible_type::<F::PhysicalType>() {
            return Err(Error::Datatype(DatatypeErrorKind::TypeMismatch {
                user_type: std::any::type_name::<F::PhysicalType>().to_owned(),
                tiledb_type: self.datatype()?,
            }));
        }
        if !self.is_nullable()? {
            /* see comment in Builder::fill_value_nullability */
            return Ok((self.fill_value()?, false));
        }

        let c_attr = *self.raw;
        let mut c_ptr: *const std::ffi::c_void = out_ptr!();
        let mut c_size: u64 = 0;
        let mut c_validity: u8 = 0;

        self.capi_call(|ctx| unsafe {
            ffi::tiledb_attribute_get_fill_value_nullable(
                ctx,
                c_attr,
                &mut c_ptr,
                &mut c_size,
                &mut c_validity,
            )
        })?;

        assert!(c_size % std::mem::size_of::<F::PhysicalType>() as u64 == 0,
            "Unexpected fill value size for compatible type {}: expected multiple of {}, found {}",
            std::any::type_name::<F::PhysicalType>(),
            std::mem::size_of::<F::PhysicalType>(), c_size);

        let len = c_size as usize / std::mem::size_of::<F::PhysicalType>();
        let slice: &[F::PhysicalType] = unsafe {
            std::slice::from_raw_parts(c_ptr as *const F::PhysicalType, len)
        };

        let is_valid = c_validity != 0;
        Ok((F::from_raw(slice)?, is_valid))
    }
}

impl Debug for Attribute {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let data =
            AttributeData::try_from(self).map_err(|_| std::fmt::Error)?;
        let mut json = json!(data);
        json["raw"] = json!(format!("{:p}", *self.raw));

        write!(f, "{}", json)
    }
}

impl PartialEq<Attribute> for Attribute {
    fn eq(&self, other: &Attribute) -> bool {
        let names_match = match (self.name(), other.name()) {
            (Ok(mine), Ok(theirs)) => mine == theirs,
            _ => false,
        };
        if !names_match {
            return false;
        }

        let types_match = match (self.datatype(), other.datatype()) {
            (Ok(mine), Ok(theirs)) => mine == theirs,
            _ => false,
        };
        if !types_match {
            return false;
        }

        let nullable_match = match (self.is_nullable(), other.is_nullable()) {
            (Ok(mine), Ok(theirs)) => mine == theirs,
            _ => false,
        };
        if !nullable_match {
            return false;
        }

        let filter_match = match (self.filter_list(), other.filter_list()) {
            (Ok(mine), Ok(theirs)) => mine == theirs,
            _ => false,
        };
        if !filter_match {
            return false;
        }

        let cell_val_match = match (self.cell_val_num(), other.cell_val_num()) {
            (Ok(mine), Ok(theirs)) => mine == theirs,
            _ => false,
        };
        if !cell_val_match {
            return false;
        }

        let fill_value_match = if self.is_nullable().unwrap() {
            fn_typed!(self.datatype().unwrap(), LT, {
                type DT = <LT as LogicalType>::PhysicalType;
                match (
                    self.fill_value_nullable::<&[DT]>(),
                    other.fill_value_nullable::<&[DT]>(),
                ) {
                    (
                        Ok((mine_value, mine_nullable)),
                        Ok((theirs_value, theirs_nullable)),
                    ) => {
                        mine_value.bits_eq(theirs_value)
                            && mine_nullable == theirs_nullable
                    }
                    _ => false,
                }
            })
        } else {
            fn_typed!(self.datatype().unwrap(), LT, {
                type DT = <LT as LogicalType>::PhysicalType;
                match (self.fill_value::<&[DT]>(), other.fill_value::<&[DT]>())
                {
                    (Ok(mine), Ok(theirs)) => mine.bits_eq(theirs),
                    _ => false,
                }
            })
        };
        if !fill_value_match {
            return false;
        }

        true
    }
}

pub struct Builder {
    attr: Attribute,
}

impl ContextBound for Builder {
    fn context(&self) -> Context {
        self.attr.context()
    }
}

impl Builder {
    pub fn new(
        context: &Context,
        name: &str,
        datatype: Datatype,
    ) -> TileDBResult<Self> {
        let mut c_attr: *mut ffi::tiledb_attribute_t = out_ptr!();
        let c_name = cstring!(name);
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_attribute_alloc(
                ctx,
                c_name.as_c_str().as_ptr(),
                datatype as u32,
                &mut c_attr,
            )
        })?;
        Ok(Builder {
            attr: Attribute {
                context: context.clone(),
                raw: RawAttribute::Owned(c_attr),
            },
        })
    }

    pub fn datatype(&self) -> TileDBResult<Datatype> {
        self.attr.datatype()
    }

    pub fn cell_val_num(self, num: CellValNum) -> TileDBResult<Self> {
        let c_num = num.capi() as std::ffi::c_uint;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_attribute_set_cell_val_num(ctx, *self.attr.raw, c_num)
        })?;
        Ok(self)
    }

    pub fn var_sized(self) -> TileDBResult<Self> {
        self.cell_val_num(CellValNum::Var)
    }

    pub fn is_nullable(&self) -> TileDBResult<bool> {
        self.attr.is_nullable()
    }

    pub fn nullability(self, nullable: bool) -> TileDBResult<Self> {
        let c_nullable: u8 = if nullable { 1 } else { 0 };
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_attribute_set_nullable(ctx, *self.attr.raw, c_nullable)
        })?;
        Ok(self)
    }

    pub fn fill_value<F: IntoFillValue>(self, value: F) -> TileDBResult<Self> {
        if !self
            .attr
            .datatype()?
            .is_compatible_type::<F::PhysicalType>()
        {
            return Err(Error::Datatype(DatatypeErrorKind::TypeMismatch {
                user_type: std::any::type_name::<F::PhysicalType>().to_owned(),
                tiledb_type: self.attr.datatype()?,
            }));
        }

        let fill: &[F::PhysicalType] = value.to_raw();

        let c_attr = *self.attr.raw;
        let c_value = fill.as_ptr() as *const std::ffi::c_void;
        let c_size = std::mem::size_of_val(fill) as u64;

        self.capi_call(|ctx| unsafe {
            ffi::tiledb_attribute_set_fill_value(ctx, c_attr, c_value, c_size)
        })?;

        Ok(self)
    }

    pub fn fill_value_nullability<F: IntoFillValue>(
        self,
        value: F,
        nullable: bool,
    ) -> TileDBResult<Self> {
        if !self.attr.is_nullable()? && !nullable {
            /*
             * This should probably be embedded in the C API, but here's the deal:
             * If the attribute is not nullable, then the fill value cannot be null.
             * Using this function with `!nullable` agrees with that, but the C API
             * `tiledb_attribute_set_fill_value_nullable` does not check for that,
             * so we will here.
             */
            return self.fill_value(value);
        }

        if !self
            .attr
            .datatype()?
            .is_compatible_type::<F::PhysicalType>()
        {
            return Err(Error::Datatype(DatatypeErrorKind::TypeMismatch {
                user_type: std::any::type_name::<F::PhysicalType>().to_owned(),
                tiledb_type: self.attr.datatype()?,
            }));
        }

        let fill: &[F::PhysicalType] = value.to_raw();

        let c_attr = *self.attr.raw;
        let c_value = fill.as_ptr() as *const std::ffi::c_void;
        let c_size = std::mem::size_of_val(fill) as u64;
        let c_nullable: u8 = if nullable { 1 } else { 0 };

        self.capi_call(|ctx| unsafe {
            ffi::tiledb_attribute_set_fill_value_nullable(
                ctx, c_attr, c_value, c_size, c_nullable,
            )
        })?;

        Ok(self)
    }

    pub fn filter_list<FL>(self, filter_list: FL) -> TileDBResult<Self>
    where
        FL: Borrow<FilterList>,
    {
        let filter_list = filter_list.borrow();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_attribute_set_filter_list(
                ctx,
                *self.attr.raw,
                // TODO: does the C API copy this? Or alias the pointer? Safety is not obvious
                filter_list.capi(),
            )
        })?;
        Ok(self)
    }

    pub fn build(self) -> Attribute {
        self.attr
    }
}

/// Trait for data which can be used as a fill value for an attribute.
pub trait IntoFillValue {
    type PhysicalType: PhysicalType;

    /// Get a reference to the raw fill value data.
    /// The returned slice will be copied into the tiledb core.
    fn to_raw(&self) -> &[Self::PhysicalType];
}

/// Trait for data which can be constructed from an attribute's raw fill value.
pub trait FromFillValue<'a>: IntoFillValue + Sized {
    /// Construct a value of this type from a raw fill value.
    fn from_raw(raw: &'a [Self::PhysicalType]) -> TileDBResult<Self>;
}

impl<T> IntoFillValue for T
where
    T: PhysicalType,
{
    type PhysicalType = Self;

    fn to_raw(&self) -> &[Self::PhysicalType] {
        std::slice::from_ref(self)
    }
}

impl<T> FromFillValue<'_> for T
where
    T: PhysicalType,
{
    fn from_raw(raw: &[Self::PhysicalType]) -> TileDBResult<Self> {
        if raw.len() == 1 {
            Ok(raw[0])
        } else {
            Err(Error::Datatype(DatatypeErrorKind::UnexpectedCellStructure {
                context: None,
                found: CellValNum::try_from(raw.len() as u32)
                    /* this should be safe because core forbids zero-length fill value */
                    .unwrap(),
                expected: CellValNum::single()
            }))
        }
    }
}

impl<T, const K: usize> IntoFillValue for [T; K]
where
    T: PhysicalType,
{
    type PhysicalType = T;

    fn to_raw(&self) -> &[Self::PhysicalType] {
        self
    }
}

impl<'a, T, const K: usize> FromFillValue<'a> for [T; K]
where
    T: PhysicalType,
{
    fn from_raw(raw: &'a [Self::PhysicalType]) -> TileDBResult<Self> {
        Self::try_from(raw).map_err(|_|
            Error::Datatype(DatatypeErrorKind::UnexpectedCellStructure {
                context: None,
                found: CellValNum::try_from(raw.len() as u32)
                    /* this should be safe because core forbids zero-length fill value */
                    .unwrap(),
                expected: {
                    /* unfortunately no clear way to bound `0 < K < u32::MAX` for a trait impl */
                    let nz = u32::try_from(K).ok().and_then(NonZeroU32::new)
                        .expect("`impl FillValue for [T; K] requires 0 < K < u32::MAX");
                    CellValNum::Fixed(nz)
                }
            }))
    }
}

impl<T> IntoFillValue for &[T]
where
    T: PhysicalType,
{
    type PhysicalType = T;

    fn to_raw(&self) -> &[Self::PhysicalType] {
        self
    }
}

impl<'a, T> FromFillValue<'a> for &'a [T]
where
    T: PhysicalType,
{
    fn from_raw(raw: &'a [Self::PhysicalType]) -> TileDBResult<Self> {
        Ok(raw)
    }
}

impl<T> IntoFillValue for Vec<T>
where
    T: PhysicalType,
{
    type PhysicalType = T;

    fn to_raw(&self) -> &[Self::PhysicalType] {
        self.as_slice()
    }
}

impl<T> FromFillValue<'_> for Vec<T>
where
    T: PhysicalType,
{
    fn from_raw(raw: &[Self::PhysicalType]) -> TileDBResult<Self> {
        Ok(raw.to_vec())
    }
}

impl IntoFillValue for &str {
    type PhysicalType = u8;

    fn to_raw(&self) -> &[Self::PhysicalType] {
        self.as_bytes()
    }
}

impl<'a> FromFillValue<'a> for &'a str {
    fn from_raw(raw: &'a [Self::PhysicalType]) -> TileDBResult<Self> {
        std::str::from_utf8(raw).map_err(|e| {
            Error::Deserialization(
                "Non-UTF8 fill value".to_string(),
                anyhow!(e),
            )
        })
    }
}

impl IntoFillValue for String {
    type PhysicalType = u8;

    fn to_raw(&self) -> &[Self::PhysicalType] {
        self.as_bytes()
    }
}

impl<'a> FromFillValue<'a> for String {
    fn from_raw(raw: &'a [Self::PhysicalType]) -> TileDBResult<Self> {
        <&'a str as FromFillValue<'a>>::from_raw(raw).map(|s| s.to_string())
    }
}

/// Encapsulation of data needed to construct an Attribute's fill value
#[derive(Clone, Debug, Deserialize, OptionSubset, PartialEq, Serialize)]
pub struct FillData {
    pub data: crate::metadata::Value,
    pub nullability: Option<bool>,
}

/// Encapsulation of data needed to construct an Attribute
#[derive(
    Clone, Default, Debug, Deserialize, OptionSubset, Serialize, PartialEq,
)]
pub struct AttributeData {
    pub name: String,
    pub datatype: Datatype,
    pub nullability: Option<bool>,
    pub cell_val_num: Option<CellValNum>,
    pub fill: Option<FillData>,
    pub filters: FilterListData,
}

#[cfg(any(test, feature = "proptest-strategies"))]
impl AttributeData {
    /// Returns a strategy for generating values of this attribute's type.
    pub fn value_strategy(&self) -> crate::query::strategy::FieldValueStrategy {
        use crate::query::strategy::FieldValueStrategy;
        use proptest::prelude::*;

        use crate::filter::{CompressionData, CompressionType, FilterData};
        let has_double_delta = self.filters.iter().any(|f| {
            matches!(
                f,
                FilterData::Compression(CompressionData {
                    kind: CompressionType::DoubleDelta { .. },
                    ..
                })
            )
        });

        fn_typed!(self.datatype, LT, {
            type DT = <LT as LogicalType>::PhysicalType;
            if has_double_delta {
                if std::any::TypeId::of::<DT>() == std::any::TypeId::of::<u64>()
                {
                    // see core `DoubleDelta::compute_bitsize`
                    let min = 0u64;
                    let max = u64::MAX >> 1;
                    return FieldValueStrategy::from((min..=max).boxed());
                } else if std::any::TypeId::of::<DT>()
                    == std::any::TypeId::of::<i64>()
                {
                    let min = i64::MIN >> 2;
                    let max = i64::MAX >> 2;
                    return FieldValueStrategy::from((min..=max).boxed());
                }
            }
            FieldValueStrategy::from(any::<DT>().boxed())
        })
    }
}

impl Display for AttributeData {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}", json!(*self))
    }
}

impl TryFrom<&Attribute> for AttributeData {
    type Error = crate::error::Error;

    fn try_from(attr: &Attribute) -> TileDBResult<Self> {
        let datatype = attr.datatype()?;
        let fill = fn_typed!(datatype, LT, {
            type DT = <LT as LogicalType>::PhysicalType;
            let (fill_value, fill_value_nullability) =
                attr.fill_value_nullable::<&[DT]>()?;
            FillData {
                data: fill_value.to_vec().into(),
                nullability: Some(fill_value_nullability),
            }
        });

        Ok(AttributeData {
            name: attr.name()?,
            datatype,
            nullability: Some(attr.is_nullable()?),
            cell_val_num: Some(attr.cell_val_num()?),
            fill: Some(fill),
            filters: FilterListData::try_from(&attr.filter_list()?)?,
        })
    }
}

impl TryFrom<Attribute> for AttributeData {
    type Error = crate::error::Error;

    fn try_from(attr: Attribute) -> TileDBResult<Self> {
        Self::try_from(&attr)
    }
}

impl Factory for AttributeData {
    type Item = Attribute;

    fn create(&self, context: &Context) -> TileDBResult<Self::Item> {
        let mut b = Builder::new(context, &self.name, self.datatype)?
            .filter_list(self.filters.create(context)?)?;

        if let Some(n) = self.nullability {
            b = b.nullability(n)?;
        }
        if let Some(c) = self.cell_val_num {
            if !matches!((self.datatype, c), (Datatype::Any, CellValNum::Var)) {
                /* SC-46696 */
                b = b.cell_val_num(c)?;
            }
        }
        if let Some(ref fill) = self.fill {
            b = crate::metadata::value_go!(fill.data, _DT, ref value, {
                if let Some(fill_nullability) = fill.nullability {
                    b.fill_value_nullability(value.as_slice(), fill_nullability)
                } else {
                    b.fill_value(value.as_slice())
                }
            })?;
        }

        Ok(b.build())
    }
}

#[cfg(feature = "arrow")]
pub mod arrow;

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;

#[cfg(test)]
mod test {
    use super::*;
    use crate::filter::list::Builder as FilterListBuilder;
    use crate::filter::*;

    #[test]
    fn attribute_get_name_and_type() {
        let ctx = Context::new().expect("Error creating context instance.");
        let attr = Builder::new(&ctx, "xkcd", Datatype::UInt32)
            .expect("Error creating attribute instance.")
            .build();

        let name = attr.name().expect("Error getting attribute name.");
        assert_eq!(&name, "xkcd");

        let dtype = attr.datatype().expect("Error getting attribute datatype.");
        assert_eq!(dtype, Datatype::UInt32);
    }

    #[test]
    fn attribute_set_nullable() {
        let ctx = Context::new().expect("Error creating context instance.");

        {
            let attr = Builder::new(&ctx, "foo", Datatype::UInt64)
                .expect("Error creating attribute instance.")
                .build();

            let nullable = attr.is_nullable().unwrap();
            assert!(!nullable);
        }
        {
            let attr = Builder::new(&ctx, "foo", Datatype::UInt64)
                .expect("Error creating attribute instance.")
                .nullability(true)
                .expect("Error setting attribute nullability.")
                .build();

            let nullable = attr.is_nullable().unwrap();
            assert!(nullable);
        }
    }

    #[test]
    fn attribute_get_set_filter_list() -> TileDBResult<()> {
        let ctx = Context::new().expect("Error creating context instance.");

        {
            let attr = Builder::new(&ctx, "foo", Datatype::UInt8)
                .expect("Error creating attribute instance.")
                .build();

            let flist1 = attr
                .filter_list()
                .expect("Error getting attribute filter list.");
            let nfilters = flist1
                .get_num_filters()
                .expect("Error getting number of filters.");
            assert_eq!(nfilters, 0);
        }

        {
            let flist2 = FilterListBuilder::new(&ctx)
                .expect("Error creating filter list builder.")
                .add_filter(Filter::create(&ctx, FilterData::None)?)?
                .add_filter(Filter::create(
                    &ctx,
                    FilterData::BitWidthReduction { max_window: None },
                )?)?
                .add_filter(Filter::create(
                    &ctx,
                    FilterData::Compression(CompressionData::new(
                        CompressionType::Zstd,
                    )),
                )?)?
                .build();

            let attr = Builder::new(&ctx, "foo", Datatype::UInt8)
                .expect("Error creating attribute instance.")
                .filter_list(&flist2)
                .expect("Error setting filter list.")
                .build();

            let flist3 =
                attr.filter_list().expect("Error getting filter list.");
            let nfilters = flist3
                .get_num_filters()
                .expect("Error getting number of filters.");
            assert_eq!(nfilters, 3);
        }

        Ok(())
    }

    #[test]
    fn attribute_cell_val_size() {
        let ctx = Context::new().expect("Error creating context instance.");
        {
            let attr = Builder::new(&ctx, "foo", Datatype::UInt16)
                .expect("Error creating attribute instance.")
                .build();

            let num = attr.cell_val_num().expect("Error getting cell val num.");
            assert_eq!(num, <CellValNum as Default>::default());
            let size = attr
                .cell_size()
                .expect("Error getting attribute cell size.");
            assert_eq!(size, 2);
        }
        {
            let attr = Builder::new(&ctx, "foo", Datatype::UInt16)
                .expect("Error creating attribute instance.")
                .cell_val_num(CellValNum::try_from(3).unwrap())
                .expect("Error setting cell val num.")
                .build();
            let num = attr.cell_val_num().expect("Error getting cell val num.");
            assert_eq!(u32::from(num), 3);
            let size = attr
                .cell_size()
                .expect("Error getting attribute cell size.");
            assert_eq!(size, 6);
        }
        {
            let attr = Builder::new(&ctx, "foo", Datatype::UInt16)
                .expect("Error creating attribute instance.")
                .cell_val_num(CellValNum::Var)
                .expect("Error setting cell val size.")
                .build();
            let is_var = attr
                .is_var_sized()
                .expect("Error getting attribute var sized-ness.");
            assert!(is_var);
        }
        {
            let attr = Builder::new(&ctx, "foo", Datatype::UInt16)
                .expect("Error creating attribute instance.")
                .cell_val_num(CellValNum::try_from(42).unwrap())
                .expect("Error setting cell val num.")
                .build();
            let num = attr.cell_val_num().expect("Error getting cell val num.");
            assert_eq!(num, CellValNum::try_from(42).unwrap());
            let size = attr.cell_size().expect("Error getting cell val size.");
            assert_eq!(size, 84);
        }
        {
            let attr = Builder::new(&ctx, "foo", Datatype::UInt16)
                .expect("Error creating attribute instance.")
                .var_sized()
                .expect("Error setting var sized.")
                .build();
            let num = attr.cell_val_num().expect("Error getting cell val num.");
            assert_eq!(num, CellValNum::Var);
            let size = attr.cell_size().expect("Error getting cell val size.");
            assert_eq!(size, u64::MAX);
        }
    }

    #[test]
    fn attribute_test_set_fill_value_error() -> TileDBResult<()> {
        let ctx = Context::new()?;

        // nullable
        {
            let attr = Builder::new(&ctx, "foo", Datatype::UInt32)?
                .nullability(true)?
                .fill_value(5_i32);
            assert!(attr.is_err());
        }
        {
            let attr = Builder::new(&ctx, "foo", Datatype::UInt32)?
                .nullability(true)?
                .fill_value(5_u32);
            assert!(attr.is_err());
        }
        {
            let attr = Builder::new(&ctx, "foo", Datatype::UInt32)?
                .nullability(true)?
                .fill_value(1.0_f64);
            assert!(attr.is_err());
        }

        // non-nullable
        {
            let attr = Builder::new(&ctx, "foo", Datatype::UInt32)?
                .nullability(false)?
                .fill_value_nullability(5_i32, true);
            assert!(attr.is_err());
        }
        {
            let attr = Builder::new(&ctx, "foo", Datatype::UInt32)?
                .nullability(false)?
                .fill_value_nullability(5_i32, true);
            assert!(attr.is_err());
        }
        {
            let attr = Builder::new(&ctx, "foo", Datatype::UInt32)?
                .nullability(false)?
                .fill_value_nullability(1.0_f64, false);
            assert!(attr.is_err());
        }

        Ok(())
    }

    #[test]
    fn attribute_test_fill_value() -> TileDBResult<()> {
        let ctx = Context::new()?;

        // default
        {
            let attr = Builder::new(&ctx, "foo", Datatype::UInt32)?.build();

            let val: u32 = attr.fill_value()?;
            assert_eq!(val, u32::MAX);
        }

        // override
        {
            let attr = Builder::new(&ctx, "foo", Datatype::UInt32)?
                .fill_value(5_u32)?
                .build();

            let val: u32 = attr.fill_value()?;
            assert_eq!(val, 5);
        }

        Ok(())
    }

    #[test]
    fn attribute_test_fill_value_nullable() -> TileDBResult<()> {
        let ctx = Context::new()?;

        // default fill value
        {
            let attr = Builder::new(&ctx, "foo", Datatype::UInt32)?
                .nullability(true)?
                .build();

            let (val, validity): (u32, bool) = attr.fill_value_nullable()?;
            assert_eq!(val, u32::MAX);
            assert!(!validity);
        }

        // overridden
        {
            let attr = Builder::new(&ctx, "foo", Datatype::UInt32)?
                .nullability(true)?
                .fill_value_nullability(5_u32, true)?
                .build();

            let (val, validity): (u32, bool) = attr.fill_value_nullable()?;
            assert_eq!(val, 5);
            assert!(validity);
        }

        Ok(())
    }

    #[test]
    fn test_eq() {
        let ctx = Context::new().unwrap();

        let start_attr =
            |name: &str, dt: Datatype, nullable: bool| -> Builder {
                Builder::new(&ctx, name, dt)
                    .unwrap()
                    .nullability(nullable)
                    .unwrap()
            };

        let default_name = "foo";
        let default_dt = Datatype::Int32;
        let default_nullable = false;

        let default_attr =
            || start_attr(default_name, default_dt, default_nullable);

        let base = default_attr().build();

        // reflexive
        {
            let other = default_attr().build();
            assert_eq!(base, other);
        }

        // change name
        {
            let other = start_attr("bar", default_dt, default_nullable).build();
            assert_ne!(base, other);
        }

        // change type
        {
            let other =
                start_attr(default_name, Datatype::Float64, default_nullable)
                    .build();
            assert_ne!(base, other);
        }

        // change nullable
        {
            let other =
                start_attr(default_name, default_dt, !default_nullable).build();
            assert_ne!(base, other);
        }

        // change cellval
        {
            let other = start_attr(default_name, default_dt, default_nullable)
                .cell_val_num(
                    ((u32::from(base.cell_val_num().unwrap()) + 1) * 2)
                        .try_into()
                        .unwrap(),
                )
                .expect("Error setting cell val num")
                .build();
            assert_ne!(base, other);
        }

        // change fill val when the attribute is not nullable
        {
            let other = default_attr()
                .fill_value(3i32)
                .expect("Error setting fill value")
                .build();

            assert_ne!(base, other);
        }

        // change fill val when the attribute *is* nullable
        {
            let default_attr = || default_attr().nullability(false).unwrap();
            let base = default_attr().build();

            let other = default_attr()
                .fill_value(3i32)
                .expect("Error setting fill value")
                .build();

            assert_ne!(base, other);
        }

        // change fill nullable
        {
            let default_attr = || default_attr().nullability(true).unwrap();
            let base = default_attr().build();

            let (base_fill_value, base_fill_nullable) =
                base.fill_value_nullable::<i32>().unwrap();
            {
                let other = default_attr()
                    .fill_value_nullability(base_fill_value, base_fill_nullable)
                    .expect("Error setting fill value")
                    .build();
                assert_eq!(base, other);
            }
            {
                let other = default_attr()
                    .fill_value_nullability(
                        base_fill_value,
                        !base_fill_nullable,
                    )
                    .expect("Error setting fill value")
                    .build();
                assert_ne!(base, other);
            }
            {
                let new_fill_value = (base_fill_value / 2) + 1;
                let other = default_attr()
                    .fill_value_nullability(new_fill_value, base_fill_nullable)
                    .expect("Error setting fill value")
                    .build();
                assert_ne!(base, other);
            }
        }

        // change fill nullable when attribute is *not* nullable
        {
            let (base_fill_value, base_fill_nullable) =
                base.fill_value_nullable::<i32>().unwrap();
            {
                // copying the same settings should be fine
                let other = default_attr()
                    .fill_value_nullability(base_fill_value, base_fill_nullable)
                    .expect("Error setting fill value")
                    .build();
                assert_eq!(base, other);
            }
            {
                // fill value positive nullability should error due to conflict
                let other = default_attr().fill_value_nullability(
                    base_fill_value,
                    !base_fill_nullable,
                );
                assert!(other.is_err());
            }
            {
                let new_fill_value = (base_fill_value / 2) + 1;

                // just changing the value should also be fine
                let other = default_attr()
                    .fill_value_nullability(new_fill_value, base_fill_nullable)
                    .expect("Error setting fill value")
                    .build();
                assert_ne!(base, other);
            }
        }
    }
}
