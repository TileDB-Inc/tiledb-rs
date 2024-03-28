extern crate tiledb_sys as ffi;

use std::borrow::Borrow;
use std::convert::From;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::ops::Deref;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::context::{CApiInterface, Context, ContextBound};
use crate::convert::{BitsEq, CAPIConverter};
use crate::error::{DatatypeErrorKind, Error};
use crate::filter_list::{FilterList, FilterListData, RawFilterList};
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

pub struct Attribute<'ctx> {
    context: &'ctx Context,
    raw: RawAttribute,
}

impl<'ctx> ContextBound<'ctx> for Attribute<'ctx> {
    fn context(&self) -> &'ctx Context {
        self.context
    }
}

impl<'ctx> Attribute<'ctx> {
    pub(crate) fn capi(&self) -> *mut ffi::tiledb_attribute_t {
        *self.raw
    }

    pub(crate) fn new(context: &'ctx Context, raw: RawAttribute) -> Self {
        Attribute { context, raw }
    }

    pub fn name(&self) -> TileDBResult<String> {
        let c_context = self.context.capi();
        let mut c_name = std::ptr::null::<std::ffi::c_char>();
        self.capi_return(unsafe {
            ffi::tiledb_attribute_get_name(c_context, *self.raw, &mut c_name)
        })?;
        let c_name = unsafe { std::ffi::CStr::from_ptr(c_name) };
        Ok(String::from(c_name.to_string_lossy()))
    }

    pub fn datatype(&self) -> TileDBResult<Datatype> {
        let c_context = self.context.capi();
        let mut c_dtype: std::ffi::c_uint = 0;
        self.capi_return(unsafe {
            ffi::tiledb_attribute_get_type(c_context, *self.raw, &mut c_dtype)
        })?;
        Datatype::try_from(c_dtype)
    }

    pub fn is_nullable(&self) -> bool {
        let c_context = self.context.capi();
        let mut c_nullable: std::ffi::c_uchar = 0;
        let c_ret = unsafe {
            ffi::tiledb_attribute_get_nullable(
                c_context,
                *self.raw,
                &mut c_nullable,
            )
        };
        assert_eq!(ffi::TILEDB_OK, c_ret); // Rust API should prevent sanity check failure
        c_nullable == 1
    }

    pub fn filter_list(&self) -> TileDBResult<FilterList<'ctx>> {
        let c_context = self.context.capi();
        let mut c_flist: *mut ffi::tiledb_filter_list_t = out_ptr!();
        self.capi_return(unsafe {
            ffi::tiledb_attribute_get_filter_list(
                c_context,
                *self.raw,
                &mut c_flist,
            )
        })?;
        Ok(FilterList {
            context: self.context,
            raw: RawFilterList::Owned(c_flist),
        })
    }

    pub fn is_var_sized(&self) -> TileDBResult<bool> {
        self.cell_val_num().map(|num| num == u32::MAX)
    }

    pub fn cell_val_num(&self) -> TileDBResult<u32> {
        let c_context = self.context.capi();
        let mut c_num: std::ffi::c_uint = 0;
        self.capi_return(unsafe {
            ffi::tiledb_attribute_get_cell_val_num(
                c_context, *self.raw, &mut c_num,
            )
        })?;
        Ok(c_num as u32)
    }

    pub fn cell_size(&self) -> TileDBResult<u64> {
        let c_context = self.context.capi();
        let mut c_size: std::ffi::c_ulonglong = 0;
        self.capi_return(unsafe {
            ffi::tiledb_attribute_get_cell_size(
                c_context,
                *self.raw,
                &mut c_size,
            )
        })?;
        Ok(c_size as u64)
    }

    pub fn fill_value<Conv: CAPIConverter>(&self) -> TileDBResult<Conv> {
        let c_context = self.context.capi();
        let c_attr = *self.raw;
        let mut c_ptr: *const std::ffi::c_void = out_ptr!();
        let mut c_size: u64 = 0;

        self.capi_return(unsafe {
            ffi::tiledb_attribute_get_fill_value(
                c_context,
                c_attr,
                &mut c_ptr,
                &mut c_size,
            )
        })?;

        if c_size != std::mem::size_of::<Conv::CAPIType>() as u64 {
            return Err(Error::Datatype(DatatypeErrorKind::TypeMismatch {
                user_type: std::any::type_name::<Conv>(),
                tiledb_type: self.datatype()?,
            }));
        }

        let c_val: Conv::CAPIType = unsafe { *c_ptr.cast::<Conv::CAPIType>() };

        Ok(Conv::to_rust(&c_val))
    }

    pub fn fill_value_nullable<Conv: CAPIConverter>(
        &self,
    ) -> TileDBResult<(Conv, bool)> {
        if !self.is_nullable() {
            /* see comment in Builder::fill_value_nullability */
            return Ok((self.fill_value()?, false));
        }

        let c_context = self.context.capi();
        let c_attr = *self.raw;
        let mut c_ptr: *const std::ffi::c_void = out_ptr!();
        let mut c_size: u64 = 0;
        let mut c_validity: u8 = 0;

        self.capi_return(unsafe {
            ffi::tiledb_attribute_get_fill_value_nullable(
                c_context,
                c_attr,
                &mut c_ptr,
                &mut c_size,
                &mut c_validity,
            )
        })?;

        if c_size != std::mem::size_of::<Conv::CAPIType>() as u64 {
            return Err(Error::Datatype(DatatypeErrorKind::TypeMismatch {
                user_type: std::any::type_name::<Conv>(),
                tiledb_type: self.datatype()?,
            }));
        }

        let c_val: Conv::CAPIType = unsafe { *c_ptr.cast::<Conv::CAPIType>() };

        Ok((Conv::to_rust(&c_val), c_validity != 0))
    }
}

impl<'ctx> Debug for Attribute<'ctx> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let data =
            AttributeData::try_from(self).map_err(|_| std::fmt::Error)?;
        let mut json = json!(data);
        json["raw"] = json!(format!("{:p}", *self.raw));

        write!(f, "{}", json)
    }
}

impl<'c1, 'c2> PartialEq<Attribute<'c2>> for Attribute<'c1> {
    fn eq(&self, other: &Attribute<'c2>) -> bool {
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

        let nullable_match = self.is_nullable() == other.is_nullable();
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

        let fill_value_match = if self.is_nullable() {
            fn_typed!(self.datatype().unwrap(), DT, {
                match (
                    self.fill_value_nullable::<DT>(),
                    other.fill_value_nullable::<DT>(),
                ) {
                    (Ok(mine), Ok(theirs)) => mine.bits_eq(&theirs),
                    _ => false,
                }
            })
        } else {
            fn_typed!(self.datatype().unwrap(), DT, {
                match (self.fill_value::<DT>(), other.fill_value::<DT>()) {
                    (Ok(mine), Ok(theirs)) => mine.bits_eq(&theirs),
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

pub struct Builder<'ctx> {
    attr: Attribute<'ctx>,
}

impl<'ctx> ContextBound<'ctx> for Builder<'ctx> {
    fn context(&self) -> &'ctx Context {
        self.attr.context
    }
}

impl<'ctx> Builder<'ctx> {
    pub fn new(
        context: &'ctx Context,
        name: &str,
        datatype: Datatype,
    ) -> TileDBResult<Self> {
        let c_context = context.capi();
        let mut c_attr: *mut ffi::tiledb_attribute_t = out_ptr!();
        let c_name = cstring!(name);
        context.capi_return(unsafe {
            ffi::tiledb_attribute_alloc(
                c_context,
                c_name.as_c_str().as_ptr(),
                datatype as u32,
                &mut c_attr,
            )
        })?;
        Ok(Builder {
            attr: Attribute {
                context,
                raw: RawAttribute::Owned(c_attr),
            },
        })
    }

    pub fn context(&self) -> &'ctx Context {
        self.attr.context
    }

    pub fn datatype(&self) -> TileDBResult<Datatype> {
        self.attr.datatype()
    }

    pub fn cell_val_num(self, num: u32) -> TileDBResult<Self> {
        let c_context = self.attr.context.capi();
        let c_num = num as std::ffi::c_uint;
        self.capi_return(unsafe {
            ffi::tiledb_attribute_set_cell_val_num(
                c_context,
                *self.attr.raw,
                c_num,
            )
        })?;
        Ok(self)
    }

    pub fn var_sized(self) -> TileDBResult<Self> {
        self.cell_val_num(u32::MAX)
    }

    pub fn is_nullable(&self) -> bool {
        self.attr.is_nullable()
    }

    pub fn nullability(self, nullable: bool) -> TileDBResult<Self> {
        let c_context = self.attr.context.capi();
        let c_nullable: u8 = if nullable { 1 } else { 0 };
        self.capi_return(unsafe {
            ffi::tiledb_attribute_set_nullable(
                c_context,
                *self.attr.raw,
                c_nullable,
            )
        })?;
        Ok(self)
    }

    // This currently does not support setting multi-value cells.
    pub fn fill_value<Conv: CAPIConverter + 'static>(
        self,
        value: Conv,
    ) -> TileDBResult<Self> {
        if !self.attr.datatype()?.is_compatible_type::<Conv>() {
            return Err(Error::Datatype(DatatypeErrorKind::TypeMismatch {
                user_type: std::any::type_name::<Conv>(),
                tiledb_type: self.attr.datatype()?,
            }));
        }

        let c_context = self.attr.context.capi();
        let c_attr = *self.attr.raw;
        let c_val: Conv::CAPIType = value.to_capi();

        self.capi_return(unsafe {
            ffi::tiledb_attribute_set_fill_value(
                c_context,
                c_attr,
                &c_val as *const Conv::CAPIType as *const std::ffi::c_void,
                std::mem::size_of::<Conv::CAPIType>() as u64,
            )
        })?;

        Ok(self)
    }

    // This currently does not support setting multi-value cells.
    pub fn fill_value_nullability<Conv: CAPIConverter + 'static>(
        self,
        value: Conv,
        nullable: bool,
    ) -> TileDBResult<Self> {
        if !self.attr.is_nullable() && !nullable {
            /*
             * This should probably be embedded in the C API, but here's the deal:
             * If the attribute is not nullable, then the fill value cannot be null.
             * Using this function with `!nullable` agrees with that, but the C API
             * `tiledb_attribute_set_fill_value_nullable` does not check for that,
             * so we will here.
             */
            return self.fill_value(value);
        }

        if !self.attr.datatype()?.is_compatible_type::<Conv>() {
            return Err(Error::Datatype(DatatypeErrorKind::TypeMismatch {
                user_type: std::any::type_name::<Conv>(),
                tiledb_type: self.attr.datatype()?,
            }));
        }

        let c_context = self.attr.context.capi();
        let c_attr = *self.attr.raw;
        let c_val: Conv::CAPIType = value.to_capi();
        let c_nullable: u8 = if nullable { 1 } else { 0 };

        self.capi_return(unsafe {
            ffi::tiledb_attribute_set_fill_value_nullable(
                c_context,
                c_attr,
                &c_val as *const Conv::CAPIType as *const std::ffi::c_void,
                std::mem::size_of::<Conv::CAPIType>() as u64,
                c_nullable,
            )
        })?;

        Ok(self)
    }

    pub fn filter_list<FL>(self, filter_list: FL) -> TileDBResult<Self>
    where
        FL: Borrow<FilterList<'ctx>>,
    {
        let filter_list = filter_list.borrow();
        let c_context = self.attr.context.capi();
        self.capi_return(unsafe {
            ffi::tiledb_attribute_set_filter_list(
                c_context,
                *self.attr.raw,
                // TODO: does the C API copy this? Or alias the pointer? Safety is not obvious
                filter_list.capi(),
            )
        })?;
        Ok(self)
    }

    pub fn build(self) -> Attribute<'ctx> {
        self.attr
    }
}

/// Encapsulation of data needed to construct an Attribute's fill value
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct FillData {
    pub data: serde_json::value::Value,
    pub nullability: Option<bool>,
}

/// Encapsulation of data needed to construct an Attribute
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AttributeData {
    pub name: String,
    pub datatype: Datatype,
    pub nullability: Option<bool>,
    pub cell_val_num: Option<u32>,
    pub fill: Option<FillData>,
    pub filters: FilterListData,
}

impl<'ctx> TryFrom<&Attribute<'ctx>> for AttributeData {
    type Error = crate::error::Error;

    fn try_from(attr: &Attribute<'ctx>) -> TileDBResult<Self> {
        let datatype = attr.datatype()?;
        let fill = fn_typed!(datatype, AT, {
            let (fill_value, fill_value_nullability) =
                attr.fill_value_nullable::<AT>()?;
            FillData {
                data: json!(fill_value),
                nullability: Some(fill_value_nullability),
            }
        });

        Ok(AttributeData {
            name: attr.name()?,
            datatype,
            nullability: Some(attr.is_nullable()),
            cell_val_num: Some(attr.cell_val_num()?),
            fill: Some(fill),
            filters: FilterListData::try_from(&attr.filter_list()?)?,
        })
    }
}

impl<'ctx> Factory<'ctx> for AttributeData {
    type Item = Attribute<'ctx>;

    fn create(&self, context: &'ctx Context) -> TileDBResult<Self::Item> {
        let mut b = Builder::new(context, &self.name, self.datatype)?
            .filter_list(self.filters.create(context)?)?;

        if let Some(n) = self.nullability {
            b = b.nullability(n)?;
        }
        if let Some(c) = self.cell_val_num {
            b = b.cell_val_num(c)?;
        }
        if let Some(ref fill) = self.fill {
            b = fn_typed!(self.datatype, AT, {
                let fill_value: AT = serde_json::from_value::<AT>(
                    fill.data.clone(),
                )
                .map_err(|e| {
                    Error::Deserialization(
                        format!("attribute '{}' fill value", self.name),
                        anyhow!(e),
                    )
                })?;
                if let Some(fill_nullability) = fill.nullability {
                    b.fill_value_nullability::<AT>(fill_value, fill_nullability)
                } else {
                    b.fill_value::<AT>(fill_value)
                }
            })?;
        }

        Ok(b.build())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::filter::*;
    use crate::filter_list::Builder as FilterListBuilder;

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

            let nullable = attr.is_nullable();
            assert!(!nullable);
        }
        {
            let attr = Builder::new(&ctx, "foo", Datatype::UInt64)
                .expect("Error creating attribute instance.")
                .nullability(true)
                .expect("Error setting attribute nullability.")
                .build();

            let nullable = attr.is_nullable();
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
            assert_eq!(num, 1);
            let size = attr
                .cell_size()
                .expect("Error getting attribute cell size.");
            assert_eq!(size, 2);
        }
        {
            let attr = Builder::new(&ctx, "foo", Datatype::UInt16)
                .expect("Error creating attribute instance.")
                .cell_val_num(3)
                .expect("Error setting cell val num.")
                .build();
            let num = attr.cell_val_num().expect("Error getting cell val num.");
            assert_eq!(num, 3);
            let size = attr
                .cell_size()
                .expect("Error getting attribute cell size.");
            assert_eq!(size, 6);
        }
        {
            let attr = Builder::new(&ctx, "foo", Datatype::UInt16)
                .expect("Error creating attribute instance.")
                .cell_val_num(u32::MAX)
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
                .cell_val_num(42)
                .expect("Error setting cell val num.")
                .build();
            let num = attr.cell_val_num().expect("Error getting cell val num.");
            assert_eq!(num, 42);
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
            assert_eq!(num, u32::MAX);
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
                .cell_val_num((base.cell_val_num().unwrap() + 1) * 2)
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
                let other = default_attr()
                    .fill_value_nullability(
                        (base_fill_value / 2) + 1,
                        base_fill_nullable,
                    )
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
                // just changing the value should also be fine
                let other = default_attr()
                    .fill_value_nullability(
                        (base_fill_value / 2) + 1,
                        base_fill_nullable,
                    )
                    .expect("Error setting fill value")
                    .build();
                assert_ne!(base, other);
            }
        }
    }
}
