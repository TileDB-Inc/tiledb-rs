extern crate tiledb_sys as ffi;

use std::borrow::Borrow;
use std::ops::Deref;

#[cfg(any(test, feature = "pod"))]
use std::fmt::{Debug, Formatter, Result as FmtResult};

use tiledb_common::array::attribute::{FromFillValue, IntoFillValue};

use crate::array::CellValNum;
use crate::context::{CApiInterface, Context, ContextBound};
use crate::datatype::physical::BitsEq;
use crate::error::{DatatypeError, Error};
use crate::filter::list::{FilterList, RawFilterList};
use crate::physical_type_go;
use crate::string::{RawTDBString, TDBString};
use crate::{Datatype, Result as TileDBResult};

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
        let mut c_dtype: ffi::tiledb_datatype_t = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_attribute_get_type(ctx, *self.raw, &mut c_dtype)
        })?;
        Ok(Datatype::try_from(c_dtype)?)
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
        let mut c_num: std::ffi::c_uint = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_attribute_get_cell_val_num(ctx, *self.raw, &mut c_num)
        })?;
        Ok(CellValNum::try_from(c_num)?)
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
            return Err(Error::Datatype(
                DatatypeError::physical_type_incompatible::<F::PhysicalType>(
                    self.datatype()?,
                ),
            ));
        }

        self.capi_call(|ctx| unsafe {
            ffi::tiledb_attribute_get_fill_value(
                ctx,
                c_attr,
                &mut c_ptr,
                &mut c_size,
            )
        })?;

        assert!(
            c_size
                .is_multiple_of(std::mem::size_of::<F::PhysicalType>() as u64),
            "Unexpected fill value size for compatible type {}: expected multiple of {}, found {}",
            std::any::type_name::<F::PhysicalType>(),
            std::mem::size_of::<F::PhysicalType>(),
            c_size
        );

        let len = c_size as usize / std::mem::size_of::<F::PhysicalType>();
        let slice: &[F::PhysicalType] = unsafe {
            std::slice::from_raw_parts(c_ptr as *const F::PhysicalType, len)
        };
        Ok(F::from_raw(slice)?)
    }

    pub fn fill_value_nullable<'a, F: FromFillValue<'a>>(
        &'a self,
    ) -> TileDBResult<(F, bool)> {
        if !self.datatype()?.is_compatible_type::<F::PhysicalType>() {
            return Err(Error::Datatype(
                DatatypeError::physical_type_incompatible::<F::PhysicalType>(
                    self.datatype()?,
                ),
            ));
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

        assert!(
            c_size
                .is_multiple_of(std::mem::size_of::<F::PhysicalType>() as u64),
            "Unexpected fill value size for compatible type {}: expected multiple of {}, found {}",
            std::any::type_name::<F::PhysicalType>(),
            std::mem::size_of::<F::PhysicalType>(),
            c_size
        );

        let len = c_size as usize / std::mem::size_of::<F::PhysicalType>();
        let slice: &[F::PhysicalType] = unsafe {
            std::slice::from_raw_parts(c_ptr as *const F::PhysicalType, len)
        };

        let is_valid = c_validity != 0;
        Ok((F::from_raw(slice)?, is_valid))
    }

    /// Get the enumeration name
    pub fn enumeration_name(&self) -> TileDBResult<Option<String>> {
        let c_enmr = self.capi();
        let mut c_str: *mut ffi::tiledb_string_t = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_attribute_get_enumeration_name(ctx, c_enmr, &mut c_str)
        })?;

        if c_str.is_null() {
            return Ok(None);
        }

        Ok(Some(
            TDBString {
                raw: RawTDBString::Owned(c_str),
            }
            .to_string()?,
        ))
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
            physical_type_go!(self.datatype().unwrap(), DT, {
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
            physical_type_go!(self.datatype().unwrap(), DT, {
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

#[cfg(any(test, feature = "pod"))]
impl Debug for Attribute {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match tiledb_pod::array::attribute::AttributeData::try_from(self) {
            Ok(a) => Debug::fmt(&a, f),
            Err(e) => {
                let RawAttribute::Owned(ptr) = self.raw;
                write!(f, "<Attribute @ {ptr:?}: serialization error: {e}>")
            }
        }
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
        let c_num = std::ffi::c_uint::from(num);
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

    /// Set the name of the enumeration to use.
    ///
    /// Note that when building schemas, the enumeration must have been added
    /// to the schema for adding an attribute that references it.
    pub fn enumeration_name<S>(self, name: S) -> TileDBResult<Self>
    where
        S: AsRef<str>,
    {
        let c_name = cstring!(name.as_ref());
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_attribute_set_enumeration_name(
                ctx,
                *self.attr.raw,
                c_name.as_c_str().as_ptr(),
            )
        })?;
        Ok(self)
    }

    pub fn fill_value<F: IntoFillValue>(self, value: F) -> TileDBResult<Self> {
        if !self
            .attr
            .datatype()?
            .is_compatible_type::<F::PhysicalType>()
        {
            return Err(Error::Datatype(
                DatatypeError::physical_type_incompatible::<F::PhysicalType>(
                    self.datatype()?,
                ),
            ));
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
            return Err(Error::Datatype(
                DatatypeError::physical_type_incompatible::<F::PhysicalType>(
                    self.attr.datatype()?,
                ),
            ));
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

#[cfg(feature = "arrow")]
pub mod arrow;

#[cfg(any(test, feature = "pod"))]
pub mod pod;

#[cfg(test)]
mod tests {
    use tiledb_pod::array::attribute::AttributeData;

    use super::*;
    use crate::Factory;
    use crate::filter::list::Builder as FilterListBuilder;
    use crate::filter::*;

    /// Test what the default values filled in for `None` with attribute data are.
    /// Mostly because if we write code which does need the default, we're expecting
    /// to match core and need to be notified if something changes or we did something
    /// wrong.
    #[test]
    fn attribute_defaults() {
        let ctx = Context::new().expect("Error creating context instance.");

        {
            let spec = AttributeData {
                name: "xkcd".to_owned(),
                datatype: Datatype::UInt32,
                ..Default::default()
            };
            let attr = spec.create(&ctx).unwrap();
            assert_eq!(CellValNum::single(), attr.cell_val_num().unwrap());

            // not nullable by default
            assert!(!attr.is_nullable().unwrap());
        }
        {
            let spec = AttributeData {
                name: "xkcd".to_owned(),
                datatype: Datatype::StringAscii,
                ..Default::default()
            };
            let attr = spec.create(&ctx).unwrap();
            assert_eq!(CellValNum::single(), attr.cell_val_num().unwrap());

            // not nullable by default
            assert!(!attr.is_nullable().unwrap());
        }
    }

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
