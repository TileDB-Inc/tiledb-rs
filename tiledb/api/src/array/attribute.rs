extern crate tiledb_sys as ffi;

use std::convert::From;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::ops::Deref;

use serde_json::json;
pub use tiledb_sys::Datatype;

use crate::context::Context;
use crate::convert::CAPIConverter;
use crate::error::Error;
use crate::filter_list::FilterList;
use crate::Result as TileDBResult;

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
        let res = unsafe {
            ffi::tiledb_attribute_get_name(c_context, *self.raw, &mut c_name)
        };
        if res == ffi::TILEDB_OK {
            let c_name = unsafe { std::ffi::CStr::from_ptr(c_name) };
            Ok(String::from(c_name.to_string_lossy()))
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn datatype(&self) -> TileDBResult<Datatype> {
        let c_context = self.context.capi();
        let mut c_dtype: std::ffi::c_uint = 0;
        let res = unsafe {
            ffi::tiledb_attribute_get_type(c_context, *self.raw, &mut c_dtype)
        };
        if res == ffi::TILEDB_OK {
            if let Some(dtype) = Datatype::from_u32(c_dtype) {
                Ok(dtype)
            } else {
                Err(Error::from("Invalid Datatype value returned by TileDB"))
            }
        } else {
            Err(self.context.expect_last_error())
        }
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

    pub fn filter_list(&self) -> TileDBResult<FilterList> {
        let c_context = self.context.capi();
        let mut c_flist: *mut ffi::tiledb_filter_list_t = out_ptr!();
        let res = unsafe {
            ffi::tiledb_attribute_get_filter_list(
                c_context,
                *self.raw,
                &mut c_flist,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(FilterList { _wrapped: c_flist })
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn is_var_sized(&self) -> TileDBResult<bool> {
        self.cell_val_num().map(|num| num == u32::MAX)
    }

    pub fn cell_val_num(&self) -> TileDBResult<u32> {
        let c_context = self.context.capi();
        let mut c_num: std::ffi::c_uint = 0;
        let res = unsafe {
            ffi::tiledb_attribute_get_cell_val_num(
                c_context, *self.raw, &mut c_num,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(c_num as u32)
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn cell_size(&self) -> TileDBResult<u64> {
        let c_context = self.context.capi();
        let mut c_size: std::ffi::c_ulonglong = 0;
        let res = unsafe {
            ffi::tiledb_attribute_get_cell_size(
                c_context,
                *self.raw,
                &mut c_size,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(c_size as u64)
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn fill_value<Conv: CAPIConverter>(&self) -> TileDBResult<Conv> {
        let c_context = self.context.capi();
        let c_attr = *self.raw;
        let mut c_ptr: *const std::ffi::c_void = out_ptr!();
        let mut c_size: u64 = 0;

        let res = unsafe {
            ffi::tiledb_attribute_get_fill_value(
                c_context,
                c_attr,
                &mut c_ptr,
                &mut c_size,
            )
        };

        if res != ffi::TILEDB_OK {
            return Err(self.context.expect_last_error());
        }

        if c_size != std::mem::size_of::<Conv::CAPIType>() as u64 {
            return Err(Error::from("Invalid value size returned by TileDB"));
        }

        let c_val: Conv::CAPIType = unsafe { *c_ptr.cast::<Conv::CAPIType>() };

        Ok(Conv::to_rust(&c_val))
    }

    pub fn fill_value_nullable<Conv: CAPIConverter>(
        &self,
    ) -> TileDBResult<(Conv, bool)> {
        let c_context = self.context.capi();
        let c_attr = *self.raw;
        let mut c_ptr: *const std::ffi::c_void = out_ptr!();
        let mut c_size: u64 = 0;
        let mut c_validity: u8 = 0;

        let res = unsafe {
            ffi::tiledb_attribute_get_fill_value_nullable(
                c_context,
                c_attr,
                &mut c_ptr,
                &mut c_size,
                &mut c_validity,
            )
        };

        if res != ffi::TILEDB_OK {
            return Err(self.context.expect_last_error());
        }

        if c_size != std::mem::size_of::<Conv::CAPIType>() as u64 {
            return Err(Error::from("Invalid value size returned by TileDB"));
        }

        let c_val: Conv::CAPIType = unsafe { *c_ptr.cast::<Conv::CAPIType>() };

        Ok((Conv::to_rust(&c_val), c_validity != 0))
    }
}

impl<'ctx> Debug for Attribute<'ctx> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let json = json!({
            "name": match self.name() {
                Ok(name) => name,
                Err(e) => format!("<error reading name: {}>", e),
            },
            "datatype": match self.datatype() {
                Ok(dt) => dt
                    .to_string()
                    .unwrap_or(String::from("<unrecognized datatype>")),
                Err(e) => format!("<error reading datatype: {}>", e),
            },
            /* TODO: other fields */
            "raw": format!("{:p}", *self.raw)
        });
        write!(f, "{}", json)
    }
}

pub struct Builder<'ctx> {
    attr: Attribute<'ctx>,
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
        let res = unsafe {
            ffi::tiledb_attribute_alloc(
                c_context,
                c_name.as_c_str().as_ptr(),
                datatype as u32,
                &mut c_attr,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(Builder {
                attr: Attribute {
                    context,
                    raw: RawAttribute::Owned(c_attr),
                },
            })
        } else {
            Err(context.expect_last_error())
        }
    }

    pub fn cell_val_num(self, num: u32) -> TileDBResult<Self> {
        let c_context = self.attr.context.capi();
        let c_num = num as std::ffi::c_uint;
        let res = unsafe {
            ffi::tiledb_attribute_set_cell_val_num(
                c_context,
                *self.attr.raw,
                c_num,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(self)
        } else {
            Err(self.attr.context.expect_last_error())
        }
    }

    pub fn var_sized(self) -> TileDBResult<Self> {
        self.cell_val_num(u32::MAX)
    }

    pub fn nullability(self, nullable: bool) -> TileDBResult<Self> {
        let c_context = self.attr.context.capi();
        let c_nullable: u8 = if nullable { 1 } else { 0 };
        let res = unsafe {
            ffi::tiledb_attribute_set_nullable(
                c_context,
                *self.attr.raw,
                c_nullable,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(self)
        } else {
            Err(self.attr.context.expect_last_error())
        }
    }

    // This currently does not support setting multi-value cells.
    pub fn fill_value<Conv: CAPIConverter + 'static>(
        self,
        value: Conv,
    ) -> TileDBResult<Self> {
        if !self.attr.datatype()?.is_compatible_type::<Conv>() {
            return Err(Error::from(format!(
                "Attribute type mismatch: expected {}, found {}",
                self.attr.datatype()?,
                std::any::type_name::<Conv>()
            )));
        }

        let c_context = self.attr.context.capi();
        let c_attr = *self.attr.raw;
        let c_val: Conv::CAPIType = value.to_capi();

        let res = unsafe {
            ffi::tiledb_attribute_set_fill_value(
                c_context,
                c_attr,
                &c_val as *const Conv::CAPIType as *const std::ffi::c_void,
                std::mem::size_of::<Conv::CAPIType>() as u64,
            )
        };

        if res == ffi::TILEDB_OK {
            Ok(self)
        } else {
            Err(self.attr.context.expect_last_error())
        }
    }

    // This currently does not support setting multi-value cells.
    pub fn fill_value_nullability<Conv: CAPIConverter + 'static>(
        self,
        value: Conv,
        nullable: bool,
    ) -> TileDBResult<Self> {
        if !self.attr.datatype()?.is_compatible_type::<Conv>() {
            return Err(Error::from(format!(
                "Attribute type mismatch: expected {}, found {}",
                self.attr.datatype()?,
                std::any::type_name::<Conv>()
            )));
        }

        let c_context = self.attr.context.capi();
        let c_attr = *self.attr.raw;
        let c_val: Conv::CAPIType = value.to_capi();
        let c_nullable: u8 = if nullable { 1 } else { 0 };

        let res = unsafe {
            ffi::tiledb_attribute_set_fill_value_nullable(
                c_context,
                c_attr,
                &c_val as *const Conv::CAPIType as *const std::ffi::c_void,
                std::mem::size_of::<Conv::CAPIType>() as u64,
                c_nullable,
            )
        };

        if res == ffi::TILEDB_OK {
            Ok(self)
        } else {
            Err(self.attr.context.expect_last_error())
        }
    }

    pub fn filter_list(self, filter_list: &FilterList) -> TileDBResult<Self> {
        let c_context = self.attr.context.capi();
        let res = unsafe {
            ffi::tiledb_attribute_set_filter_list(
                c_context,
                *self.attr.raw,
                // TODO: does the C API copy this? Or alias the pointer? Safety is not obvious
                filter_list.capi(),
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(self)
        } else {
            Err(self.attr.context.expect_last_error())
        }
    }

    pub fn build(self) -> Attribute<'ctx> {
        self.attr
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::filter::Filter;
    pub use tiledb_sys::FilterType;

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
    fn attribute_get_set_filter_list() {
        let ctx = Context::new().expect("Error creating context instance.");

        {
            let attr = Builder::new(&ctx, "foo", Datatype::UInt8)
                .expect("Error creating attribute instance.")
                .build();

            let flist1 = attr
                .filter_list()
                .expect("Error getting attribute filter list.");
            let nfilters = flist1
                .get_num_filters(&ctx)
                .expect("Error getting number of filters.");
            assert_eq!(nfilters, 0);
        }

        {
            let f1 = Filter::new(&ctx, FilterType::NONE)
                .expect("Error creating filter 1.");
            let f2 = Filter::new(&ctx, FilterType::BIT_WIDTH_REDUCTION)
                .expect("Error creating filter 2.");
            let f3 = Filter::new(&ctx, FilterType::ZSTD)
                .expect("Error creating filter 3.");
            let mut flist2 =
                FilterList::new(&ctx).expect("Error creating filter list.");
            flist2
                .add_filter(&ctx, &f1)
                .expect("Error adding filter 1 to list.");
            flist2
                .add_filter(&ctx, &f2)
                .expect("Error adding filter 2 to list.");
            flist2
                .add_filter(&ctx, &f3)
                .expect("Error adding filter 3 to list.");

            let attr = Builder::new(&ctx, "foo", Datatype::UInt8)
                .expect("Error creating attribute instance.")
                .filter_list(&flist2)
                .expect("Error setting filter list.")
                .build();

            let flist3 =
                attr.filter_list().expect("Error getting filter list.");
            let nfilters = flist3
                .get_num_filters(&ctx)
                .expect("Error getting number of filters.");
            assert_eq!(nfilters, 3);
        }
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
}
