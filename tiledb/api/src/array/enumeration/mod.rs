use std::fmt::{self, Debug, Formatter, Result as FmtResult};
use std::ops::Deref;

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::context::{CApiInterface, Context, ContextBound};
use crate::string::{RawTDBString, TDBString};
use crate::{Datatype, Factory, Result as TileDBResult};

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;

pub(crate) enum RawEnumeration {
    Owned(*mut ffi::tiledb_enumeration_t),
}

impl Deref for RawEnumeration {
    type Target = *mut ffi::tiledb_enumeration_t;
    fn deref(&self) -> &Self::Target {
        let RawEnumeration::Owned(ref ffi) = *self;
        ffi
    }
}

impl Drop for RawEnumeration {
    fn drop(&mut self) {
        let RawEnumeration::Owned(ref mut ffi) = *self;
        unsafe {
            ffi::tiledb_enumeration_free(ffi);
        }
    }
}

pub struct Enumeration {
    context: Context,
    raw: RawEnumeration,
}

impl ContextBound for Enumeration {
    fn context(&self) -> Context {
        self.context.clone()
    }
}

impl Enumeration {
    pub(crate) fn capi(&self) -> *mut ffi::tiledb_enumeration_t {
        *self.raw
    }

    pub fn name(&self) -> TileDBResult<String> {
        let c_enmr = self.capi();
        let mut c_str: *mut ffi::tiledb_string_t = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_enumeration_get_name(ctx, c_enmr, &mut c_str)
        })?;

        TDBString {
            raw: RawTDBString::Owned(c_str),
        }
        .to_string()
    }

    pub fn datatype(&self) -> TileDBResult<Datatype> {
        let c_enmr = self.capi();
        let mut dtype: ffi::tiledb_datatype_t = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_enumeration_get_type(ctx, c_enmr, &mut dtype)
        })?;

        Datatype::try_from(dtype)
    }

    pub fn cell_val_num(&self) -> TileDBResult<u32> {
        let c_enmr = self.capi();
        let mut c_cvn: u32 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_enumeration_get_cell_val_num(ctx, c_enmr, &mut c_cvn)
        })?;

        Ok(c_cvn)
    }

    pub fn is_var_sized(&self) -> TileDBResult<bool> {
        Ok(self.cell_val_num()? == u32::MAX)
    }

    pub fn ordered(&self) -> TileDBResult<bool> {
        let c_enmr = self.capi();
        let mut c_ordered: i32 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_enumeration_get_ordered(ctx, c_enmr, &mut c_ordered)
        })?;

        Ok(c_ordered != 0)
    }

    pub fn data(&self) -> TileDBResult<&[u8]> {
        let c_enmr = self.capi();
        let mut ptr: *const std::ffi::c_uchar = out_ptr!();
        let mut size: u64 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_enumeration_get_data(
                ctx,
                c_enmr,
                &mut ptr as *mut *const std::ffi::c_uchar
                    as *mut *const std::ffi::c_void,
                &mut size,
            )
        })?;

        // Rust docs say that we're not allowed to pass a nullptr to the
        // std::slice::from_raw_parts because that breaks how Option<&[T]>::None
        // is represented.
        if ptr.is_null() {
            ptr = std::ptr::NonNull::dangling().as_ptr();
        }

        Ok(unsafe { std::slice::from_raw_parts(ptr, size as usize) })
    }

    pub fn offsets(&self) -> TileDBResult<Option<&[u64]>> {
        let c_enmr = self.capi();
        let mut ptr: *const std::ffi::c_ulonglong = out_ptr!();
        let mut size: u64 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_enumeration_get_offsets(
                ctx,
                c_enmr,
                &mut ptr as *mut *const std::ffi::c_ulonglong
                    as *mut *const std::ffi::c_void,
                &mut size,
            )
        })?;

        // N.B., never put a null pointer in something that is wrapped in an
        // Option. Rust uses the nullptr to detect the difference between
        // Some and None. Here we're just returning None directly to avoid
        // the issue.
        if ptr.is_null() {
            return Ok(None);
        }

        // The size returned is the number of bytes, where from_raw_parts
        // wants the number of elements.
        assert!(size as usize % std::mem::size_of::<u64>() == 0);
        let elems = size as usize / std::mem::size_of::<u64>();
        Ok(Some(unsafe { std::slice::from_raw_parts(ptr, elems) }))
    }

    pub fn extend<T>(
        &self,
        data: &[T],
        offsets: Option<&[u64]>,
    ) -> TileDBResult<Enumeration> {
        let c_enmr = self.capi();
        let mut c_new_enmr: *mut ffi::tiledb_enumeration_t = out_ptr!();

        // Rust semantics require that slice pointers aren't nullptr so that
        // nullptr can be used to distinguish between Some and None. The stdlib
        // empty slices all appear to return 0x1 which is mentioned in the docs
        // as a valid strategy.
        let (offsets_ptr, offsets_len) = if offsets.is_none() {
            (std::ptr::null_mut() as *const u64, 0u64)
        } else {
            let offsets = offsets.unwrap();
            (offsets.as_ptr(), std::mem::size_of_val(offsets) as u64)
        };

        // An important note here is that the Enumeration allocator copies the
        // contents of data of offsets rather than assumes ownership. That
        // means this is safe as those bytes are guaranteed to be alive until
        // we drop self at the end of this method after returning from
        // tiledb_enumeration_alloc.
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_enumeration_extend(
                ctx,
                c_enmr,
                data.as_ptr() as *const std::ffi::c_void,
                std::mem::size_of_val(data) as u64,
                offsets_ptr as *const std::ffi::c_void,
                offsets_len,
                &mut c_new_enmr,
            )
        })?;

        Ok(Enumeration {
            context: self.context.clone(),
            raw: RawEnumeration::Owned(c_new_enmr),
        })
    }
}

impl Debug for Enumeration {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let name = self.name().map_err(|_| fmt::Error)?;
        let dtype = self.datatype().map_err(|_| fmt::Error)?;

        let dtype_string = dtype.to_string();
        let cell_val_num = self.cell_val_num().map_err(|_| fmt::Error)?;
        let ordered = self.ordered().map_err(|_| fmt::Error)?;

        let json = json!({
            "name": name,
            "datatype": dtype_string,
            "cell_val_num": cell_val_num,
            "ordered": ordered,
            "values": [], // TODO: Render values
        });
        write!(f, "{}", json)
    }
}

impl PartialEq<Enumeration> for Enumeration {
    fn eq(&self, other: &Enumeration) -> bool {
        eq_helper!(self.name(), other.name());
        eq_helper!(self.datatype(), other.datatype());
        eq_helper!(self.cell_val_num(), other.cell_val_num());
        eq_helper!(self.ordered(), other.ordered());
        eq_helper!(self.data(), other.data());

        // Can't use eq_helper here as offsets are considered equal when both
        // are None.
        let offsets_match = match (self.offsets(), other.offsets()) {
            (Ok(Some(mine)), Ok(Some(theirs))) => mine == theirs,
            (Ok(None), Ok(None)) => true,
            _ => false,
        };
        if !offsets_match {
            return false;
        }

        true
    }
}

pub struct Builder<'data, 'offsets> {
    context: Context,
    name: String,
    dtype: Datatype,
    cell_val_num: u32,
    ordered: bool,
    data: &'data [u8],
    offsets: Option<&'offsets [u64]>,
}

impl<'data, 'offsets> ContextBound for Builder<'data, 'offsets> {
    fn context(&self) -> Context {
        self.context.clone()
    }
}

pub trait EnumerationBuilderData {}
impl EnumerationBuilderData for u8 {}
impl EnumerationBuilderData for u16 {}
impl EnumerationBuilderData for u32 {}
impl EnumerationBuilderData for u64 {}
impl EnumerationBuilderData for i8 {}
impl EnumerationBuilderData for i16 {}
impl EnumerationBuilderData for i32 {}
impl EnumerationBuilderData for i64 {}
impl EnumerationBuilderData for f32 {}
impl EnumerationBuilderData for f64 {}

impl<'data, 'offsets> Builder<'data, 'offsets> {
    pub fn new<T: EnumerationBuilderData + 'static>(
        context: &Context,
        name: &str,
        dtype: Datatype,
        data: &'data [T],
        offsets: Option<&'offsets [u64]>,
    ) -> Self {
        let data = unsafe {
            std::slice::from_raw_parts(
                data.as_ptr() as *const std::ffi::c_void as *const u8,
                std::mem::size_of_val(data),
            )
        };

        Builder {
            context: context.clone(),
            name: name.to_owned(),
            dtype,
            cell_val_num: 1,
            ordered: false,
            data,
            offsets,
        }
    }

    pub fn cell_val_num(self, cell_val_num: u32) -> Self {
        Self {
            cell_val_num,
            ..self
        }
    }

    pub fn var_sized(self) -> Self {
        Self {
            cell_val_num: u32::MAX,
            ..self
        }
    }

    pub fn ordered(self, ordered: bool) -> Self {
        Self { ordered, ..self }
    }

    pub fn build(self) -> TileDBResult<Enumeration> {
        let mut c_enmr: *mut ffi::tiledb_enumeration_t = out_ptr!();
        let name_bytes = self.name.as_bytes();
        let c_name = cstring!(name_bytes);
        let c_dtype = self.dtype.capi_enum();

        // Rust semantics require that slice pointers aren't nullptr so that
        // nullptr can be used to distinguish between Some and None. The stdlib
        // empty slices all appear to return 0x1 which is mentioned in the docs
        // as a valid strategy.
        let (offsets_ptr, offsets_len) = if self.offsets.is_none() {
            (std::ptr::null_mut() as *const u64, 0u64)
        } else {
            let offsets = self.offsets.unwrap();
            (offsets.as_ptr(), std::mem::size_of_val(offsets) as u64)
        };

        // An important note here is that the Enumeration allocator copies the
        // contents of data and offsets rather than assumes ownership. That
        // means this is safe as those bytes are guaranteed to be alive until
        // we drop self at the end of this method after returning from
        // tiledb_enumeration_alloc.
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_enumeration_alloc(
                ctx,
                c_name.as_c_str().as_ptr(),
                c_dtype,
                self.cell_val_num,
                if self.ordered { 1 } else { 0 },
                self.data.as_ptr() as *const std::ffi::c_void,
                std::mem::size_of_val(self.data) as u64,
                offsets_ptr as *const std::ffi::c_void,
                offsets_len,
                &mut c_enmr,
            )
        })?;

        Ok(Enumeration {
            context: self.context,
            raw: RawEnumeration::Owned(c_enmr),
        })
    }
}

/// Encapsulation of data needed to construct an Enumeration
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct EnumerationData {
    pub name: String,
    pub datatype: Datatype,
    pub cell_val_num: Option<u32>,
    pub ordered: Option<bool>,
    pub data: Box<[u8]>,
    pub offsets: Option<Box<[u64]>>,
}

impl TryFrom<&Enumeration> for EnumerationData {
    type Error = crate::error::Error;

    fn try_from(enmr: &Enumeration) -> TileDBResult<Self> {
        let datatype = enmr.datatype()?;
        let cell_val_num = enmr.cell_val_num()?;
        let data = Box::from(enmr.data()?);
        let offsets: Option<Box<[u64]>> = enmr.offsets()?.map(Box::from);

        Ok(EnumerationData {
            name: enmr.name()?,
            datatype,
            cell_val_num: Some(cell_val_num),
            ordered: Some(enmr.ordered()?),
            data,
            offsets,
        })
    }
}

impl Factory for EnumerationData {
    type Item = Enumeration;

    fn create(&self, context: &Context) -> TileDBResult<Self::Item> {
        let mut b = Builder::new(
            context,
            &self.name,
            self.datatype,
            &self.data[..],
            self.offsets.as_ref().map(|o| &o[..]),
        );

        if let Some(cvn) = self.cell_val_num {
            b = b.cell_val_num(cvn);
        }

        if let Some(ordered) = self.ordered {
            b = b.ordered(ordered);
        }

        b.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_build() -> TileDBResult<()> {
        let ctx = Context::new().expect("Error creating context instance.");
        let data = &vec![0, 1, 2, 3, 4][..];
        let enmr = Builder::new(&ctx, "foo", Datatype::Int32, data, None)
            .build()
            .expect("Error building enumeration.");

        assert_eq!(enmr.name()?, "foo");
        assert_eq!(enmr.datatype()?, Datatype::Int32);
        assert_eq!(enmr.cell_val_num()?, 1);
        assert!(!enmr.ordered()?);

        Ok(())
    }

    #[test]
    fn var_sized_error_build() -> TileDBResult<()> {
        let ctx = Context::new().expect("Error creating context instance.");
        let data = &vec![0u8, 1, 2, 3, 4][..];
        let enmr_res = Builder::new(&ctx, "foo", Datatype::Int32, data, None)
            .var_sized()
            .build();

        assert!(enmr_res.is_err());

        Ok(())
    }

    #[test]
    fn ordered_build() -> TileDBResult<()> {
        let ctx = Context::new().expect("Error creating context instance.");
        let data = &vec![0, 1, 2, 3, 4][..];
        let enmr = Builder::new(&ctx, "foo", Datatype::Int32, data, None)
            .ordered(true)
            .build()
            .expect("Error building enumeration.");

        assert_eq!(enmr.name()?, "foo");
        assert_eq!(enmr.datatype()?, Datatype::Int32);
        assert_eq!(enmr.cell_val_num()?, 1);
        assert!(enmr.ordered()?);

        Ok(())
    }

    #[test]
    fn string_build() -> TileDBResult<()> {
        let ctx = Context::new().expect("Error creating context instance.");
        let data = &("foobarbazbammam"
            .chars()
            .map(|c| c as u8)
            .collect::<Vec<u8>>())[..];
        let offsets = &vec![0u64, 3, 6, 9, 12][..];
        let enmr = Builder::new(
            &ctx,
            "foo",
            Datatype::StringAscii,
            data,
            Some(offsets),
        )
        .var_sized()
        .build()
        .expect("Error building enumeration.");

        assert_eq!(enmr.name()?, "foo");
        assert_eq!(enmr.datatype()?, Datatype::StringAscii);
        assert_eq!(enmr.cell_val_num()?, u32::MAX);
        assert!(!enmr.ordered()?);

        Ok(())
    }

    #[test]
    fn extend_enumeration() -> TileDBResult<()> {
        let ctx = Context::new().expect("Error creating context instance.");
        let data = &vec![1, 2, 3, 4, 5][..];
        let enmr1 = Builder::new(&ctx, "foo", Datatype::Int32, data, None)
            .build()
            .expect("Error building enumeration.");

        let enmr2 = enmr1
            .extend(&vec![6, 7, 8, 9, 10][..], None)
            .expect("Error extending enumeration.");

        assert_eq!(enmr1.name()?, enmr2.name()?);
        assert_eq!(enmr1.datatype()?, enmr2.datatype()?);
        assert_eq!(enmr1.cell_val_num()?, enmr2.cell_val_num()?);
        assert_eq!(enmr1.ordered()?, enmr2.ordered()?);
        assert_ne!(enmr1, enmr2);

        Ok(())
    }

    #[test]
    fn inequal_enumerations() -> TileDBResult<()> {
        let ctx = Context::new()?;

        let base = EnumerationData {
            name: "foo".to_owned(),
            datatype: Datatype::StringAscii,
            cell_val_num: Some(u32::MAX),
            ordered: Some(false),
            data: Box::from("foobarbazbam".as_bytes()),
            offsets: Some(Box::from(&vec![0, 3, 6, 9][..])),
        };

        let enmr1 = base.create(&ctx)?;

        {
            let enmr2 = base.create(&ctx)?;
            assert_eq!(enmr1, enmr2);
        }

        {
            let base2 = EnumerationData {
                name: "bar".to_owned(),
                ..base.clone()
            };
            let enmr2 = base2.create(&ctx)?;
            assert_ne!(enmr1, enmr2);
        }

        {
            let base2 = EnumerationData {
                datatype: Datatype::StringUtf8,
                ..base.clone()
            };
            let enmr2 = base2.create(&ctx)?;
            assert_ne!(enmr1, enmr2);
        }

        // cell_val_num is covered in a separate test as it requires that
        // offsets is None.

        {
            let base2 = EnumerationData {
                data: Box::from("aaabbbcccddd".as_bytes()),
                ..base.clone()
            };
            let enmr2 = base2.create(&ctx)?;
            assert_ne!(enmr1, enmr2);
        }

        {
            let base2 = EnumerationData {
                // N.B., the repeated values below may look weird, but that's
                // just an example where the third value is an empty string.
                offsets: Some(Box::from(&vec![0, 2, 6, 6][..])),
                ..base.clone()
            };
            let enmr2 = base2.create(&ctx)?;
            assert_ne!(enmr1, enmr2);
        }

        Ok(())
    }

    #[test]
    fn inequal_enumeration_cell_val_nums() -> TileDBResult<()> {
        let ctx = Context::new()?;

        let base = EnumerationData {
            name: "foo".to_owned(),
            datatype: Datatype::UInt8,
            cell_val_num: Some(1),
            ordered: Some(false),
            data: Box::from(&vec![0u8, 1, 2, 3, 4, 5][..]),
            offsets: None,
        };

        let enmr1 = base.create(&ctx)?;

        let base2 = EnumerationData {
            cell_val_num: Some(2),
            ..base
        };

        let enmr2 = base2.create(&ctx)?;
        assert_ne!(enmr1, enmr2);

        Ok(())
    }
}
