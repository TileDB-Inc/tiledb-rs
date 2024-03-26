use std::fmt::{self, Debug, Formatter, Result as FmtResult};
use std::ops::Deref;

use serde_json::json;

use crate::column::{Column, TryAsColumn};
use crate::context::Context;
use crate::string::{RawTDBString, TDBString};
use crate::Datatype;
use crate::Result as TileDBResult;

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

pub struct Enumeration<'ctx> {
    pub(crate) context: &'ctx Context,
    pub(crate) raw: RawEnumeration,
}

impl<'ctx> Enumeration<'ctx> {
    pub(crate) fn capi(&self) -> *mut ffi::tiledb_enumeration_t {
        *self.raw
    }

    pub fn name(&self) -> TileDBResult<String> {
        let mut c_str: *mut ffi::tiledb_string_t = out_ptr!();
        let res = unsafe {
            ffi::tiledb_enumeration_get_name(
                self.context.capi(),
                self.capi(),
                &mut c_str,
            )
        };
        if res == ffi::TILEDB_OK {
            let tdb_str = TDBString {
                raw: RawTDBString::Owned(c_str),
            };
            tdb_str.to_string()
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn datatype(&self) -> TileDBResult<Datatype> {
        let mut dtype: ffi::tiledb_datatype_t = out_ptr!();
        let res = unsafe {
            ffi::tiledb_enumeration_get_type(
                self.context.capi(),
                self.capi(),
                &mut dtype,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(Datatype::from_capi_enum(dtype))
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn cell_val_num(&self) -> TileDBResult<u32> {
        let mut c_cvn: u32 = 0;
        let res = unsafe {
            ffi::tiledb_enumeration_get_cell_val_num(
                self.context.capi(),
                self.capi(),
                &mut c_cvn,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(c_cvn)
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn is_var_sized(&self) -> TileDBResult<bool> {
        Ok(self.cell_val_num()? == u32::MAX)
    }

    pub fn ordered(&self) -> TileDBResult<bool> {
        let mut c_ordered: i32 = 0;
        let res = unsafe {
            ffi::tiledb_enumeration_get_ordered(
                self.context.capi(),
                self.capi(),
                &mut c_ordered,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(c_ordered != 0)
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn data(&self) -> TileDBResult<&[u8]> {
        let mut ptr: *const std::ffi::c_uchar = out_ptr!();
        let mut size: u64 = 0;
        let res = unsafe {
            ffi::tiledb_enumeration_get_data(
                self.context.capi(),
                self.capi(),
                &mut ptr as *mut *const std::ffi::c_uchar
                    as *mut *const std::ffi::c_void,
                &mut size,
            )
        };

        // Rust docs say that we're not allowed to pass a nullptr to the
        // std::slice::from_raw_parts because that breaks how Option<&[T]>::None
        // is represented.
        if ptr.is_null() {
            ptr = std::ptr::NonNull::dangling().as_ptr();
        }

        if res == ffi::TILEDB_OK {
            let slice =
                unsafe { std::slice::from_raw_parts(ptr, size as usize) };
            Ok(slice)
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn offsets(&self) -> TileDBResult<&[u64]> {
        let mut ptr: *const std::ffi::c_ulonglong = out_ptr!();
        let mut size: u64 = 0;
        let res = unsafe {
            ffi::tiledb_enumeration_get_offsets(
                self.context.capi(),
                self.capi(),
                &mut ptr as *mut *const std::ffi::c_ulonglong
                    as *mut *const std::ffi::c_void,
                &mut size,
            )
        };

        // Rust docs say that we're not allowed to pass a nullptr to the
        // std::slice::from_raw_parts because that breaks how Option<&[T]>::None
        // is represented.
        if ptr.is_null() {
            ptr = std::ptr::NonNull::dangling().as_ptr();
        }

        // The size returned is the number of bytes, where from_raw_parts
        // wants the number of elements.
        assert!(size as usize % std::mem::size_of::<u64>() == 0);
        let elems = size as usize / std::mem::size_of::<u64>();

        if res == ffi::TILEDB_OK {
            let slice = unsafe { std::slice::from_raw_parts(ptr, elems) };
            Ok(slice)
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn extend(&self, column: Column) -> TileDBResult<Enumeration<'ctx>> {
        let mut c_new_enmr: *mut ffi::tiledb_enumeration_t = out_ptr!();

        // Rust semantics require that slice pointers aren't nullptr so that
        // nullptr can be used to distinguish between Some and None. The stdlib
        // empty slices all appear to return 0x1 which is mentioned in the docs
        // as a valid strategy.
        let (offsets_ptr, offsets_len) = if column.offsets().is_none() {
            (std::ptr::null_mut() as *const u64, 0u64)
        } else {
            let offsets = column.offsets().unwrap();
            (offsets.as_ptr(), std::mem::size_of_val(offsets) as u64)
        };

        // An important note here is that the Enumeration allocator copies the
        // contents of data of offsets rather than assumes ownership. That
        // means this is safe as those bytes are guaranteed to be alive until
        // we drop self at the end of this method after returning from
        // tiledb_enumeration_alloc.
        let res = unsafe {
            ffi::tiledb_enumeration_extend(
                self.context.capi(),
                self.capi(),
                column.data().as_ptr() as *const std::ffi::c_void,
                column.data().len() as u64,
                offsets_ptr as *const std::ffi::c_void,
                offsets_len,
                &mut c_new_enmr,
            )
        };

        if res == ffi::TILEDB_OK {
            Ok(Enumeration {
                context: self.context,
                raw: RawEnumeration::Owned(c_new_enmr),
            })
        } else {
            Err(self.context.expect_last_error())
        }
    }
}

impl<'data> TryAsColumn<'data> for Enumeration<'data> {
    fn try_as_column(&'data self) -> TileDBResult<Column<'data>> {
        let dtype = self.datatype()?;
        let cell_val_num = self.cell_val_num()?;
        let data = self.data()?;
        let offsets = self.offsets()?;

        let num_values = if offsets.is_empty() {
            data.len() as u64 / (dtype.size() * cell_val_num as u64)
        } else {
            offsets.len() as u64
        };

        let value_size = if cell_val_num == u32::MAX {
            dtype.size()
        } else {
            dtype.size() * cell_val_num as u64
        };

        Ok(Column::from_references(
            num_values,
            value_size,
            data,
            Some(offsets),
        ))
    }
}

impl<'ctx> Debug for Enumeration<'ctx> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let name = self.name().map_err(|_| fmt::Error)?;
        let dtype = self.datatype().map_err(|_| fmt::Error)?;

        let dtype_string =
            dtype.to_string().unwrap_or("<unknown datatype>".to_owned());
        let cell_val_num = self.cell_val_num().map_err(|_| fmt::Error)?;
        let ordered = self.ordered().map_err(|_| fmt::Error)?;
        let col = self.try_as_column().map_err(|_| fmt::Error)?;

        let json = json!({
            "name": name,
            "datatype": dtype_string,
            "cell_val_num": cell_val_num,
            "ordered": ordered,
            "values": col.to_json(dtype, cell_val_num).map_err(|_| fmt::Error)?,
        });
        write!(f, "{}", json)
    }
}

impl<'c1, 'c2> PartialEq<Enumeration<'c2>> for Enumeration<'c1> {
    fn eq(&self, other: &Enumeration<'c2>) -> bool {
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

        let cell_val_num_match =
            match (self.cell_val_num(), other.cell_val_num()) {
                (Ok(mine), Ok(theirs)) => mine == theirs,
                _ => false,
            };
        if !cell_val_num_match {
            return false;
        }

        let ordered_match = match (self.ordered(), other.ordered()) {
            (Ok(mine), Ok(theirs)) => mine == theirs,
            _ => false,
        };
        if !ordered_match {
            return false;
        }

        let cols_match = match (self.try_as_column(), other.try_as_column()) {
            (Ok(mine), Ok(theirs)) => mine == theirs,
            _ => false,
        };
        if !cols_match {
            return false;
        }

        true
    }
}

pub struct Builder<'ctx> {
    context: &'ctx Context,
    name: String,
    dtype: Datatype,
    cell_val_num: u32,
    ordered: bool,
}

impl<'ctx> Builder<'ctx> {
    pub fn new(context: &'ctx Context, name: &str, dtype: Datatype) -> Self {
        Builder {
            context,
            name: name.to_owned(),
            dtype,
            cell_val_num: 1,
            ordered: false,
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

    pub fn build(self, column: Column) -> TileDBResult<Enumeration<'ctx>> {
        let mut c_enmr: *mut ffi::tiledb_enumeration_t = out_ptr!();
        let name_bytes = self.name.as_bytes();
        let c_name = cstring!(name_bytes);
        let c_dtype = self.dtype.capi_enum();

        // Rust semantics require that slice pointers aren't nullptr so that
        // nullptr can be used to distinguish between Some and None. The stdlib
        // empty slices all appear to return 0x1 which is mentioned in the docs
        // as a valid strategy.
        let (offsets_ptr, offsets_len) = if column.offsets().is_none() {
            (std::ptr::null_mut() as *const u64, 0u64)
        } else {
            let offsets = column.offsets().unwrap();
            (offsets.as_ptr(), std::mem::size_of_val(offsets) as u64)
        };

        // An important note here is that the Enumeration allocator copies the
        // contents of data and offsets rather than assumes ownership. That
        // means this is safe as those bytes are guaranteed to be alive until
        // we drop self at the end of this method after returning from
        // tiledb_enumeration_alloc.
        let res = unsafe {
            ffi::tiledb_enumeration_alloc(
                self.context.capi(),
                c_name.as_c_str().as_ptr(),
                c_dtype,
                self.cell_val_num,
                if self.ordered { 1 } else { 0 },
                column.data().as_ptr() as *const std::ffi::c_void,
                column.data().len() as u64,
                offsets_ptr as *const std::ffi::c_void,
                offsets_len,
                &mut c_enmr,
            )
        };

        if res == ffi::TILEDB_OK {
            Ok(Enumeration {
                context: self.context,
                raw: RawEnumeration::Owned(c_enmr),
            })
        } else {
            Err(self.context.expect_last_error())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::AsColumn;

    #[test]
    fn basic_build() -> TileDBResult<()> {
        let ctx = Context::new().expect("Error creating context instance.");
        let enmr = Builder::new(&ctx, "foo", Datatype::Int32)
            .build(vec![0, 1, 2, 3, 4].as_column())
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
        let enmr_res = Builder::new(&ctx, "foo", Datatype::Int32)
            .var_sized()
            .build(vec![0u8, 1, 2, 3, 4].as_column());

        assert!(enmr_res.is_err());

        Ok(())
    }

    #[test]
    fn ordered_build() -> TileDBResult<()> {
        let ctx = Context::new().expect("Error creating context instance.");
        let enmr = Builder::new(&ctx, "foo", Datatype::Int32)
            .ordered(true)
            .build(vec![0, 1, 2, 3, 4].as_column())
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
        let enmr = Builder::new(&ctx, "foo", Datatype::StringAscii)
            .var_sized()
            .build(vec!["foo", "bar", "baz", "bam", "mam"].as_column())
            .expect("Error building enumeration.");

        assert_eq!(enmr.name()?, "foo");
        assert_eq!(enmr.datatype()?, Datatype::StringAscii);
        assert_eq!(enmr.cell_val_num()?, u32::MAX);
        assert!(!enmr.ordered()?);

        Ok(())
    }

    #[test]
    fn try_as_column() -> TileDBResult<()> {
        let values = vec!["foo", "bar", "baz", "bam", "mam"];
        let ctx = Context::new().expect("Error creating context instance.");
        let enmr = Builder::new(&ctx, "foo", Datatype::StringAscii)
            .var_sized()
            .build(values.as_column())
            .expect("Error building enumeration.");

        let col = enmr
            .try_as_column()
            .expect("Error converting Enumeration to column.");

        assert_eq!(col.data().len(), 15);
        assert_eq!(col.data()[7], b'a');
        assert_eq!(col.data()[8], b'z');
        assert_eq!(col.data()[9], b'b');
        assert_eq!(col.offsets().expect("Invalid offsets")[0], 0);
        assert_eq!(col.offsets().expect("Invalid offsets")[2], 6);

        assert_eq!(
            col.to_string_vec(enmr.datatype()?, enmr.cell_val_num()?)?,
            values
        );

        Ok(())
    }

    #[test]
    fn extend_enumeration() -> TileDBResult<()> {
        let ctx = Context::new().expect("Error creating context instance.");
        let enmr1 = Builder::new(&ctx, "foo", Datatype::Int32)
            .build(vec![1, 2, 3, 4, 5].as_column())
            .expect("Error building enumeration.");

        let enmr2 = enmr1
            .extend(vec![6, 7, 8, 9, 10].as_column())
            .expect("Error extending enumeration.");

        assert_eq!(enmr1.name()?, enmr2.name()?);
        assert_eq!(enmr1.datatype()?, enmr2.datatype()?);
        assert_eq!(enmr1.cell_val_num()?, enmr2.cell_val_num()?);
        assert_eq!(enmr1.ordered()?, enmr2.ordered()?);

        Ok(())
    }

    #[test]
    fn json_int_test() -> TileDBResult<()> {
        let ctx = Context::new()?;
        let enmr = Builder::new(&ctx, "foo", Datatype::Int32)
            .build(vec![1, 2, 3].as_column())?;

        let jsonstr = format!("{:?}", enmr);
        assert!(jsonstr.contains("[1,2,3]"));

        Ok(())
    }

    #[test]
    fn json_str_test() -> TileDBResult<()> {
        let ctx = Context::new()?;
        let enmr = Builder::new(&ctx, "foo", Datatype::StringAscii)
            .var_sized()
            .build(vec!["foo", "bar", "baz"].as_column())?;

        let jsonstr = format!("{:?}", enmr);
        assert!(jsonstr.contains("[\"foo\",\"bar\",\"baz\"]"));
        println!("{}", jsonstr);
        Ok(())
    }
}
