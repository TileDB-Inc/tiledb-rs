use std::ops::Deref;

use crate::error::Error;
use crate::Result as TileDBResult;

pub(crate) enum RawTDBString {
    Owned(*mut ffi::tiledb_string_t),
}

impl Deref for RawTDBString {
    type Target = *mut ffi::tiledb_string_t;
    fn deref(&self) -> &Self::Target {
        let RawTDBString::Owned(ref ffi) = *self;
        ffi
    }
}

impl Drop for RawTDBString {
    fn drop(&mut self) {
        let RawTDBString::Owned(ref mut ffi) = *self;
        unsafe {
            ffi::tiledb_string_free(ffi);
        }
    }
}

pub struct TDBString {
    pub(crate) raw: RawTDBString,
}

impl TDBString {
    pub(crate) fn from_raw(raw: RawTDBString) -> Self {
        Self { raw }
    }

    pub fn to_string(&self) -> TileDBResult<String> {
        let mut c_str = out_ptr!();
        let mut c_len: usize = 0;

        let res = unsafe {
            ffi::tiledb_string_view(*self.raw, &mut c_str, &mut c_len)
        };

        if res == ffi::TILEDB_OK {
            let raw_slice: &[u8] = unsafe {
                std::slice::from_raw_parts(c_str as *const u8, c_len)
            };
            let c_str = std::str::from_utf8(raw_slice).map_err(|e| {
                Error::LibTileDB(format!(
                    "TileDB returned a string that is not UTF-8: {}",
                    e
                ))
            })?;
            Ok(c_str.to_owned())
        } else {
            Err(Error::Internal(
                "Error getting string view from core.".to_owned(),
            ))
        }
    }
}
