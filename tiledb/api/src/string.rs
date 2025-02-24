use std::ops::Deref;
use std::str::Utf8Error;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Internal error reading string view from libtiledb")]
    Internal,
    #[error("String is not UTF-8")]
    NonUtf8(Vec<u8>, Utf8Error),
}

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

    pub fn to_string(&self) -> Result<String, Error> {
        let mut c_str = out_ptr!();
        let mut c_len: usize = 0;

        let res = unsafe {
            ffi::tiledb_string_view(*self.raw, &mut c_str, &mut c_len)
        };

        if res == ffi::TILEDB_OK {
            // The type of `c_str` is platform dependent which means that we
            // have to cast anything that might use i8 to u8. However, this
            // means that platforms (i.e., Ubuntu arm64) that have a u8
            // c_char type will generate a clippy error about an unnecessary
            // cast from u8 to u8. Hence why we're ignoring the lint here.
            #[allow(clippy::unnecessary_cast)]
            let c_u8_str = c_str as *const u8;

            let raw_slice: &[u8] =
                unsafe { std::slice::from_raw_parts(c_u8_str, c_len) };
            match std::str::from_utf8(raw_slice) {
                Ok(s) => Ok(s.to_owned()),
                Err(e) => Err(Error::NonUtf8(raw_slice.to_vec(), e)),
            }
        } else {
            Err(Error::Internal)
        }
    }
}
