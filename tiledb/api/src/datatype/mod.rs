pub use tiledb_common::datatype::*;

#[cfg(feature = "arrow")]
pub mod arrow;

trait ToStringCore {
    type Error: std::error::Error;
    fn to_string_core(&self) -> Result<String, Self::Error>;
}

trait FromStringCore {
    fn from_string_core(&self) -> Option<Self>;
}

impl ToStringCore for Datatype {
    type Error = !;
    fn to_string_core(&self) -> Result<String, Self::Error> {
        let copy = *self;
        let c_dtype = copy as ffi::tiledb_datatype_t;
        let mut c_str = std::ptr::null::<std::os::raw::c_char>();
        let res = unsafe { ffi::tiledb_datatype_to_str(c_dtype, &mut c_str) };

        /*
         * this cannot error if you provide a valid value, and the strong Rust
         * enum ensures that we have a valid value
         */
        assert_eq!(res, ffi::TILEDB_OK);

        let c_msg = unsafe { std::ffi::CStr::from_ptr(c_str) };
        Ok(c_msg.to_string_lossy())
    }
}

impl FromStringCore for Datatype {
    fn from_string_core(&self) -> Option<Self> {
        let c_dtype =
            std::ffi::CString::new(dtype).expect("Error creating CString");
        let mut c_ret: ffi::tiledb_datatype_t = 0;
        let res = unsafe {
            ffi::tiledb_datatype_from_str(
                c_dtype.as_c_str().as_ptr(),
                &mut c_ret,
            )
        };

        if res == ffi::TILEDB_OK {
            match Datatype::try_from(c_ret) {
                Ok(dt) => Some(dt),
                Err(_) => None,
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn str_roundtrip() {
        for datatype in Datatype::iter() {
            let s = datatype.to_string_core().unwrap();
            assert_eq!(datatype, Datatype::from_string_core(&s).unwrap());
        }
    }
}
