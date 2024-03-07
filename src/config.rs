extern crate tiledb_sys as ffi;

use crate::error::Error;

pub struct Config {
    _wrapped: *mut ffi::tiledb_config_t,
}

impl Config {
    pub fn new() -> Result<Config, String> {
        let mut cfg = Config {
            _wrapped: std::ptr::null_mut::<ffi::tiledb_config_t>(),
        };
        let mut err = Error::default();
        let res = unsafe {
            ffi::tiledb_config_alloc(
                &mut cfg._wrapped as *mut *mut ffi::tiledb_config_t,
                err.as_mut_ptr_ptr(),
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(cfg)
        } else if err.is_null() {
            Err(String::from(
                "Invalid error after failure to create config.",
            ))
        } else {
            let mut msg = std::ptr::null::<std::os::raw::c_char>();
            let res = unsafe {
                ffi::tiledb_error_message(
                    err.as_mut_ptr(),
                    &mut msg as *mut *const std::os::raw::c_char,
                )
            };
            if res == ffi::TILEDB_OK {
                let c_msg = unsafe { std::ffi::CStr::from_ptr(msg) };
                let msg = String::from(c_msg.to_string_lossy());
                Err(msg)
            } else {
                Err(String::from(
                    "Failed to retreive error message from TileDB.",
                ))
            }
        }
    }
}

impl Drop for Config {
    fn drop(&mut self) {
        if self._wrapped.is_null() {
            return;
        }
        unsafe {
            ffi::tiledb_config_free(
                &mut self._wrapped as *mut *mut ffi::tiledb_config_t,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_config_alloc() {
        Config::new().expect("Error creating config instance.");
    }
}
