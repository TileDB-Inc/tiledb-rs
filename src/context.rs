extern crate tiledb_sys as ffi;

use crate::config::Config;
use crate::error::Error;

pub struct Context {
    _wrapped: *mut ffi::tiledb_ctx_t,
}

impl Context {
    pub fn new() -> Result<Context, String> {
        let cfg = Config::new().expect("Error creating config instance.");
        Context::from_config(&cfg)
    }

    pub fn from_config(cfg: &Config) -> Result<Context, String> {
        let mut ctx = Context {
            _wrapped: std::ptr::null_mut::<ffi::tiledb_ctx_t>(),
        };
        let res = unsafe {
            ffi::tiledb_ctx_alloc(cfg.as_mut_ptr(), &mut ctx._wrapped)
        };
        if res == ffi::TILEDB_OK {
            Ok(ctx)
        } else {
            Err(String::from("Error creating context."))
        }
    }

    pub fn get_stats(&self) -> Result<String, String> {
        let mut c_json = std::ptr::null_mut::<std::os::raw::c_char>();
        let res = unsafe {
            ffi::tiledb_ctx_get_stats(
                self._wrapped,
                &mut c_json as *mut *mut std::os::raw::c_char,
            )
        };
        if res == ffi::TILEDB_OK {
            assert!(!c_json.is_null());
            let json = unsafe { std::ffi::CStr::from_ptr(c_json) };
            Ok(String::from(json.to_string_lossy()))
        } else {
            Err(self.get_last_error().unwrap_or_else(|| {
                String::from("Error getting last error from context")
            }))
        }
    }

    pub fn get_config(&self) -> Result<Config, String> {
        let mut cfg = Config::default();
        let res = unsafe {
            ffi::tiledb_ctx_get_config(self._wrapped, cfg.as_mut_ptr_ptr())
        };
        if res == ffi::TILEDB_OK {
            Ok(cfg)
        } else {
            Err(self.get_last_error().unwrap_or_else(|| {
                String::from("Error getting last error from context.")
            }))
        }
    }

    pub fn get_last_error(&self) -> Option<String> {
        let mut err = Error::default();
        let res = unsafe {
            ffi::tiledb_ctx_get_last_error(self._wrapped, err.as_mut_ptr_ptr())
        };
        if res == ffi::TILEDB_OK {
            Some(err.get_message())
        } else {
            None
        }
    }

    pub fn is_supported_fs(&self, fs: ffi::TileDBFilesystem) -> bool {
        let mut supported: i32 = 0;
        let res = unsafe {
            ffi::tiledb_ctx_is_supported_fs(
                self._wrapped,
                fs as u32,
                &mut supported,
            )
        };
        if res == ffi::TILEDB_OK {
            supported == 1
        } else {
            false
        }
    }

    pub fn set_tag(&self, key: &str, val: &str) -> Result<(), String> {
        let c_key =
            std::ffi::CString::new(key).expect("Error creating CString");
        let c_val =
            std::ffi::CString::new(val).expect("Error creating CString");
        let res = unsafe {
            ffi::tiledb_ctx_set_tag(
                self._wrapped,
                c_key.as_c_str().as_ptr(),
                c_val.as_c_str().as_ptr(),
            )
        };

        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(self.get_last_error().unwrap_or_else(|| {
                String::from("Error getting last error from context.")
            }))
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        if self._wrapped.is_null() {
            return;
        }
        unsafe { ffi::tiledb_ctx_free(&mut self._wrapped) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ctx_alloc() {
        Context::new().expect("Error creating context instance.");
    }

    #[test]
    fn ctx_from_config() {
        let cfg = Config::new().expect("Error creating config instance.");
        Context::from_config(&cfg).expect("Error creating context instance.");
    }

    #[test]
    fn ctx_set_tag() {
        let ctx = Context::new().expect("Error creating context instance.");
        ctx.set_tag("foo", "bar")
            .expect("Error setting tag on context.");
    }
}
