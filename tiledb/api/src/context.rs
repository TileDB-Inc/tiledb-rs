extern crate tiledb_sys as ffi;

use std::convert::From;

use crate::config::Config;
use crate::error::Error;
use crate::Result as TileDBResult;

pub enum ObjectType {
    Array,
    Group,
}

pub struct Context {
    _wrapped: *mut ffi::tiledb_ctx_t,
}

impl Context {
    pub fn new() -> TileDBResult<Context> {
        let cfg = Config::new().expect("Error creating config instance.");
        Context::from_config(&cfg)
    }

    pub fn as_mut_ptr(&self) -> *mut ffi::tiledb_ctx_t {
        self._wrapped
    }

    pub fn from_config(cfg: &Config) -> TileDBResult<Context> {
        let mut ctx = Context {
            _wrapped: std::ptr::null_mut::<ffi::tiledb_ctx_t>(),
        };
        let res = unsafe {
            ffi::tiledb_ctx_alloc(cfg.as_mut_ptr(), &mut ctx._wrapped)
        };
        if res == ffi::TILEDB_OK {
            Ok(ctx)
        } else {
            Err(Error::from("Error creating context."))
        }
    }

    pub fn get_stats(&self) -> TileDBResult<String> {
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
            Err(self.expect_last_error())
        }
    }

    pub fn get_config(&self) -> TileDBResult<Config> {
        let mut cfg = Config::default();
        let res = unsafe {
            ffi::tiledb_ctx_get_config(self._wrapped, cfg.as_mut_ptr_ptr())
        };
        if res == ffi::TILEDB_OK {
            Ok(cfg)
        } else {
            Err(self.expect_last_error())
        }
    }

    pub fn get_last_error(&self) -> Option<Error> {
        let mut c_err: *mut ffi::tiledb_error_t = std::ptr::null_mut();
        let res = unsafe {
            ffi::tiledb_ctx_get_last_error(self._wrapped, &mut c_err)
        };
        if res == ffi::TILEDB_OK && !c_err.is_null() {
            Some(Error::from(c_err))
        } else {
            None
        }
    }

    pub fn expect_last_error(&self) -> Error {
        self.get_last_error().unwrap_or(Error::from(
            "TileDB internal error: expected error data but found none",
        ))
    }

    pub fn is_supported_fs(&self, fs: ffi::Filesystem) -> bool {
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

    pub fn set_tag(&self, key: &str, val: &str) -> TileDBResult<()> {
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
            Err(self.expect_last_error())
        }
    }

    pub fn object_type(&self, name: &str) -> TileDBResult<Option<ObjectType>> {
        let c_name = cstring!(name);
        let mut c_objtype: ffi::tiledb_object_t = out_ptr!();

        let c_ret = unsafe {
            ffi::tiledb_object_type(
                self.as_mut_ptr(),
                c_name.as_ptr(),
                &mut c_objtype,
            )
        };
        if c_ret == ffi::TILEDB_OK {
            Ok(match c_objtype {
                ffi::tiledb_object_t_TILEDB_ARRAY => Some(ObjectType::Array),
                ffi::tiledb_object_t_TILEDB_GROUP => Some(ObjectType::Group),
                _ => None,
            })
        } else {
            Err(self.expect_last_error())
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
    fn ctx_get_stats() {
        let ctx = Context::new().expect("Error creating context instance.");
        let json = ctx.get_stats();
        // I have to wrap enable_stats/disable_stats before we'll get anything
        // useful out of this.
        assert!(json.unwrap() == "");
    }

    #[test]
    fn ctx_get_last_error() {
        let ctx = Context::new().expect("Error creating instance.");
        assert!(ctx.get_last_error().is_none());
    }

    #[test]
    fn ctx_is_supported_fs() {
        let ctx = Context::new().expect("Error creating instance.");

        // MEMFS is by default enabled in TileDB builds while HDFS is rarely
        // enabled. These tests failing most likely means a non "standard"
        // build of libtiledb.{so,dylib,dll}
        assert!(ctx.is_supported_fs(ffi::Filesystem::MEMFS));

        // On GitHub Actions, we use the release tarball which enables all
        // backends. Thus we skip this test when running in CI.
        let var = std::env::var("GITHUB_ACTIONS").unwrap_or(String::from(""));
        if var != *"true" {
            assert!(!ctx.is_supported_fs(ffi::Filesystem::HDFS));
        }
    }

    #[test]
    fn ctx_set_tag() {
        let ctx = Context::new().expect("Error creating context instance.");
        ctx.set_tag("foo", "bar")
            .expect("Error setting tag on context.");
    }
}
