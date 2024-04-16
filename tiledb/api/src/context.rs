use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

use crate::config::{Config, RawConfig};
use crate::error::{Error, RawError};
use crate::filesystem::Filesystem;
use crate::stats::RawStatsString;
use crate::Result as TileDBResult;

pub enum ObjectType {
    Array,
    Group,
}

pub(crate) enum RawContext {
    Owned(*mut ffi::tiledb_ctx_t),
}

impl Deref for RawContext {
    type Target = *mut ffi::tiledb_ctx_t;
    fn deref(&self) -> &Self::Target {
        let RawContext::Owned(ref ffi) = *self;
        ffi
    }
}

impl Drop for RawContext {
    fn drop(&mut self) {
        let RawContext::Owned(ref mut ffi) = *self;
        unsafe {
            ffi::tiledb_ctx_free(ffi);
        }
    }
}

pub trait ContextBound<'ctx> {
    fn context(&self) -> &'ctx Context;
}

impl<'ctx, T> ContextBound<'ctx> for Arc<T>
where
    T: ContextBound<'ctx>,
{
    fn context(&self) -> &'ctx Context {
        (**self).context()
    }
}

impl<'ctx, T> ContextBound<'ctx> for Rc<T>
where
    T: ContextBound<'ctx>,
{
    fn context(&self) -> &'ctx Context {
        (**self).context()
    }
}

pub trait CApiInterface {
    fn capi_return(&self, c_ret: i32) -> TileDBResult<()>;
}

impl<'ctx, T> CApiInterface for T
where
    T: ContextBound<'ctx>,
{
    fn capi_return(&self, c_ret: i32) -> TileDBResult<()> {
        self.context().capi_return(c_ret)
    }
}

pub struct Context {
    raw: RawContext,
}

impl Context {
    pub fn capi(&self) -> *mut ffi::tiledb_ctx_t {
        *self.raw
    }

    pub fn new() -> TileDBResult<Context> {
        let cfg = Config::new()?;
        Context::from_config(&cfg)
    }

    pub fn from_config(cfg: &Config) -> TileDBResult<Context> {
        let mut c_ctx: *mut ffi::tiledb_ctx_t = out_ptr!();
        let res = unsafe { ffi::tiledb_ctx_alloc(cfg.capi(), &mut c_ctx) };
        if res == ffi::TILEDB_OK {
            Ok(Context {
                raw: RawContext::Owned(c_ctx),
            })
        } else {
            Err(Error::LibTileDB(String::from("Could not create context")))
        }
    }

    pub fn get_stats(&self) -> TileDBResult<String> {
        let mut c_json = std::ptr::null_mut::<std::os::raw::c_char>();
        let res = unsafe {
            ffi::tiledb_ctx_get_stats(
                *self.raw,
                &mut c_json as *mut *mut std::os::raw::c_char,
            )
        };
        if res == ffi::TILEDB_OK {
            assert!(!c_json.is_null());
            let raw = RawStatsString::Owned(c_json);
            let json = unsafe { std::ffi::CStr::from_ptr(*raw) };
            Ok(String::from(json.to_string_lossy()))
        } else {
            Err(self.expect_last_error())
        }
    }

    pub fn get_config(&self) -> TileDBResult<Config> {
        let mut c_cfg: *mut ffi::tiledb_config_t = out_ptr!();
        let res = unsafe { ffi::tiledb_ctx_get_config(*self.raw, &mut c_cfg) };
        if res == ffi::TILEDB_OK {
            Ok(Config {
                raw: RawConfig::Owned(c_cfg),
            })
        } else {
            Err(self.expect_last_error())
        }
    }

    pub fn get_last_error(&self) -> Option<Error> {
        let mut c_err: *mut ffi::tiledb_error_t = out_ptr!();
        let res =
            unsafe { ffi::tiledb_ctx_get_last_error(*self.raw, &mut c_err) };
        if res == ffi::TILEDB_OK && !c_err.is_null() {
            Some(Error::from(RawError::Owned(c_err)))
        } else {
            None
        }
    }

    pub fn expect_last_error(&self) -> Error {
        self.get_last_error()
            .unwrap_or(Error::Internal(String::from(
                "libtiledb: expected error data but found none",
            )))
    }

    pub fn is_supported_fs(&self, fs: Filesystem) -> bool {
        let mut supported: i32 = 0;
        let res = unsafe {
            ffi::tiledb_ctx_is_supported_fs(
                *self.raw,
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
                *self.raw,
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

    pub fn object_type<S>(&self, name: S) -> TileDBResult<Option<ObjectType>>
    where
        S: AsRef<str>,
    {
        let c_name = cstring!(name.as_ref());
        let mut c_objtype: ffi::tiledb_object_t = out_ptr!();

        let c_ret = unsafe {
            ffi::tiledb_object_type(*self.raw, c_name.as_ptr(), &mut c_objtype)
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

    /// Safely translate a return value from the C API into a TileDBResult
    pub(crate) fn capi_return(&self, c_ret: i32) -> TileDBResult<()> {
        if c_ret == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(self.expect_last_error())
        }
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
        assert!(ctx.is_supported_fs(Filesystem::Memfs));

        // We can't guarantee that any VFS backend is not present so any test
        // for an unsupported backend is guaranteed to fail somewhere.
    }

    #[test]
    fn ctx_set_tag() {
        let ctx = Context::new().expect("Error creating context instance.");
        ctx.set_tag("foo", "bar")
            .expect("Error setting tag on context.");
    }
}
