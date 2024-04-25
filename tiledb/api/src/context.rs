use std::sync::{Arc, Mutex};

use crate::config::{Config, RawConfig};
use crate::error::{Error, RawError};
use crate::filesystem::Filesystem;
use crate::stats::RawStatsString;
use crate::Result as TileDBResult;

pub enum ObjectType {
    Array,
    Group,
}

pub(crate) struct RawContext {
    raw: *mut ffi::tiledb_ctx_t,
}

unsafe impl Send for RawContext {}

impl Drop for RawContext {
    fn drop(&mut self) {
        unsafe {
            ffi::tiledb_ctx_free(
                std::ptr::addr_of!(self.raw) as *mut *mut ffi::tiledb_ctx_t
            );
        }
    }
}

pub trait ContextBound<'ctx> {
    fn context(&self) -> &'ctx Context;
}

pub trait CApiInterface {
    // The callback is intentionally *not* returning a TileDBResult<i32> as this
    // forces folks to avoid putting anything that can error into the callback.
    // This will hopefully lead to our collective decision to do as minimal
    // work as possible in unsafe blocks.
    fn capi_call<Callable>(&self, action: Callable) -> TileDBResult<()>
    where
        Callable: FnOnce(*mut ffi::tiledb_ctx_t) -> i32;
}

impl<'ctx, T> CApiInterface for T
where
    T: ContextBound<'ctx>,
{
    fn capi_call<Callable>(&self, action: Callable) -> TileDBResult<()>
    where
        Callable: FnOnce(*mut ffi::tiledb_ctx_t) -> i32,
    {
        self.context().capi_call(action)
    }
}

pub struct Context {
    raw: Arc<Mutex<RawContext>>,
}

impl Context {
    pub fn new() -> TileDBResult<Context> {
        let cfg = Config::new()?;
        Context::from_config(&cfg)
    }

    pub fn from_config(cfg: &Config) -> TileDBResult<Context> {
        let mut c_ctx: *mut ffi::tiledb_ctx_t = out_ptr!();
        let res = unsafe { ffi::tiledb_ctx_alloc(cfg.capi(), &mut c_ctx) };
        if res == ffi::TILEDB_OK {
            Ok(Context {
                raw: Arc::new(Mutex::new(RawContext { raw: c_ctx })),
            })
        } else {
            Err(Error::LibTileDB(String::from("Could not create context")))
        }
    }

    pub fn capi_call<Callable>(&self, action: Callable) -> TileDBResult<()>
    where
        Callable: FnOnce(*mut ffi::tiledb_ctx_t) -> i32,
    {
        // Docs say that the error result from a poisoned lock are usually
        // just propagated to all threads by just calling unwrap on locks.
        let raw_ctx = self.raw.lock().unwrap();
        if action(raw_ctx.raw) == ffi::TILEDB_OK {
            Ok(())
        } else {
            // I've just pulled a second copy of the error code rather than
            // futz around with something like creating a private shared
            // function that has different guarantees about whether it holds
            // a lock or not.
            //
            // N.B., we want to do this while we have the lock held from
            // running the action callable so that any error we return is for
            // the correct API call. Without this an error may have come from
            // an intermediate API call instead.
            let mut c_err: *mut ffi::tiledb_error_t = out_ptr!();
            unsafe { ffi::tiledb_ctx_get_last_error(raw_ctx.raw, &mut c_err) };
            if !c_err.is_null() {
                Err(Error::from(RawError::Owned(c_err)))
            } else {
                Err(Error::Internal(String::from(
                    "libtiledb: expected error data but found none",
                )))
            }
        }
    }

    pub fn get_stats(&self) -> TileDBResult<String> {
        let mut c_json: *mut std::ffi::c_char = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_ctx_get_stats(
                ctx,
                &mut c_json as *mut *mut std::ffi::c_char,
            )
        })?;

        assert!(!c_json.is_null());
        let raw = RawStatsString::Owned(c_json);
        let json = unsafe { std::ffi::CStr::from_ptr(*raw) };
        Ok(String::from(json.to_string_lossy()))
    }

    pub fn get_config(&self) -> TileDBResult<Config> {
        let mut c_cfg: *mut ffi::tiledb_config_t = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_ctx_get_config(ctx, &mut c_cfg)
        })?;

        Ok(Config {
            raw: RawConfig::Owned(c_cfg),
        })
    }

    pub fn get_last_error(&self) -> Option<Error> {
        let mut c_err: *mut ffi::tiledb_error_t = out_ptr!();
        let res = self.capi_call(|ctx| unsafe {
            ffi::tiledb_ctx_get_last_error(ctx, &mut c_err)
        });

        if res.is_ok() && !c_err.is_null() {
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

    pub fn is_supported_fs(&self, fs: Filesystem) -> TileDBResult<bool> {
        let mut supported: i32 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_ctx_is_supported_fs(ctx, fs as u32, &mut supported)
        })?;

        Ok(supported == 1)
    }

    pub fn set_tag(&self, key: &str, val: &str) -> TileDBResult<()> {
        let c_key =
            std::ffi::CString::new(key).expect("Error creating CString");
        let c_val =
            std::ffi::CString::new(val).expect("Error creating CString");
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_ctx_set_tag(
                ctx,
                c_key.as_c_str().as_ptr(),
                c_val.as_c_str().as_ptr(),
            )
        })?;

        Ok(())
    }

    pub fn object_type<S>(&self, name: S) -> TileDBResult<Option<ObjectType>>
    where
        S: AsRef<str>,
    {
        let c_name = cstring!(name.as_ref());
        let mut c_objtype: ffi::tiledb_object_t = out_ptr!();

        self.capi_call(|ctx| unsafe {
            ffi::tiledb_object_type(ctx, c_name.as_ptr(), &mut c_objtype)
        })?;

        Ok(match c_objtype {
            ffi::tiledb_object_t_TILEDB_ARRAY => Some(ObjectType::Array),
            ffi::tiledb_object_t_TILEDB_GROUP => Some(ObjectType::Group),
            _ => None,
        })
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
        assert!(ctx.is_supported_fs(Filesystem::Memfs).unwrap());

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
