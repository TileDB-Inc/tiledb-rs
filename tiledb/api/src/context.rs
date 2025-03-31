use std::convert::From;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::ops::Deref;
use std::rc::Rc;

use crate::Result as TileDBResult;
use crate::config::{Config, RawConfig};
use crate::filesystem::Filesystem;
use crate::stats::RawStatsString;

/// An error which can occur when creating a new `Context`.
#[derive(Debug, thiserror::Error)]
pub enum CreateContextError {
    #[error("Error configuring context: {0}")]
    Config(CApiError),
    #[error("Internal error: out of memory")]
    OutOfMemory,
    #[error("Internal error: unknown")]
    Fatal,
    #[error(
        "Internal error: invalid return from libtiledb when allocating context: {0}"
    )]
    InternalInvalidReturnValue(i64),
}

/// An error which can occur when calling a `libtiledb` C API function.
#[derive(Debug, thiserror::Error)]
pub enum CApiError {
    #[error("Invalid string argument to C API: {0}")]
    InvalidCString(std::ffi::NulError),
    #[error("Error returned from libtiledb: {0}")]
    Error(String),
    #[error("Internal error retrieving error message from libtiledb")]
    Internal,
}

pub type CApiResult<T> = Result<T, CApiError>;

pub(crate) enum RawError {
    Owned(*mut ffi::tiledb_error_t),
}

impl Deref for RawError {
    type Target = *mut ffi::tiledb_error_t;
    fn deref(&self) -> &Self::Target {
        let RawError::Owned(ref ffi) = *self;
        ffi
    }
}

impl Drop for RawError {
    fn drop(&mut self) {
        let RawError::Owned(ref mut ffi) = *self;
        unsafe { ffi::tiledb_error_free(ffi) }
    }
}

impl From<RawError> for CApiError {
    fn from(value: RawError) -> Self {
        let mut c_msg: *const std::os::raw::c_char = out_ptr!();
        let c_ret = unsafe {
            ffi::tiledb_error_message(
                *value,
                &mut c_msg as *mut *const std::os::raw::c_char,
            )
        };
        if c_ret == ffi::TILEDB_OK && !c_msg.is_null() {
            let c_message = unsafe { std::ffi::CStr::from_ptr(c_msg) };
            Self::Error(c_message.to_string_lossy().into_owned())
        } else {
            Self::Internal
        }
    }
}

pub(crate) struct RawContext {
    raw: *mut ffi::tiledb_ctx_t,
}

impl Drop for RawContext {
    fn drop(&mut self) {
        unsafe {
            ffi::tiledb_ctx_free(&mut self.raw as *mut *mut ffi::tiledb_ctx_t);
        }
    }
}

pub trait ContextBound {
    fn context(&self) -> Context;
}

pub trait CApiInterface {
    // The callback is intentionally *not* returning a TileDBResult<i32> as this
    // forces folks to avoid putting anything that can error into the callback.
    // This will hopefully lead to our collective decision to do as minimal
    // work as possible in unsafe blocks.
    fn capi_call<Callable>(&self, action: Callable) -> CApiResult<()>
    where
        Callable: FnOnce(*mut ffi::tiledb_ctx_t) -> i32;
}

impl<T> CApiInterface for T
where
    T: ContextBound,
{
    fn capi_call<Callable>(&self, action: Callable) -> CApiResult<()>
    where
        Callable: FnOnce(*mut ffi::tiledb_ctx_t) -> i32,
    {
        self.context().capi_call(action)
    }
}

#[derive(Clone)]
pub struct Context {
    raw: Rc<RawContext>,
}

impl Context {
    pub fn new() -> Result<Context, CreateContextError> {
        let cfg = Config::new().map_err(CreateContextError::Config)?;
        Context::from_config(&cfg)
    }

    pub fn from_config(cfg: &Config) -> Result<Context, CreateContextError> {
        let mut c_ctx: *mut ffi::tiledb_ctx_t = out_ptr!();
        let res = unsafe { ffi::tiledb_ctx_alloc(cfg.capi(), &mut c_ctx) };
        match res {
            ffi::TILEDB_OK => Ok(Context {
                raw: Rc::new(RawContext { raw: c_ctx }),
            }),
            ffi::TILEDB_OOM => Err(CreateContextError::OutOfMemory),
            ffi::TILEDB_ERR => Err(CreateContextError::Fatal),
            _ => {
                Err(CreateContextError::InternalInvalidReturnValue(res as i64))
            }
        }
    }

    pub fn capi_call<Callable>(&self, action: Callable) -> CApiResult<()>
    where
        Callable: FnOnce(*mut ffi::tiledb_ctx_t) -> i32,
    {
        let c_ret = action(self.raw.raw);
        if c_ret == ffi::TILEDB_OK {
            Ok(())
        } else if let Some(e) = self.get_last_error() {
            Err(e)
        } else {
            panic!(
                "libtiledb context did not have error for error return value: {}",
                c_ret
            )
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

    pub fn get_last_error(&self) -> Option<CApiError> {
        let mut c_err: *mut ffi::tiledb_error_t = out_ptr!();
        let res = self.capi_call(|ctx| unsafe {
            ffi::tiledb_ctx_get_last_error(ctx, &mut c_err)
        });

        if res.is_ok() && !c_err.is_null() {
            Some(CApiError::from(RawError::Owned(c_err)))
        } else {
            None
        }
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

    /// Returns the `ObjectType` for the resource located at the given `uri`,
    /// if any. If there is no resource, returns `None`.
    ///
    /// # Errors
    ///
    /// This function performs I/O operations which may result in a return of `Err`.
    pub fn object_type<S>(&self, uri: S) -> CApiResult<Option<ObjectType>>
    where
        S: AsRef<str>,
    {
        let c_uri = cstring!(uri.as_ref());
        let mut c_objtype: ffi::tiledb_object_t = out_ptr!();

        self.capi_call(|ctx| unsafe {
            ffi::tiledb_object_type(ctx, c_uri.as_ptr(), &mut c_objtype)
        })?;

        // SAFETY: libtiledb only returns TILEDB_OK if it finds a valid tiledb_object_type_t
        let object_type = ObjectType::from_capi(c_objtype).unwrap();
        Ok(object_type)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectType {
    Array,
    Group,
}

impl ObjectType {
    pub(crate) fn from_capi(
        value: ffi::tiledb_object_t,
    ) -> Result<Option<ObjectType>, ObjectTypeError> {
        match value {
            ffi::tiledb_object_t_TILEDB_INVALID => Ok(None),
            ffi::tiledb_object_t_TILEDB_ARRAY => Ok(Some(ObjectType::Array)),
            ffi::tiledb_object_t_TILEDB_GROUP => Ok(Some(ObjectType::Group)),
            other => Err(ObjectTypeError::InvalidDiscriminant(other as u64)),
        }
    }
}

impl Display for ObjectType {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        <Self as Debug>::fmt(self, f)
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum ObjectTypeError {
    #[error("Invalid discriminant for {}: {0}", std::any::type_name::<ObjectType>())]
    InvalidDiscriminant(u64),
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

    #[test]
    fn ctx_object_type_not_found() {
        let ctx = Context::new().expect("Error creating context instance.");
        let obj = ctx.object_type(
            "this_uri_should_not_exist_with_overwhelming_probability",
        );
        assert!(matches!(obj, Ok(None)));

        let obj = ctx.object_type(
            "this_dir_should_not_exist_with_overwhelming_probability/file",
        );
        assert!(matches!(obj, Ok(None)));
    }
}
