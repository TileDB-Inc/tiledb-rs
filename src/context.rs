extern crate tiledb_sys as ffi;

use crate::config::Config;
//use crate::error::Error;

pub struct Context {
    _wrapped: *mut ffi::tiledb_ctx_t,
}

impl Context {
    pub fn new() -> Result<Context, String> {
        let cfg = Config::new().expect("Error creating config instance.");
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
}

impl Drop for Context {
    fn drop(&mut self) {
        if self._wrapped.is_null() {
            return;
        }
        unsafe { ffi::tiledb_ctx_free(&mut self._wrapped) }
    }
}
