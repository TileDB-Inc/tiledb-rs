extern crate tiledb_sys as ffi;

pub struct Error {
    _wrapped: *mut ffi::tiledb_error_t,
}

impl Error {
    pub fn as_mut_ptr(&mut self) -> *mut ffi::tiledb_error_t {
        self._wrapped
    }

    pub fn as_mut_ptr_ptr(&mut self) -> *mut *mut ffi::tiledb_error_t {
        &mut self._wrapped
    }

    pub fn is_null(&self) -> bool {
        self._wrapped.is_null()
    }
}

impl Default for Error {
    fn default() -> Self {
        Self {
            _wrapped: std::ptr::null_mut::<ffi::tiledb_error_t>(),
        }
    }
}

impl Drop for Error {
    fn drop(&mut self) {
        if self._wrapped.is_null() {
            return;
        }
        unsafe { ffi::tiledb_error_free(self.as_mut_ptr_ptr()) }
    }
}
