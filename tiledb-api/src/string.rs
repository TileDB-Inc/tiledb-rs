extern crate tiledb_sys as ffi;

pub struct String {
    _wrapped: *mut ffi::tiledb_string_t,
}

impl Default for String {
    fn default() -> String {
        Self {
            _wrapped: std::ptr::null_mut::<ffi::tiledb_string_t>(),
        }
    }
}

impl Drop for String {
    fn drop(&mut self) {
        if self._wrapped.is_null() {
            return;
        }
        unsafe {
            ffi::tiledb_string_free(&mut self._wrapped);
        }
    }
}
