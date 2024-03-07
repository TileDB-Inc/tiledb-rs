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

    pub fn get_message(&mut self) -> String {
        let mut msg = std::ptr::null::<std::os::raw::c_char>();
        let res = unsafe {
            ffi::tiledb_error_message(
                self.as_mut_ptr(),
                &mut msg as *mut *const std::os::raw::c_char,
            )
        };
        if res == ffi::TILEDB_OK && !msg.is_null() {
            let c_msg = unsafe { std::ffi::CStr::from_ptr(msg) };
            let msg = String::from(c_msg.to_string_lossy());
            msg
        } else {
            String::from("Failed to retrieve an error message from TileDB.")
        }
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
