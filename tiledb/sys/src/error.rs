use crate::types::{capi_return_t, tiledb_error_t};

extern "C" {
    pub fn tiledb_error_message(
        err: *mut tiledb_error_t,
        errmsg: *mut *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_error_free(err: *mut *mut tiledb_error_t);
}
