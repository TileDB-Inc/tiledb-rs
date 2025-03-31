use crate::types::{capi_return_t, tiledb_string_t};

unsafe extern "C" {
    pub fn tiledb_string_view(
        s: *mut tiledb_string_t,
        data: *mut *const ::std::os::raw::c_char,
        length: *mut usize,
    ) -> capi_return_t;

    pub fn tiledb_string_free(s: *mut *mut tiledb_string_t) -> capi_return_t;
}
