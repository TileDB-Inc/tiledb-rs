use crate::capi_enum::tiledb_filesystem_t;
use crate::types::capi_return_t;

unsafe extern "C" {
    pub fn tiledb_filesystem_to_str(
        filesystem: tiledb_filesystem_t,
        str_: *mut *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_filesystem_from_str(
        str_: *const ::std::os::raw::c_char,
        filesystem: *mut tiledb_filesystem_t,
    ) -> capi_return_t;
}
