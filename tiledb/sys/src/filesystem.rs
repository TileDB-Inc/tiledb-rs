use crate::types::capi_return_t;

extern "C" {
    pub fn tiledb_filesystem_to_str(
        filesystem: u32,
        str_: *mut *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_filesystem_from_str(
        str_: *const ::std::os::raw::c_char,
        filesystem: *mut u32,
    ) -> capi_return_t;
}
