use crate::types::capi_return_t;

extern "C" {
    pub fn tiledb_filter_type_to_str(
        filter_type: u32,
        str_: *mut *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_filter_type_from_str(
        str_: *const ::std::os::raw::c_char,
        filter_type: *mut u32,
    ) -> capi_return_t;
}
