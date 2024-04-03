use crate::types::capi_return_t;

extern "C" {
    pub fn tiledb_filter_option_to_str(
        filter_option: u32,
        str_: *mut *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_filter_option_from_str(
        str_: *const ::std::os::raw::c_char,
        filter_option: *mut u32,
    ) -> capi_return_t;
}
