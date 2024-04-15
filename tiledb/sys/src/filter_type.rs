use crate::capi_enum::tiledb_filter_type_t;
use crate::types::capi_return_t;

extern "C" {
    pub fn tiledb_filter_type_to_str(
        filter_type: tiledb_filter_type_t,
        str_: *mut *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_filter_type_from_str(
        str_: *const ::std::os::raw::c_char,
        filter_type: *mut tiledb_filter_type_t,
    ) -> capi_return_t;
}
