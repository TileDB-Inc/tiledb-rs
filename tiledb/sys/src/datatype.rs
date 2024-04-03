use crate::types::capi_return_t;

extern "C" {
    pub fn tiledb_datatype_to_str(
        datatype: u32,
        str_: *mut *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_datatype_from_str(
        str_: *const ::std::os::raw::c_char,
        datatype: *mut u32,
    ) -> capi_return_t;

    pub fn tiledb_datatype_size(type_: u32) -> u64;
}
