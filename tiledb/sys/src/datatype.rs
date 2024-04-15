use crate::capi_enum::tiledb_datatype_t;
use crate::types::capi_return_t;

extern "C" {
    pub fn tiledb_datatype_to_str(
        datatype: tiledb_datatype_t,
        str_: *mut *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_datatype_from_str(
        str_: *const ::std::os::raw::c_char,
        datatype: *mut tiledb_datatype_t,
    ) -> capi_return_t;

    pub fn tiledb_datatype_size(type_: tiledb_datatype_t) -> u64;
}
