use crate::capi_enum::tiledb_encryption_type_t;
use crate::capi_return_t;

unsafe extern "C" {
    pub fn tiledb_encryption_type_to_str(
        encryption_type: tiledb_encryption_type_t,
        str_: *mut *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_encryption_type_from_str(
        str_: *const ::std::os::raw::c_char,
        encryption_type: *mut tiledb_encryption_type_t,
    ) -> capi_return_t;
}
