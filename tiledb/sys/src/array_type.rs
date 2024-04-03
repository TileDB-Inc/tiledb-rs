use crate::capi_enum::tiledb_array_type_t;

extern "C" {
    pub fn tiledb_array_type_to_str(
        array_type: tiledb_array_type_t,
        str_: *mut *const ::std::os::raw::c_char,
    ) -> i32;

    pub fn tiledb_array_type_from_str(
        str_: *const ::std::os::raw::c_char,
        array_type: *mut tiledb_array_type_t,
    ) -> i32;
}
