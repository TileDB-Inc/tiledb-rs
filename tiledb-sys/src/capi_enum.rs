#[doc = " Array type."]
pub type tiledb_array_type_t = ::std::os::raw::c_uint;
#[doc = " Dense array"]
pub const tiledb_array_type_t_TILEDB_DENSE: tiledb_array_type_t = 0;
#[doc = " Sparse array"]
pub const tiledb_array_type_t_TILEDB_SPARSE: tiledb_array_type_t = 1;

extern "C" {
    #[doc = " Returns a string representation of the given array type.\n\n @param array_type Array type\n @param str Set to point to a constant string representation of the array type\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_type_to_str(
        array_type: tiledb_array_type_t,
        str_: *mut *const ::std::os::raw::c_char,
    ) -> i32;
    #[doc = " Parses a array type from the given string.\n\n @param str String representation to parse\n @param array_type Set to the parsed array type\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_array_type_from_str(
        str_: *const ::std::os::raw::c_char,
        array_type: *mut tiledb_array_type_t,
    ) -> i32;
}

#[doc = " TileDB query type."]
pub type tiledb_query_type_t = ::std::os::raw::c_uint;
#[doc = " Read query"]
pub const tiledb_query_type_t_TILEDB_READ: tiledb_query_type_t = 0;
#[doc = " Write query"]
pub const tiledb_query_type_t_TILEDB_WRITE: tiledb_query_type_t = 1;
#[doc = " Delete query"]
pub const tiledb_query_type_t_TILEDB_DELETE: tiledb_query_type_t = 2;
#[doc = " Update query"]
pub const tiledb_query_type_t_TILEDB_UPDATE: tiledb_query_type_t = 3;
#[doc = " Exclusive Modification query"]
pub const tiledb_query_type_t_TILEDB_MODIFY_EXCLUSIVE: tiledb_query_type_t = 4;
