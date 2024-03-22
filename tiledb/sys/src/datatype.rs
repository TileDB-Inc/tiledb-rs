use crate::types::capi_return_t;

pub const tiledb_datatype_t_TILEDB_INT32: tiledb_datatype_t = 0;
pub const tiledb_datatype_t_TILEDB_INT64: tiledb_datatype_t = 1;
pub const tiledb_datatype_t_TILEDB_FLOAT32: tiledb_datatype_t = 2;
pub const tiledb_datatype_t_TILEDB_FLOAT64: tiledb_datatype_t = 3;
pub const tiledb_datatype_t_TILEDB_CHAR: tiledb_datatype_t = 4;
pub const tiledb_datatype_t_TILEDB_INT8: tiledb_datatype_t = 5;
pub const tiledb_datatype_t_TILEDB_UINT8: tiledb_datatype_t = 6;
pub const tiledb_datatype_t_TILEDB_INT16: tiledb_datatype_t = 7;
pub const tiledb_datatype_t_TILEDB_UINT16: tiledb_datatype_t = 8;
pub const tiledb_datatype_t_TILEDB_UINT32: tiledb_datatype_t = 9;
pub const tiledb_datatype_t_TILEDB_UINT64: tiledb_datatype_t = 10;
pub const tiledb_datatype_t_TILEDB_STRING_ASCII: tiledb_datatype_t = 11;
pub const tiledb_datatype_t_TILEDB_STRING_UTF8: tiledb_datatype_t = 12;
pub const tiledb_datatype_t_TILEDB_STRING_UTF16: tiledb_datatype_t = 13;
pub const tiledb_datatype_t_TILEDB_STRING_UTF32: tiledb_datatype_t = 14;
pub const tiledb_datatype_t_TILEDB_STRING_UCS2: tiledb_datatype_t = 15;
pub const tiledb_datatype_t_TILEDB_STRING_UCS4: tiledb_datatype_t = 16;
pub const tiledb_datatype_t_TILEDB_ANY: tiledb_datatype_t = 17;
pub const tiledb_datatype_t_TILEDB_DATETIME_YEAR: tiledb_datatype_t = 18;
pub const tiledb_datatype_t_TILEDB_DATETIME_MONTH: tiledb_datatype_t = 19;
pub const tiledb_datatype_t_TILEDB_DATETIME_WEEK: tiledb_datatype_t = 20;
pub const tiledb_datatype_t_TILEDB_DATETIME_DAY: tiledb_datatype_t = 21;
pub const tiledb_datatype_t_TILEDB_DATETIME_HR: tiledb_datatype_t = 22;
pub const tiledb_datatype_t_TILEDB_DATETIME_MIN: tiledb_datatype_t = 23;
pub const tiledb_datatype_t_TILEDB_DATETIME_SEC: tiledb_datatype_t = 24;
pub const tiledb_datatype_t_TILEDB_DATETIME_MS: tiledb_datatype_t = 25;
pub const tiledb_datatype_t_TILEDB_DATETIME_US: tiledb_datatype_t = 26;
pub const tiledb_datatype_t_TILEDB_DATETIME_NS: tiledb_datatype_t = 27;
pub const tiledb_datatype_t_TILEDB_DATETIME_PS: tiledb_datatype_t = 28;
pub const tiledb_datatype_t_TILEDB_DATETIME_FS: tiledb_datatype_t = 29;
pub const tiledb_datatype_t_TILEDB_DATETIME_AS: tiledb_datatype_t = 30;
pub const tiledb_datatype_t_TILEDB_TIME_HR: tiledb_datatype_t = 31;
pub const tiledb_datatype_t_TILEDB_TIME_MIN: tiledb_datatype_t = 32;
pub const tiledb_datatype_t_TILEDB_TIME_SEC: tiledb_datatype_t = 33;
pub const tiledb_datatype_t_TILEDB_TIME_MS: tiledb_datatype_t = 34;
pub const tiledb_datatype_t_TILEDB_TIME_US: tiledb_datatype_t = 35;
pub const tiledb_datatype_t_TILEDB_TIME_NS: tiledb_datatype_t = 36;
pub const tiledb_datatype_t_TILEDB_TIME_PS: tiledb_datatype_t = 37;
pub const tiledb_datatype_t_TILEDB_TIME_FS: tiledb_datatype_t = 38;
pub const tiledb_datatype_t_TILEDB_TIME_AS: tiledb_datatype_t = 39;
pub const tiledb_datatype_t_TILEDB_BLOB: tiledb_datatype_t = 40;
pub const tiledb_datatype_t_TILEDB_BOOL: tiledb_datatype_t = 41;
pub const tiledb_datatype_t_TILEDB_GEOM_WKB: tiledb_datatype_t = 42;
pub const tiledb_datatype_t_TILEDB_GEOM_WKT: tiledb_datatype_t = 43;
pub type tiledb_datatype_t = ::std::os::raw::c_uint;

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
