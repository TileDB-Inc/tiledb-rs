#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

pub type capi_return_t = i32;
pub type capi_status_t = i32;

pub const TILEDB_OK: i32 = 0;
pub const TILEDB_ERR: i32 = -1;
pub const TILEDB_OOM: i32 = -2;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct tiledb_config_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct tiledb_error_t {
    _unused: [u8; 0],
}

extern "C" {
    pub fn tiledb_error_message(
        err: *mut tiledb_error_t,
        errmsg: *mut *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_error_free(err: *mut *mut tiledb_error_t);

    pub fn tiledb_config_alloc(
        config: *mut *mut tiledb_config_t,
        error: *mut *mut tiledb_error_t,
    ) -> capi_return_t;

    pub fn tiledb_config_free(config: *mut *mut tiledb_config_t);
}
