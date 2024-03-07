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
pub struct tiledb_config_iter_t {
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
}

extern "C" {
    pub fn tiledb_config_alloc(
        config: *mut *mut tiledb_config_t,
        error: *mut *mut tiledb_error_t,
    ) -> capi_return_t;

    pub fn tiledb_config_free(config: *mut *mut tiledb_config_t);

    pub fn tiledb_config_set(
        config: *mut tiledb_config_t,
        param: *const ::std::os::raw::c_char,
        value: *const ::std::os::raw::c_char,
        error: *mut *mut tiledb_error_t,
    ) -> capi_return_t;

    pub fn tiledb_config_get(
        config: *mut tiledb_config_t,
        param: *const ::std::os::raw::c_char,
        value: *mut *const ::std::os::raw::c_char,
        error: *mut *mut tiledb_error_t,
    ) -> capi_return_t;

    pub fn tiledb_config_unset(
        config: *mut tiledb_config_t,
        param: *const ::std::os::raw::c_char,
        error: *mut *mut tiledb_error_t,
    ) -> capi_return_t;

    pub fn tiledb_config_load_from_file(
        config: *mut tiledb_config_t,
        filename: *const ::std::os::raw::c_char,
        error: *mut *mut tiledb_error_t,
    ) -> capi_return_t;

    pub fn tiledb_config_save_to_file(
        config: *mut tiledb_config_t,
        filename: *const ::std::os::raw::c_char,
        error: *mut *mut tiledb_error_t,
    ) -> capi_return_t;

    pub fn tiledb_config_compare(
        lhs: *mut tiledb_config_t,
        rhs: *mut tiledb_config_t,
        equal: *mut u8,
    ) -> capi_return_t;

    pub fn tiledb_config_iter_alloc(
        config: *mut tiledb_config_t,
        prefix: *const ::std::os::raw::c_char,
        config_iter: *mut *mut tiledb_config_iter_t,
        error: *mut *mut tiledb_error_t,
    ) -> capi_return_t;

    // Not used.
    // pub fn tiledb_config_iter_reset(
    //     config: *mut tiledb_config_t,
    //     config_iter: *mut tiledb_config_iter_t,
    //     prefix: *const ::std::os::raw::c_char,
    //     error: *mut *mut tiledb_error_t,
    // ) -> capi_return_t;

    pub fn tiledb_config_iter_free(config_iter: *mut *mut tiledb_config_iter_t);

    pub fn tiledb_config_iter_here(
        config_iter: *mut tiledb_config_iter_t,
        param: *mut *const ::std::os::raw::c_char,
        value: *mut *const ::std::os::raw::c_char,
        error: *mut *mut tiledb_error_t,
    ) -> capi_return_t;

    pub fn tiledb_config_iter_next(
        config_iter: *mut tiledb_config_iter_t,
        error: *mut *mut tiledb_error_t,
    ) -> capi_return_t;

    pub fn tiledb_config_iter_done(
        config_iter: *mut tiledb_config_iter_t,
        done: *mut i32,
        error: *mut *mut tiledb_error_t,
    ) -> capi_return_t;
}
