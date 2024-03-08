#![allow(non_camel_case_types)]

pub type capi_return_t = i32;
pub type capi_status_t = i32;

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
pub struct tiledb_ctx_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct tiledb_error_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct tiledb_filter_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct tiledb_filter_list_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct tiledb_string_t {
    _unused: [u8; 0],
}
