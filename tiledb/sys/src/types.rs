#![allow(non_camel_case_types)]

pub type capi_return_t = i32;
pub type capi_status_t = i32;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct tiledb_array_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct tiledb_array_schema_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct tiledb_attribute_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct tiledb_config_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct tiledb_config_iter_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct tiledb_ctx_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct tiledb_dimension_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct tiledb_domain_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct tiledb_enumeration_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct tiledb_error_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct tiledb_filter_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct tiledb_filter_list_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct tiledb_fragment_info_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct tiledb_query_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct tiledb_query_condition_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct tiledb_string_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct tiledb_subarray_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct tiledb_vfs_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct tiledb_vfs_fh_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct tiledb_group_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct tiledb_query_channel_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct tiledb_channel_operation_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct tiledb_channel_operator_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct tiledb_array_schema_evolution_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct tiledb_current_domain_t {
    _unused: [u8; 0],
}
