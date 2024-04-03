use crate::capi_enum::tiledb_datatype_t;
use crate::types::{
    capi_return_t, tiledb_ctx_t, tiledb_enumeration_t, tiledb_string_t,
};

extern "C" {
    pub fn tiledb_enumeration_alloc(
        ctx: *mut tiledb_ctx_t,
        name: *const ::std::os::raw::c_char,
        type_: tiledb_datatype_t,
        cell_val_num: u32,
        ordered: ::std::os::raw::c_int,
        data: *const ::std::os::raw::c_void,
        data_size: u64,
        offsets: *const ::std::os::raw::c_void,
        offsets_size: u64,
        enumeration: *mut *mut tiledb_enumeration_t,
    ) -> capi_return_t;

    pub fn tiledb_enumeration_free(enumeration: *mut *mut tiledb_enumeration_t);

    pub fn tiledb_enumeration_extend(
        ctx: *mut tiledb_ctx_t,
        old_enumeration: *mut tiledb_enumeration_t,
        data: *const ::std::os::raw::c_void,
        data_size: u64,
        offsets: *const ::std::os::raw::c_void,
        offsets_size: u64,
        new_enumeration: *mut *mut tiledb_enumeration_t,
    ) -> capi_return_t;

    pub fn tiledb_enumeration_get_name(
        ctx: *mut tiledb_ctx_t,
        enumeration: *mut tiledb_enumeration_t,
        name: *mut *mut tiledb_string_t,
    ) -> capi_return_t;

    pub fn tiledb_enumeration_get_type(
        ctx: *mut tiledb_ctx_t,
        enumeration: *mut tiledb_enumeration_t,
        type_: *mut tiledb_datatype_t,
    ) -> capi_return_t;

    pub fn tiledb_enumeration_get_cell_val_num(
        ctx: *mut tiledb_ctx_t,
        enumeration: *mut tiledb_enumeration_t,
        cell_val_num: *mut u32,
    ) -> capi_return_t;

    pub fn tiledb_enumeration_get_ordered(
        ctx: *mut tiledb_ctx_t,
        enumeration: *mut tiledb_enumeration_t,
        ordered: *mut ::std::os::raw::c_int,
    ) -> capi_return_t;

    pub fn tiledb_enumeration_get_data(
        ctx: *mut tiledb_ctx_t,
        enumeration: *mut tiledb_enumeration_t,
        data: *mut *const ::std::os::raw::c_void,
        data_size: *mut u64,
    ) -> capi_return_t;

    pub fn tiledb_enumeration_get_offsets(
        ctx: *mut tiledb_ctx_t,
        enumeration: *mut tiledb_enumeration_t,
        offsets: *mut *const ::std::os::raw::c_void,
        offsets_size: *mut u64,
    ) -> capi_return_t;
}
