use crate::capi_enum::{tiledb_object_t, tiledb_walk_order_t};
use crate::types::tiledb_ctx_t;

unsafe extern "C" {
    pub fn tiledb_object_type(
        ctx: *mut tiledb_ctx_t,
        path: *const ::std::os::raw::c_char,
        type_: *mut tiledb_object_t,
    ) -> i32;

    pub fn tiledb_object_remove(
        ctx: *mut tiledb_ctx_t,
        path: *const ::std::os::raw::c_char,
    ) -> i32;

    pub fn tiledb_object_move(
        ctx: *mut tiledb_ctx_t,
        old_path: *const ::std::os::raw::c_char,
        new_path: *const ::std::os::raw::c_char,
    ) -> i32;

    pub fn tiledb_object_walk(
        ctx: *mut tiledb_ctx_t,
        path: *const ::std::os::raw::c_char,
        order: tiledb_walk_order_t,
        callback: ::std::option::Option<
            unsafe extern "C" fn(
                arg1: *const ::std::os::raw::c_char,
                arg2: tiledb_object_t,
                arg3: *mut ::std::os::raw::c_void,
            ) -> i32,
        >,
        data: *mut ::std::os::raw::c_void,
    ) -> i32;

    pub fn tiledb_object_ls(
        ctx: *mut tiledb_ctx_t,
        path: *const ::std::os::raw::c_char,
        callback: ::std::option::Option<
            unsafe extern "C" fn(
                arg1: *const ::std::os::raw::c_char,
                arg2: tiledb_object_t,
                arg3: *mut ::std::os::raw::c_void,
            ) -> i32,
        >,
        data: *mut ::std::os::raw::c_void,
    ) -> i32;
}
