use crate::types::{
    capi_return_t, tiledb_config_t, tiledb_ctx_t, tiledb_vfs_fh_t, tiledb_vfs_t,
};

#[repr(C)]
pub enum VFSMode {
    Read = 0,
    Write = 1,
    Append = 2,
}

pub type LSCallback = ::std::option::Option<
    unsafe extern "C" fn(
        path: *const ::std::os::raw::c_char,
        callback_data: *mut ::std::os::raw::c_void,
    ) -> i32,
>;

pub type LSRecursiveCallback = ::std::option::Option<
    unsafe extern "C" fn(
        path: *const std::os::raw::c_uchar,
        path_len: usize,
        object_size: u64,
        callback_data: *mut ::std::os::raw::c_void,
    ) -> i32,
>;

extern "C" {
    pub fn tiledb_vfs_mode_to_str(
        vfs_mode: VFSMode,
        str_: *mut *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_vfs_mode_from_str(
        str_: *const ::std::os::raw::c_char,
        vfs_mode: *mut VFSMode,
    ) -> capi_return_t;

    pub fn tiledb_vfs_alloc(
        ctx: *mut tiledb_ctx_t,
        config: *mut tiledb_config_t,
        vfs: *mut *mut tiledb_vfs_t,
    ) -> capi_return_t;

    pub fn tiledb_vfs_free(vfs: *mut *mut tiledb_vfs_t);

    pub fn tiledb_vfs_get_config(
        ctx: *mut tiledb_ctx_t,
        vfs: *mut tiledb_vfs_t,
        config: *mut *mut tiledb_config_t,
    ) -> capi_return_t;

    pub fn tiledb_vfs_create_bucket(
        ctx: *mut tiledb_ctx_t,
        vfs: *mut tiledb_vfs_t,
        uri: *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_vfs_remove_bucket(
        ctx: *mut tiledb_ctx_t,
        vfs: *mut tiledb_vfs_t,
        uri: *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_vfs_empty_bucket(
        ctx: *mut tiledb_ctx_t,
        vfs: *mut tiledb_vfs_t,
        uri: *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_vfs_is_empty_bucket(
        ctx: *mut tiledb_ctx_t,
        vfs: *mut tiledb_vfs_t,
        uri: *const ::std::os::raw::c_char,
        is_empty: *mut i32,
    ) -> capi_return_t;

    pub fn tiledb_vfs_is_bucket(
        ctx: *mut tiledb_ctx_t,
        vfs: *mut tiledb_vfs_t,
        uri: *const ::std::os::raw::c_char,
        is_bucket: *mut i32,
    ) -> capi_return_t;

    pub fn tiledb_vfs_is_dir(
        ctx: *mut tiledb_ctx_t,
        vfs: *mut tiledb_vfs_t,
        uri: *const ::std::os::raw::c_char,
        is_dir: *mut i32,
    ) -> capi_return_t;

    pub fn tiledb_vfs_dir_size(
        ctx: *mut tiledb_ctx_t,
        vfs: *mut tiledb_vfs_t,
        uri: *const ::std::os::raw::c_char,
        size: *mut u64,
    ) -> capi_return_t;

    pub fn tiledb_vfs_create_dir(
        ctx: *mut tiledb_ctx_t,
        vfs: *mut tiledb_vfs_t,
        uri: *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_vfs_remove_dir(
        ctx: *mut tiledb_ctx_t,
        vfs: *mut tiledb_vfs_t,
        uri: *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_vfs_copy_dir(
        ctx: *mut tiledb_ctx_t,
        vfs: *mut tiledb_vfs_t,
        old_uri: *const ::std::os::raw::c_char,
        new_uri: *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_vfs_move_dir(
        ctx: *mut tiledb_ctx_t,
        vfs: *mut tiledb_vfs_t,
        old_uri: *const ::std::os::raw::c_char,
        new_uri: *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_vfs_is_file(
        ctx: *mut tiledb_ctx_t,
        vfs: *mut tiledb_vfs_t,
        uri: *const ::std::os::raw::c_char,
        is_file: *mut i32,
    ) -> capi_return_t;

    pub fn tiledb_vfs_file_size(
        ctx: *mut tiledb_ctx_t,
        vfs: *mut tiledb_vfs_t,
        uri: *const ::std::os::raw::c_char,
        size: *mut u64,
    ) -> capi_return_t;

    pub fn tiledb_vfs_touch(
        ctx: *mut tiledb_ctx_t,
        vfs: *mut tiledb_vfs_t,
        uri: *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_vfs_remove_file(
        ctx: *mut tiledb_ctx_t,
        vfs: *mut tiledb_vfs_t,
        uri: *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_vfs_copy_file(
        ctx: *mut tiledb_ctx_t,
        vfs: *mut tiledb_vfs_t,
        old_uri: *const ::std::os::raw::c_char,
        new_uri: *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_vfs_move_file(
        ctx: *mut tiledb_ctx_t,
        vfs: *mut tiledb_vfs_t,
        old_uri: *const ::std::os::raw::c_char,
        new_uri: *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_vfs_ls(
        ctx: *mut tiledb_ctx_t,
        vfs: *mut tiledb_vfs_t,
        path: *const ::std::os::raw::c_char,
        callback: LSCallback,
        data: *mut ::std::os::raw::c_void,
    ) -> capi_return_t;

    pub fn tiledb_vfs_ls_recursive(
        ctx: *mut tiledb_ctx_t,
        vfs: *mut tiledb_vfs_t,
        path: *const ::std::os::raw::c_char,
        callback: LSRecursiveCallback,
        data: *mut ::std::os::raw::c_void,
    ) -> capi_return_t;

    pub fn tiledb_vfs_open(
        ctx: *mut tiledb_ctx_t,
        vfs: *mut tiledb_vfs_t,
        uri: *const ::std::os::raw::c_char,
        mode: VFSMode,
        fh: *mut *mut tiledb_vfs_fh_t,
    ) -> capi_return_t;

    pub fn tiledb_vfs_close(
        ctx: *mut tiledb_ctx_t,
        fh: *mut tiledb_vfs_fh_t,
    ) -> capi_return_t;

    pub fn tiledb_vfs_read(
        ctx: *mut tiledb_ctx_t,
        fh: *mut tiledb_vfs_fh_t,
        offset: u64,
        buffer: *mut ::std::os::raw::c_void,
        nbytes: u64,
    ) -> capi_return_t;

    pub fn tiledb_vfs_write(
        ctx: *mut tiledb_ctx_t,
        fh: *mut tiledb_vfs_fh_t,
        buffer: *const ::std::os::raw::c_void,
        nbytes: u64,
    ) -> capi_return_t;

    pub fn tiledb_vfs_sync(
        ctx: *mut tiledb_ctx_t,
        fh: *mut tiledb_vfs_fh_t,
    ) -> capi_return_t;

    pub fn tiledb_vfs_fh_free(fh: *mut *mut tiledb_vfs_fh_t);

    pub fn tiledb_vfs_fh_is_closed(
        ctx: *mut tiledb_ctx_t,
        fh: *mut tiledb_vfs_fh_t,
        is_closed: *mut i32,
    ) -> capi_return_t;
}
