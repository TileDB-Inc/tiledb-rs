use crate::capi_enum::{tiledb_object_t, tiledb_walk_order_t};
use crate::types::tiledb_ctx_t;

extern "C" {
    #[doc = " Returns the TileDB object type for a given resource path.\n\n **Example:**\n\n @code{.c}\n tiledb_object_t type;\n tiledb_object_type(ctx, \"arrays/my_array\", &type);\n @endcode\n\n @param ctx The TileDB context.\n @param path The URI path to the TileDB resource.\n @param type The type to be retrieved.\n @return `TILEDB_OK` on success, `TILEDB_ERR` on error."]
    pub fn tiledb_object_type(
        ctx: *mut tiledb_ctx_t,
        path: *const ::std::os::raw::c_char,
        type_: *mut tiledb_object_t,
    ) -> i32;

    #[doc = " Deletes a TileDB resource (group, array, key-value).\n\n **Example:**\n\n @code{.c}\n tiledb_object_remove(ctx, \"arrays/my_array\");\n @endcode\n\n @param ctx The TileDB context.\n @param path The URI path to the tiledb resource.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_object_remove(
        ctx: *mut tiledb_ctx_t,
        path: *const ::std::os::raw::c_char,
    ) -> i32;

    #[doc = " Moves a TileDB resource (group, array, key-value).\n\n **Example:**\n\n @code{.c}\n tiledb_object_move(ctx, \"arrays/my_array\", \"arrays/my_array_2\");\n @endcode\n\n @param ctx The TileDB context.\n @param old_path The old TileDB directory.\n @param new_path The new TileDB directory.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_object_move(
        ctx: *mut tiledb_ctx_t,
        old_path: *const ::std::os::raw::c_char,
        new_path: *const ::std::os::raw::c_char,
    ) -> i32;

    #[doc = " Walks (iterates) over the TileDB objects contained in *path*. The traversal\n is done recursively in the order defined by the user. The user provides\n a callback function which is applied on each of the visited TileDB objects.\n The iteration continues for as long the callback returns non-zero, and stops\n when the callback returns 0. Note that this function ignores any object\n (e.g., file or directory) that is not TileDB-related.\n\n **Example:**\n\n @code{.c}\n tiledb_object_walk(ctx, \"arrays\", TILEDB_PREORDER, NULL, NULL);\n @endcode\n\n @param ctx The TileDB context.\n @param path The path in which the traversal will occur.\n @param order The order of the recursive traversal (e.g., pre-order or\n     post-order.\n @param callback The callback function to be applied on every visited object.\n     The callback should return `0` if the iteration must stop, and `1`\n     if the iteration must continue. It takes as input the currently visited\n     path, the type of that path (e.g., array or group), and the data\n     provided by the user for the callback. The callback returns `-1` upon\n     error. Note that `path` in the callback will be an **absolute** path.\n @param data The data passed in the callback as the last argument.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
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

    #[doc = " Similar to `tiledb_walk`, but now the function visits only the children of\n `path` (i.e., it does not recursively continue to the children directories).\n\n **Example:**\n\n @code{.c}\n tiledb_object_ls(ctx, \"arrays\", NULL, NULL);\n @endcode\n\n @param ctx The TileDB context.\n @param path The path in which the traversal will occur.\n @param callback The callback function to be applied on every visited object.\n     The callback should return `0` if the iteration must stop, and `1`\n     if the iteration must continue. It takes as input the currently visited\n     path, the type of that path (e.g., array or group), and the data\n     provided by the user for the callback. The callback returns `-1` upon\n     error. Note that `path` in the callback will be an **absolute** path.\n @param data The data passed in the callback as the last argument.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
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
