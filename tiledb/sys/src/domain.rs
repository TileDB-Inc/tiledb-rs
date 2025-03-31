use crate::capi_enum::tiledb_datatype_t;
use crate::types::{tiledb_ctx_t, tiledb_dimension_t, tiledb_domain_t};

unsafe extern "C" {
    pub fn tiledb_domain_alloc(
        ctx: *mut tiledb_ctx_t,
        domain: *mut *mut tiledb_domain_t,
    ) -> i32;

    pub fn tiledb_domain_free(domain: *mut *mut tiledb_domain_t);

    pub fn tiledb_domain_get_type(
        ctx: *mut tiledb_ctx_t,
        domain: *const tiledb_domain_t,
        type_: *mut tiledb_datatype_t,
    ) -> i32;

    pub fn tiledb_domain_get_ndim(
        ctx: *mut tiledb_ctx_t,
        domain: *const tiledb_domain_t,
        ndim: *mut u32,
    ) -> i32;

    pub fn tiledb_domain_add_dimension(
        ctx: *mut tiledb_ctx_t,
        domain: *mut tiledb_domain_t,
        dim: *mut tiledb_dimension_t,
    ) -> i32;

    pub fn tiledb_domain_get_dimension_from_index(
        ctx: *mut tiledb_ctx_t,
        domain: *const tiledb_domain_t,
        index: u32,
        dim: *mut *mut tiledb_dimension_t,
    ) -> i32;

    pub fn tiledb_domain_get_dimension_from_name(
        ctx: *mut tiledb_ctx_t,
        domain: *const tiledb_domain_t,
        name: *const ::std::os::raw::c_char,
        dim: *mut *mut tiledb_dimension_t,
    ) -> i32;

    pub fn tiledb_domain_has_dimension(
        ctx: *mut tiledb_ctx_t,
        domain: *const tiledb_domain_t,
        name: *const ::std::os::raw::c_char,
        has_dim: *mut i32,
    ) -> i32;
}
