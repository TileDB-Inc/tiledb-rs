use crate::types::{
    capi_return_t, tiledb_array_schema_evolution_t, tiledb_attribute_t,
    tiledb_ctx_t, tiledb_current_domain_t, tiledb_enumeration_t,
};

extern "C" {
    pub fn tiledb_array_evolve(
        ctx: *mut tiledb_ctx_t,
        array_uri: *const ::std::os::raw::c_char,
        array_schema_evolution: *mut tiledb_array_schema_evolution_t,
    ) -> capi_return_t;

    pub fn tiledb_array_schema_evolution_alloc(
        ctx: *mut tiledb_ctx_t,
        array_schema_evolution: *mut *mut tiledb_array_schema_evolution_t,
    ) -> i32;

    pub fn tiledb_array_schema_evolution_free(
        array_schema_evolution: *mut *mut tiledb_array_schema_evolution_t,
    );

    pub fn tiledb_array_schema_evolution_add_attribute(
        ctx: *mut tiledb_ctx_t,
        array_schema_evolution: *mut tiledb_array_schema_evolution_t,
        attribute: *mut tiledb_attribute_t,
    ) -> i32;

    pub fn tiledb_array_schema_evolution_drop_attribute(
        ctx: *mut tiledb_ctx_t,
        array_schema_evolution: *mut tiledb_array_schema_evolution_t,
        attribute_name: *const ::std::os::raw::c_char,
    ) -> i32;

    pub fn tiledb_array_schema_evolution_add_enumeration(
        ctx: *mut tiledb_ctx_t,
        array_schema_evolution: *mut tiledb_array_schema_evolution_t,
        enumeration: *mut tiledb_enumeration_t,
    ) -> capi_return_t;

    pub fn tiledb_array_schema_evolution_extend_enumeration(
        ctx: *mut tiledb_ctx_t,
        array_schema_evolution: *mut tiledb_array_schema_evolution_t,
        enumeration: *mut tiledb_enumeration_t,
    ) -> capi_return_t;

    pub fn tiledb_array_schema_evolution_drop_enumeration(
        ctx: *mut tiledb_ctx_t,
        array_schema_evolution: *mut tiledb_array_schema_evolution_t,
        enumeration_name: *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_array_schema_evolution_set_timestamp_range(
        ctx: *mut tiledb_ctx_t,
        array_schema_evolution: *mut tiledb_array_schema_evolution_t,
        lo: u64,
        hi: u64,
    ) -> i32;

    pub fn tiledb_array_schema_evolution_expand_current_domain(
        ctx: *mut tiledb_ctx_t,
        array_schema_evolution: *mut tiledb_array_schema_evolution_t,
        expanded_domain: *mut tiledb_current_domain_t,
    ) -> capi_return_t;
}
