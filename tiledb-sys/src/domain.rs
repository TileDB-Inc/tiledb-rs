use crate::datatype::tiledb_datatype_t;
use crate::types::{tiledb_ctx_t, tiledb_dimension_t, tiledb_domain_t};

extern "C" {
    #[doc = " Creates a TileDB domain.\n\n **Example:**\n\n @code{.c}\n tiledb_domain_t* domain;\n tiledb_domain_alloc(ctx, &domain);\n @endcode\n\n @param ctx The TileDB context.\n @param domain The TileDB domain to be created.\n @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error."]
    pub fn tiledb_domain_alloc(
        ctx: *mut tiledb_ctx_t,
        domain: *mut *mut tiledb_domain_t,
    ) -> i32;

    #[doc = " Destroys a TileDB domain, freeing associated memory.\n\n **Example:**\n\n @code{.c}\n tiledb_domain_t* domain;\n tiledb_domain_alloc(ctx, &domain);\n tiledb_domain_free(&domain);\n @endcode\n\n @param domain The domain to be destroyed."]
    pub fn tiledb_domain_free(domain: *mut *mut tiledb_domain_t);

    #[doc = " Retrieves the domain's type.\n\n **Example:**\n\n @code{.c}\n tiledb_datatype_t type;\n tiledb_domain_get_type(ctx, domain, &type);\n @endcode\n\n @param ctx The TileDB context.\n @param domain The domain.\n @param type The type to be retrieved.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_domain_get_type(
        ctx: *mut tiledb_ctx_t,
        domain: *const tiledb_domain_t,
        type_: *mut tiledb_datatype_t,
    ) -> i32;

    #[doc = " Retrieves the number of dimensions in a domain.\n\n **Example:**\n\n @code{.c}\n uint32_t dim_num;\n tiledb_domain_get_ndim(ctx, domain, &dim_num);\n @endcode\n\n @param ctx The TileDB context\n @param domain The domain\n @param ndim The number of dimensions in a domain.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_domain_get_ndim(
        ctx: *mut tiledb_ctx_t,
        domain: *const tiledb_domain_t,
        ndim: *mut u32,
    ) -> i32;

    #[doc = " Adds a dimension to a TileDB domain.\n\n **Example:**\n\n @code{.c}\n tiledb_dimension_t* dim;\n int64_t dim_domain[] = {1, 10};\n int64_t tile_extent = 5;\n tiledb_dimension_alloc(\n     ctx, \"dim_0\", TILEDB_INT64, dim_domain, &tile_extent, &dim);\n tiledb_domain_add_dimension(ctx, domain, dim);\n @endcode\n\n @param ctx The TileDB context.\n @param domain The domain to add the dimension to.\n @param dim The dimension to be added.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_domain_add_dimension(
        ctx: *mut tiledb_ctx_t,
        domain: *mut tiledb_domain_t,
        dim: *mut tiledb_dimension_t,
    ) -> i32;

    #[doc = " Retrieves a dimension object from a domain by index.\n\n **Example:**\n\n The following retrieves the first dimension from a domain.\n\n @code{.c}\n tiledb_dimension_t* dim;\n tiledb_domain_get_dimension_from_index(ctx, domain, 0, &dim);\n @endcode\n\n @param ctx The TileDB context\n @param domain The domain to add the dimension to.\n @param index The index of domain dimension\n @param dim The retrieved dimension object.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_domain_get_dimension_from_index(
        ctx: *mut tiledb_ctx_t,
        domain: *const tiledb_domain_t,
        index: u32,
        dim: *mut *mut tiledb_dimension_t,
    ) -> i32;

    #[doc = " Retrieves a dimension object from a domain by name (key).\n\n **Example:**\n\n @code{.c}\n tiledb_dimension_t* dim;\n tiledb_domain_get_dimension_from_name(ctx, domain, \"dim_0\", &dim);\n @endcode\n\n @param ctx The TileDB context\n @param domain The domain to add the dimension to.\n @param name The name (key) of the requested dimension\n @param dim The retrieved dimension object.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_domain_get_dimension_from_name(
        ctx: *mut tiledb_ctx_t,
        domain: *const tiledb_domain_t,
        name: *const ::std::os::raw::c_char,
        dim: *mut *mut tiledb_dimension_t,
    ) -> i32;

    #[doc = " Checks whether the domain has a dimension of the given name.\n\n **Example:**\n\n @code{.c}\n int32_t has_dim;\n tiledb_domain_has_dimension(ctx, domain, \"dim_0\", &has_dim);\n @endcode\n\n @param ctx The TileDB context.\n @param domain The domain.\n @param name The name of the dimension to check for.\n @param has_dim Set to `1` if the domain has a dimension of the given name,\n      else `0`.\n @return `TILEDB_OK` for success and `TILEDB_ERR` for error."]
    pub fn tiledb_domain_has_dimension(
        ctx: *mut tiledb_ctx_t,
        domain: *const tiledb_domain_t,
        name: *const ::std::os::raw::c_char,
        has_dim: *mut i32,
    ) -> i32;
}
