#[cxx::bridge(namespace = "tiledb::rs")]
mod ffi {
    unsafe extern "C++" {
        include!("tiledb-sys2/cpp/domain.h");

        type Domain;
        type DomainBuilder;

        type Context = crate::context::Context;
        type Datatype = crate::datatype::FFIDatatype;
        type Dimension = crate::dimension::Dimension;

        pub fn datatype(self: &Domain) -> Result<Datatype>;

        pub fn num_dimensions(self: &Domain) -> Result<u32>;

        pub fn dimension_from_index(
            self: &Domain,
            idx: u32,
        ) -> Result<SharedPtr<Dimension>>;

        pub fn dimension_from_name(
            self: &Domain,
            name: &str,
        ) -> Result<SharedPtr<Dimension>>;

        pub fn has_dimension(self: &Domain, name: &str) -> Result<bool>;

        pub fn create_domain_builder(
            ctx: SharedPtr<Context>,
        ) -> Result<SharedPtr<DomainBuilder>>;

        pub fn build(self: &DomainBuilder) -> Result<SharedPtr<Domain>>;

        pub fn add_dimension(
            self: &DomainBuilder,
            dim: SharedPtr<Dimension>,
        ) -> Result<()>;
    }
}

pub use ffi::*;
