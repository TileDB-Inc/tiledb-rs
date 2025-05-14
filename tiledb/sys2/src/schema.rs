#[cxx::bridge(namespace = "tiledb::rs")]
mod ffi {
    unsafe extern "C++" {
        include!("tiledb-sys2/cpp/schema.h");

        type Schema;
        type SchemaBuilder;

        type ArrayType = crate::array_type::FFIArrayType;
        type Attribute = crate::attribute::Attribute;
        type CellOrder = crate::layout::FFICellOrder;
        type Context = crate::context::Context;
        type Domain = crate::domain::Domain;
        type Enumeration = crate::enumeration::Enumeration;
        type FilterList = crate::filter_list::FilterList;
        type TileOrder = crate::layout::FFITileOrder;

        pub fn array_type(self: &Schema) -> Result<ArrayType>;
        pub fn capacity(self: &Schema) -> Result<u64>;
        pub fn allows_dups(self: &Schema) -> Result<bool>;
        pub fn tile_order(self: &Schema) -> Result<TileOrder>;
        pub fn cell_order(self: &Schema) -> Result<CellOrder>;

        pub fn domain(self: &Schema) -> Result<SharedPtr<Domain>>;
        pub fn num_attributes(self: &Schema) -> Result<u32>;
        pub fn has_attribute(self: &Schema, name: &str) -> Result<bool>;

        pub fn attribute_from_name(
            self: &Schema,
            name: &str,
        ) -> Result<SharedPtr<Attribute>>;

        pub fn attribute_from_index(
            self: &Schema,
            index: u32,
        ) -> Result<SharedPtr<Attribute>>;

        pub fn enumeration(
            self: &Schema,
            enmr_name: &str,
        ) -> Result<SharedPtr<Enumeration>>;

        pub fn enumeration_for_attribute(
            self: &Schema,
            attr_name: &str,
        ) -> Result<SharedPtr<Enumeration>>;

        pub fn coords_filter_list(
            self: &Schema,
        ) -> Result<SharedPtr<FilterList>>;

        pub fn offsets_filter_list(
            self: &Schema,
        ) -> Result<SharedPtr<FilterList>>;

        pub fn validity_filter_list(
            self: &Schema,
        ) -> Result<SharedPtr<FilterList>>;

        pub fn timestamp_range(
            self: &Schema,
            start: &mut u64,
            end: &mut u64,
        ) -> Result<()>;

        pub fn create_schema_builder(
            ctx: SharedPtr<Context>,
            atype: ArrayType,
        ) -> Result<SharedPtr<SchemaBuilder>>;

        pub fn build(self: &SchemaBuilder) -> Result<SharedPtr<Schema>>;

        pub fn set_capacity(self: &SchemaBuilder, capacity: u64) -> Result<()>;

        pub fn set_allows_dups(
            self: &SchemaBuilder,
            allows_dups: bool,
        ) -> Result<()>;

        pub fn set_tile_order(
            self: &SchemaBuilder,
            order: TileOrder,
        ) -> Result<()>;

        pub fn set_cell_order(
            self: &SchemaBuilder,
            order: CellOrder,
        ) -> Result<()>;

        pub fn set_domain(
            self: &SchemaBuilder,
            domain: SharedPtr<Domain>,
        ) -> Result<()>;

        pub fn add_attribute(
            self: &SchemaBuilder,
            attr: SharedPtr<Attribute>,
        ) -> Result<()>;

        pub fn add_enumeration(
            self: &SchemaBuilder,
            attr: SharedPtr<Enumeration>,
        ) -> Result<()>;

        pub fn set_coords_filter_list(
            self: &SchemaBuilder,
            filters: SharedPtr<FilterList>,
        ) -> Result<()>;

        pub fn set_offsets_filter_list(
            self: &SchemaBuilder,
            filters: SharedPtr<FilterList>,
        ) -> Result<()>;

        pub fn set_validity_filter_list(
            self: &SchemaBuilder,
            filters: SharedPtr<FilterList>,
        ) -> Result<()>;
    }
}

pub use ffi::*;
