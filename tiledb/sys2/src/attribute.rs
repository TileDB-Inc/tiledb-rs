#[cxx::bridge(namespace = "tiledb::rs")]
mod ffi {
    unsafe extern "C++" {
        include!("tiledb-sys2/cpp/attribute.h");

        type Attribute;
        type AttributeBuilder;

        type Buffer = crate::buffer::Buffer;
        type Context = crate::context::Context;
        type Datatype = crate::datatype::FFIDatatype;
        type FilterList = crate::filter_list::FilterList;

        pub fn name(self: &Attribute) -> Result<String>;
        pub fn datatype(self: &Attribute) -> Result<Datatype>;
        pub fn cell_size(self: &Attribute) -> Result<u64>;
        pub fn cell_val_num(self: &Attribute) -> Result<u32>;
        pub fn nullable(self: &Attribute) -> Result<bool>;

        pub fn enumeration_name(
            self: &Attribute,
            name: &mut String,
        ) -> Result<bool>;

        pub fn filter_list(self: &Attribute) -> Result<SharedPtr<FilterList>>;

        pub fn fill_value(
            self: &Attribute,
            buf: Pin<&mut Buffer>,
        ) -> Result<()>;

        pub fn fill_value_nullable(
            self: &Attribute,
            buf: Pin<&mut Buffer>,
            validity: &mut u8,
        ) -> Result<()>;

        pub fn create_attribute_builder(
            ctx: SharedPtr<Context>,
            name: &str,
            datatype: Datatype,
        ) -> Result<SharedPtr<AttributeBuilder>>;

        pub fn build(self: &AttributeBuilder) -> Result<SharedPtr<Attribute>>;

        pub fn set_nullable(
            self: &AttributeBuilder,
            nullable: bool,
        ) -> Result<()>;

        pub fn set_cell_val_num(
            self: &AttributeBuilder,
            num: u32,
        ) -> Result<()>;

        pub fn set_enumeration_name(
            self: &AttributeBuilder,
            name: &str,
        ) -> Result<()>;

        pub fn set_filter_list(
            self: &AttributeBuilder,
            filter_list: SharedPtr<FilterList>,
        ) -> Result<()>;

        pub fn set_fill_value(
            self: &AttributeBuilder,
            value: Pin<&mut Buffer>,
        ) -> Result<()>;

        pub fn set_fill_value_nullable(
            self: &AttributeBuilder,
            value: Pin<&mut Buffer>,
            validity: u8,
        ) -> Result<()>;
    }
}

pub use ffi::*;
