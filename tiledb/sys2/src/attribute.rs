#[cxx::bridge(namespace = "tiledb::rs")]
mod ffi {
    unsafe extern "C++" {
        include!("tiledb-sys2/cpp/attribute.h");

        type Attribute;
        type AttributeBuilder;
        type Context = crate::context::Context;
        type Datatype = crate::datatype::Datatype;
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

        pub fn set_fill_value_i8(
            self: &AttributeBuilder,
            value: &[i8],
        ) -> Result<()>;

        pub fn set_fill_value_i16(
            self: &AttributeBuilder,
            value: &[i16],
        ) -> Result<()>;

        pub fn set_fill_value_i32(
            self: &AttributeBuilder,
            value: &[i32],
        ) -> Result<()>;

        pub fn set_fill_value_i64(
            self: &AttributeBuilder,
            value: &[i64],
        ) -> Result<()>;

        pub fn set_fill_value_u8(
            self: &AttributeBuilder,
            value: &[u8],
        ) -> Result<()>;

        pub fn set_fill_value_u16(
            self: &AttributeBuilder,
            value: &[u16],
        ) -> Result<()>;

        pub fn set_fill_value_u32(
            self: &AttributeBuilder,
            value: &[u32],
        ) -> Result<()>;

        pub fn set_fill_value_u64(
            self: &AttributeBuilder,
            value: &[u64],
        ) -> Result<()>;

        pub fn set_fill_value_f32(
            self: &AttributeBuilder,
            value: &[f32],
        ) -> Result<()>;

        pub fn set_fill_value_f64(
            self: &AttributeBuilder,
            value: &[f64],
        ) -> Result<()>;

        pub fn set_fill_value_nullable_i8(
            self: &AttributeBuilder,
            value: &[i8],
            validity: u8,
        ) -> Result<()>;

        pub fn set_fill_value_nullable_i16(
            self: &AttributeBuilder,
            value: &[i16],
            validity: u8,
        ) -> Result<()>;

        pub fn set_fill_value_nullable_i32(
            self: &AttributeBuilder,
            value: &[i32],
            validity: u8,
        ) -> Result<()>;

        pub fn set_fill_value_nullable_i64(
            self: &AttributeBuilder,
            value: &[i64],
            validity: u8,
        ) -> Result<()>;

        pub fn set_fill_value_nullable_u8(
            self: &AttributeBuilder,
            value: &[u8],
            validity: u8,
        ) -> Result<()>;

        pub fn set_fill_value_nullable_u16(
            self: &AttributeBuilder,
            value: &[u16],
            validity: u8,
        ) -> Result<()>;

        pub fn set_fill_value_nullable_u32(
            self: &AttributeBuilder,
            value: &[u32],
            validity: u8,
        ) -> Result<()>;

        pub fn set_fill_value_nullable_u64(
            self: &AttributeBuilder,
            value: &[u64],
            validity: u8,
        ) -> Result<()>;

        pub fn set_fill_value_nullable_f32(
            self: &AttributeBuilder,
            value: &[f32],
            validity: u8,
        ) -> Result<()>;

        pub fn set_fill_value_nullable_f64(
            self: &AttributeBuilder,
            value: &[f64],
            validity: u8,
        ) -> Result<()>;

    }
}

pub use ffi::*;
