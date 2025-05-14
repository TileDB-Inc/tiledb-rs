#[cxx::bridge(namespace = "tiledb::rs")]
mod ffi {
    unsafe extern "C++" {
        include!("tiledb-sys2/cpp/dimension.h");

        type Dimension;
        type DimensionBuilder;

        type Buffer = crate::buffer::Buffer;
        type Context = crate::context::Context;
        type Datatype = crate::datatype::FFIDatatype;
        type FilterList = crate::filter_list::FilterList;

        pub fn name(self: &Dimension) -> Result<String>;
        pub fn datatype(self: &Dimension) -> Result<Datatype>;
        pub fn domain(self: &Dimension, buf: Pin<&mut Buffer>) -> Result<bool>;
        pub fn tile_extent(
            self: &Dimension,
            buf: Pin<&mut Buffer>,
        ) -> Result<bool>;
        pub fn cell_val_num(self: &Dimension) -> Result<u32>;
        pub fn filter_list(self: &Dimension) -> Result<SharedPtr<FilterList>>;

        pub fn create_dimension_builder(
            ctx: SharedPtr<Context>,
            name: &str,
            dtype: Datatype,
            domain: Pin<&mut Buffer>,
            extent: Pin<&mut Buffer>,
        ) -> Result<SharedPtr<DimensionBuilder>>;

        pub fn build(self: &DimensionBuilder) -> Result<SharedPtr<Dimension>>;

        fn set_cell_val_num(self: &DimensionBuilder, cvn: u32) -> Result<()>;

        fn set_filter_list(
            self: &DimensionBuilder,
            filters: SharedPtr<FilterList>,
        ) -> Result<()>;
    }
}

pub use ffi::*;
