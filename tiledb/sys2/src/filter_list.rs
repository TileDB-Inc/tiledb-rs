#[cxx::bridge(namespace = "tiledb::rs")]
mod ffi {
    unsafe extern "C++" {
        include!("tiledb-sys2/cpp/filter.h");
        include!("tiledb-sys2/cpp/filter_list.h");

        type FilterList;
        type FilterListBuilder;
        type Context = crate::context::Context;
        type Filter = crate::filter::Filter;

        pub fn num_filters(self: &FilterList) -> Result<u32>;

        pub fn get_filter(
            self: &FilterList,
            idx: u32,
        ) -> Result<SharedPtr<Filter>>;

        pub fn max_chunk_size(self: &FilterList) -> Result<u32>;

        pub fn create_filter_list_builder(
            ctx: SharedPtr<Context>,
        ) -> Result<SharedPtr<FilterListBuilder>>;

        pub fn build(self: &FilterListBuilder)
        -> Result<SharedPtr<FilterList>>;

        pub fn add_filter(
            self: &FilterListBuilder,
            filter: SharedPtr<Filter>,
        ) -> Result<()>;

        pub fn set_max_chunk_size(
            self: &FilterListBuilder,
            size: u32,
        ) -> Result<()>;
    }
}

pub use ffi::*;
