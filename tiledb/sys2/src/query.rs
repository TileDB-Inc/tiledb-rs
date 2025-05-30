#[cxx::bridge(namespace = "tiledb::rs")]
mod ffi {
    unsafe extern "C++" {
        include!("tiledb-sys2/cpp/query.h");

        type Query;
        type QueryBuilder;

        type Array = crate::array::Array;
        type Buffer = crate::buffer::Buffer;
        type CellOrder = crate::layout::FFICellOrder;
        type Config = crate::config::Config;
        type Context = crate::context::Context;
        type Mode = crate::mode::FFIMode;
        type QueryStatus = crate::query_status::FFIQueryStatus;

        pub fn mode(self: &Query) -> Result<Mode>;
        pub fn config(self: &Query) -> Result<SharedPtr<Config>>;
        pub fn layout(self: &Query) -> Result<CellOrder>;

        pub(crate) fn set_data_buffer(
            self: &Query,
            field: &str,
            buffer: Pin<&mut Buffer>,
        ) -> Result<()>;

        pub(crate) fn set_offsets_buffer(
            self: &Query,
            field: &str,
            buffer: Pin<&mut Buffer>,
        ) -> Result<()>;

        pub(crate) fn set_validity_buffer(
            self: &Query,
            field: &str,
            buffer: Pin<&mut Buffer>,
        ) -> Result<()>;

        pub(crate) fn get_buffer_sizes(
            self: &Query,
            field: &str,
            data_size: &mut u64,
            offset_size: &mut u64,
            validity_size: &mut u64,
        ) -> Result<bool>;

        pub fn submit(self: &Query) -> Result<()>;
        pub fn finalize(self: &Query) -> Result<()>;
        pub fn submit_and_finalize(self: &Query) -> Result<()>;

        pub fn status(self: &Query) -> Result<QueryStatus>;
        pub fn has_results(self: &Query) -> Result<bool>;

        pub fn est_result_size(
            self: &Query,
            name: &str,
            data_size: &mut u64,
            offsets_size: &mut u64,
            validity_size: &mut u64,
        ) -> Result<()>;

        pub fn num_fragments(self: &Query) -> Result<u32>;
        pub fn num_relevant_fragments(self: &Query) -> Result<u64>;
        pub fn fragment_uri(self: &Query, index: u32) -> Result<String>;
        pub fn fragment_timestamp_range(
            self: &Query,
            index: u32,
            start: &mut u64,
            end: &mut u64,
        ) -> Result<()>;

        pub fn stats(self: &Query) -> Result<String>;

        pub fn create_query_builder(
            ctx: SharedPtr<Context>,
            array: SharedPtr<Array>,
            mode: Mode,
        ) -> Result<SharedPtr<QueryBuilder>>;

        pub fn build(self: &QueryBuilder) -> Result<SharedPtr<Query>>;

        pub fn set_layout(self: &QueryBuilder, order: CellOrder) -> Result<()>;
        pub fn set_config(
            self: &QueryBuilder,
            config: SharedPtr<Config>,
        ) -> Result<()>;
    }
}

pub use ffi::*;
