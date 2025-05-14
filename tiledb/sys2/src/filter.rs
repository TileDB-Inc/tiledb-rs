#[cxx::bridge(namespace = "tiledb::rs")]
mod ffi {
    unsafe extern "C++" {
        include!("tiledb-sys2/cpp/filter.h");

        type Filter;
        type FilterBuilder;
        type FilterType = crate::filter_type::FilterType;
        type Context = crate::context::Context;
        type Datatype = crate::datatype::FFIDatatype;
        type WebPFormat = crate::webp_format::WebPFormat;

        pub fn get_type(self: &Filter) -> Result<FilterType>;
        pub fn get_compression_level(self: &Filter) -> Result<i32>;
        pub fn get_compression_reinterpret_datatype(
            self: &Filter,
        ) -> Result<Datatype>;
        pub fn get_bit_width_max_window(self: &Filter) -> Result<u32>;
        pub fn get_positive_delta_max_window(self: &Filter) -> Result<u32>;
        pub fn get_scale_float_bytewidth(self: &Filter) -> Result<u64>;
        pub fn get_scale_float_factor(self: &Filter) -> Result<f64>;
        pub fn get_scale_float_offset(self: &Filter) -> Result<f64>;
        pub fn get_webp_quality(self: &Filter) -> Result<f32>;
        pub fn get_webp_input_format(self: &Filter) -> Result<WebPFormat>;
        pub fn get_webp_lossless(self: &Filter) -> Result<bool>;

        pub fn create_filter_builder(
            ctx: SharedPtr<Context>,
            filter_type: FilterType,
        ) -> Result<SharedPtr<FilterBuilder>>;

        pub fn build(self: &FilterBuilder) -> Result<SharedPtr<Filter>>;

        pub fn set_compression_level(
            self: &FilterBuilder,
            val: i32,
        ) -> Result<()>;

        pub fn set_compression_reinterpret_datatype(
            self: &FilterBuilder,
            val: Datatype,
        ) -> Result<()>;

        pub fn set_bit_width_max_window(
            self: &FilterBuilder,
            val: u32,
        ) -> Result<()>;

        pub fn set_positive_delta_max_window(
            self: &FilterBuilder,
            val: u32,
        ) -> Result<()>;

        pub fn set_scale_float_bytewidth(
            self: &FilterBuilder,
            val: u64,
        ) -> Result<()>;

        pub fn set_scale_float_factor(
            self: &FilterBuilder,
            val: f64,
        ) -> Result<()>;

        pub fn set_scale_float_offset(
            self: &FilterBuilder,
            val: f64,
        ) -> Result<()>;

        pub fn set_webp_quality(self: &FilterBuilder, val: f32) -> Result<()>;

        pub fn set_webp_input_format(
            self: &FilterBuilder,
            val: WebPFormat,
        ) -> Result<()>;

        pub fn set_webp_lossless(self: &FilterBuilder, val: bool)
        -> Result<()>;

    }
}

pub use ffi::*;
