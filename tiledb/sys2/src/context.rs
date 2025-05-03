#[cxx::bridge(namespace = "tiledb::rs")]
mod ffi {
    unsafe extern "C++" {
        include!("tiledb-sys2/cpp/context.h");

        type Config = crate::config::Config;
        type Context;

        pub fn create_context() -> Result<UniquePtr<Context>>;
        pub fn create_context_with_config(
            cfg: &UniquePtr<Config>,
        ) -> Result<UniquePtr<Context>>;

        pub fn is_supported_fs(self: &Context, fs: i32) -> Result<bool>;
        pub fn set_tag(self: &Context, key: &str, val: &str) -> Result<()>;
        pub fn stats(self: &Context) -> Result<String>;
    }
}

pub use ffi::*;
