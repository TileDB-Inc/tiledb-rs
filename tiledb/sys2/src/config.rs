#[cxx::bridge(namespace = "tiledb::rs")]
mod ffi {
    unsafe extern "C++" {
        include!("tiledb-sys2/cpp/config.h");

        type Config;

        fn create_config() -> Result<SharedPtr<Config>>;
        fn get(&self, key: &str) -> Result<String>;
        fn contains(&self, key: &str) -> Result<bool>;
        fn set(&self, key: &str, val: &str) -> Result<()>;
        fn unset(&self, key: &str) -> Result<()>;
        fn load_from_file(&self, path: &str) -> Result<()>;
        fn save_to_file(&self, path: &str) -> Result<()>;
    }
}

pub use ffi::*;
