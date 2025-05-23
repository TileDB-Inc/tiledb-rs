#[cxx::bridge(namespace = "tiledb::rs")]
mod ffi {
    unsafe extern "C++" {
        include!("tiledb-sys2/cpp/array.h");

        type Array;

        type Buffer = crate::buffer::Buffer;
        type Config = crate::config::Config;
        type Context = crate::context::Context;
        type Datatype = crate::datatype::FFIDatatype;
        type Enumeration = crate::enumeration::Enumeration;
        type FilterList = crate::filter_list::FilterList;
        type Mode = crate::mode::FFIMode;
        type Schema = crate::schema::Schema;

        pub fn create_array(
            ctx: SharedPtr<Context>,
            uri: &str,
        ) -> Result<SharedPtr<Array>>;

        pub fn uri(self: &Array) -> Result<String>;

        pub fn set_config(self: &Array, cfg: SharedPtr<Config>) -> Result<()>;
        pub fn set_open_timestamp_start(self: &Array, ts: u64) -> Result<()>;
        pub fn set_open_timestamp_end(self: &Array, ts: u64) -> Result<()>;

        pub fn open(self: &Array, mode: Mode) -> Result<()>;
        pub fn reopen(self: &Array) -> Result<()>;
        pub fn close(self: &Array) -> Result<()>;

        pub fn is_open(self: &Array) -> Result<bool>;
        pub fn mode(self: &Array) -> Result<Mode>;
        pub fn config(self: &Array) -> Result<SharedPtr<Config>>;
        pub fn schema(self: &Array) -> Result<SharedPtr<Schema>>;
        pub fn open_timestamp_start(self: &Array) -> Result<u64>;
        pub fn open_timestamp_end(self: &Array) -> Result<u64>;

        pub fn get_enumeration(
            self: &Array,
            attr_name: &str,
        ) -> Result<SharedPtr<Enumeration>>;

        pub fn load_all_enumerations(self: &Array) -> Result<()>;
        pub fn load_enumerations_all_schemas(self: &Array) -> Result<()>;

        pub fn non_empty_domain_from_index(
            self: &Array,
            index: u32,
            values: Pin<&mut Buffer>,
        ) -> Result<bool>;

        pub fn non_empty_domain_from_name(
            self: &Array,
            name: &str,
            values: Pin<&mut Buffer>,
        ) -> Result<bool>;

        pub fn non_empty_domain_var_from_index(
            self: &Array,
            index: u32,
            lower: Pin<&mut Buffer>,
            upper: Pin<&mut Buffer>,
        ) -> Result<bool>;

        pub fn non_empty_domain_var_from_name(
            self: &Array,
            name: &str,
            lower: Pin<&mut Buffer>,
            upper: Pin<&mut Buffer>,
        ) -> Result<bool>;

        pub fn put_metadata(
            self: &Array,
            key: &str,
            dtype: Datatype,
            num: u32,
            values: Pin<&mut Buffer>,
        ) -> Result<()>;

        pub fn get_metadata(
            self: &Array,
            key: &str,
            dtype: &mut Datatype,
            values: Pin<&mut Buffer>,
        ) -> Result<()>;

        pub fn delete_metadata(self: &Array, key: &str) -> Result<()>;

        pub fn num_metadata(self: &Array) -> Result<u64>;

        pub fn get_metadata_from_index(
            self: &Array,
            index: u64,
            key: &mut Vec<u8>,
            dtype: &mut Datatype,
            values: Pin<&mut Buffer>,
        ) -> Result<()>;
    }
}

pub use ffi::*;
