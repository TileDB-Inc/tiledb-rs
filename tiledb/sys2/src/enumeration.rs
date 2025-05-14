#[cxx::bridge(namespace = "tiledb::rs")]
mod ffi {
    unsafe extern "C++" {
        include!("tiledb-sys2/cpp/enumeration.h");

        type Enumeration;

        type Buffer = crate::buffer::Buffer;
        type Context = crate::context::Context;
        type Datatype = crate::datatype::FFIDatatype;

        pub fn name(self: &Enumeration) -> Result<String>;
        pub fn datatype(self: &Enumeration) -> Result<Datatype>;
        pub fn cell_val_num(self: &Enumeration) -> Result<u32>;
        pub fn ordered(self: &Enumeration) -> Result<bool>;

        pub fn get_data(
            self: &Enumeration,
            buf: Pin<&mut Buffer>,
        ) -> Result<()>;

        pub fn get_offsets(
            self: &Enumeration,
            buf: Pin<&mut Buffer>,
        ) -> Result<()>;

        pub fn get_index(
            self: &Enumeration,
            buf: Pin<&mut Buffer>,
            index: &mut u64,
        ) -> Result<bool>;

        pub fn create_enumeration(
            ctx: SharedPtr<Context>,
            name: &str,
            dtype: Datatype,
            cell_val_num: u32,
            ordered: bool,
            data: Pin<&mut Buffer>,
            offsets: Pin<&mut Buffer>,
        ) -> Result<SharedPtr<Enumeration>>;
    }
}

pub use ffi::*;
