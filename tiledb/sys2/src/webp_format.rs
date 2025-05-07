#[cxx::bridge(namespace = "tiledb::rs")]
mod ffi {
    pub enum WebPFormat {
        None,
        Rgb,
        Bgr,
        Rgba,
        Bgra,
    }
}

pub use ffi::*;
