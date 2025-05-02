use crate::error::TileDBError;

#[cxx::bridge(namespace = "tiledb::rs")]
mod ffi {
    unsafe extern "C++" {
        include!("tiledb-api2/cpp/context.h");

        type Context;

    }
}
