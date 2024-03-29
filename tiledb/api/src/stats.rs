use std::collections::HashMap;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::Result as TileDBResult;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Metrics {
    pub timers: HashMap<String, f64>,
    pub counters: HashMap<String, u64>,
}

pub fn enable() -> TileDBResult<()> {
    let c_ret = unsafe { ffi::tiledb_stats_enable() };

    if c_ret == ffi::TILEDB_OK {
        Ok(())
    } else {
        Err(Error::LibTileDB(String::from("Failed to enable stats.")))
    }
}

pub fn disable() -> TileDBResult<()> {
    let c_ret = unsafe { ffi::tiledb_stats_disable() };

    if c_ret == ffi::TILEDB_OK {
        Ok(())
    } else {
        Err(Error::LibTileDB(String::from("Failed to disable stats.")))
    }
}

pub fn reset() -> TileDBResult<()> {
    let c_ret = unsafe { ffi::tiledb_stats_reset() };

    if c_ret == ffi::TILEDB_OK {
        Ok(())
    } else {
        Err(Error::LibTileDB(String::from("Failed to reset stats.")))
    }
}

pub fn dump() -> TileDBResult<Option<String>> {
    let mut c_str = std::ptr::null_mut::<std::ffi::c_char>();
    let c_ret = unsafe {
        ffi::tiledb_stats_dump_str(&mut c_str as *mut *mut std::ffi::c_char)
    };
    if c_ret == ffi::TILEDB_OK {
        assert!(!c_str.is_null());
        let stats_dump = unsafe { std::ffi::CStr::from_ptr(c_str) };
        let stats_dump_rust_str = stats_dump.to_string_lossy().into_owned();
        let free_c_ret = unsafe {
            ffi::tiledb_stats_free_str(&mut c_str as *mut *mut std::ffi::c_char)
        };
        if free_c_ret != ffi::TILEDB_OK {
            return Err(Error::LibTileDB(String::from(
                "Failed to free stats str.",
            )));
        }
        if stats_dump_rust_str.is_empty() {
            return Ok(None);
        }
        Ok(Some(stats_dump_rust_str))
    } else {
        Err(Error::LibTileDB(String::from("Failed to retrieve stats.")))
    }
}

pub fn dump_json() -> TileDBResult<Option<Vec<Metrics>>> {
    let dump_option = dump()?;
    let dump = match dump_option {
        None => return Ok(None),
        Some(dump_str) => dump_str,
    };
    let datas: Vec<Metrics> =
        serde_json::from_str::<Vec<Metrics>>(dump.as_str()).map_err(|e| {
            Error::Deserialization(
                format!("Failed to deserialize stats JSON value {}", dump),
                anyhow!(e),
            )
        })?;
    Ok(Some(datas))
}
