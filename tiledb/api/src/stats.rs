use std::collections::HashMap;
use std::ops::Deref;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::Result as TileDBResult;

pub(crate) enum RawStatsString {
    Owned(*mut std::ffi::c_char),
}

impl Deref for RawStatsString {
    type Target = *mut std::ffi::c_char;
    fn deref(&self) -> &Self::Target {
        let RawStatsString::Owned(ref ffi) = *self;
        ffi
    }
}

impl Drop for RawStatsString {
    fn drop(&mut self) {
        let RawStatsString::Owned(ref mut ffi) = *self;
        let res = unsafe {
            ffi::tiledb_stats_free_str(ffi as *mut *mut std::ffi::c_char)
        };
        // This is currently a hardcoded return value so this assertion only
        // exists to know if that ever changes.
        assert_eq!(res, ffi::TILEDB_OK);
    }
}

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

    if c_ret != ffi::TILEDB_OK {
        return Err(Error::LibTileDB(String::from(
            "Failed to retrieve stats.",
        )));
    }

    assert!(!c_str.is_null());
    let raw = RawStatsString::Owned(c_str);
    let stats_dump = unsafe { std::ffi::CStr::from_ptr(*raw) };
    let stats_dump_rust_str = stats_dump.to_string_lossy().into_owned();
    if stats_dump_rust_str.is_empty() {
        return Ok(None);
    }

    Ok(Some(stats_dump_rust_str))
}

pub fn dump_json() -> TileDBResult<Option<Vec<Metrics>>> {
    if let Some(dump) = dump()? {
        let datas: Vec<Metrics> = serde_json::from_str::<Vec<Metrics>>(
            dump.as_str(),
        )
        .map_err(|e| {
            Error::Deserialization(
                format!("Failed to deserialize stats JSON value {}", dump),
                anyhow!(e),
            )
        })?;
        return Ok(Some(datas));
    }
    Ok(None)
}
