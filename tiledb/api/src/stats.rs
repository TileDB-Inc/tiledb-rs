use std::collections::HashMap;
use std::ops::Deref;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Internal error enabling stats")]
    Enable,
    #[error("Internal error disabling stats")]
    Disable,
    #[error("Internal error resetting stats")]
    Reset,
    #[error("Internal error retrieving stats")]
    ToString,
    #[error("Error parsing stats to json: {0}")]
    ToJson(anyhow::Error),
}

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

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct Metrics {
    pub timers: HashMap<String, f64>,
    pub counters: HashMap<String, u64>,
}

pub fn enable() -> Result<(), Error> {
    let c_ret = unsafe { ffi::tiledb_stats_enable() };

    if c_ret == ffi::TILEDB_OK {
        Ok(())
    } else {
        Err(Error::Enable)
    }
}

pub fn disable() -> Result<(), Error> {
    let c_ret = unsafe { ffi::tiledb_stats_disable() };

    if c_ret == ffi::TILEDB_OK {
        Ok(())
    } else {
        Err(Error::Disable)
    }
}

pub fn reset() -> Result<(), Error> {
    let c_ret = unsafe { ffi::tiledb_stats_reset() };

    if c_ret == ffi::TILEDB_OK {
        Ok(())
    } else {
        Err(Error::Reset)
    }
}

pub fn dump() -> Result<Option<String>, Error> {
    let mut c_str = std::ptr::null_mut::<std::ffi::c_char>();

    let c_ret = unsafe {
        ffi::tiledb_stats_dump_str(&mut c_str as *mut *mut std::ffi::c_char)
    };

    if c_ret != ffi::TILEDB_OK {
        return Err(Error::ToString);
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

#[cfg(feature = "serde")]
pub fn dump_json() -> Result<Option<Vec<Metrics>>, Error> {
    use anyhow::anyhow;
    if let Some(dump) = dump()? {
        Ok(serde_json::from_str::<Vec<Metrics>>(dump.as_str())
            .map(Some)
            .map_err(|e| Error::ToJson(anyhow!(e)))?)
    } else {
        Ok(None)
    }
}
