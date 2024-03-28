use crate::error::Error;
use crate::Result as TileDBResult;

pub fn enable() -> TileDBResult<()> {
    let c_ret: i32 = unsafe { ffi::tiledb_stats_enable() };

    if c_ret == ffi::TILEDB_OK {
        Ok(())
    } else {
        Err(Error::from("Failed to enable stats."))
    }
}

pub fn disable() -> TileDBResult<()> {
    let c_ret: i32 = unsafe { ffi::tiledb_stats_disable() };

    if c_ret == ffi::TILEDB_OK {
        Ok(())
    } else {
        Err(Error::from("Failed to disable stats."))
    }
}

pub fn reset() -> TileDBResult<()> {
    let c_ret: i32 = unsafe { ffi::tiledb_stats_reset() };

    if c_ret == ffi::TILEDB_OK {
        Ok(())
    } else {
        Err(Error::from("Failed to reset stats."))
    }
}

pub fn dump() -> TileDBResult<Option<String>> {
    let mut c_str = std::ptr::null_mut::<std::ffi::c_char>();
    let res = unsafe {
        ffi::tiledb_stats_dump_str(&mut c_str as *mut *mut std::ffi::c_char)
    };
    if res == ffi::TILEDB_OK {
        assert!(!c_str.is_null());
        // ABI TODO: memory leaking
        let stats_dump = unsafe { std::ffi::CStr::from_ptr(c_str) };
        if stats_dump.is_empty() {
            return Ok(None);
        }
        Ok(Some(stats_dump.to_string_lossy().into_owned()))
    } else {
        Err(Error::from("Failed to retrieve stats."))
    }
}
