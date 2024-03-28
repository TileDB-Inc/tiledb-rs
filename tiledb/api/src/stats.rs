use crate::error::Error;
use crate::Result as TileDBResult;

pub fn stats_enable() -> TileDBResult<()> {
    let c_ret: i32 = unsafe { ffi::tiledb_stats_enable() };

    if c_ret == ffi::TILEDB_OK {
        Ok(())
    } else {
        Err(Error::from("TileDB stats were not successfully enabled."))
    }
}

pub fn stats_disable() -> TileDBResult<()> {
    let c_ret: i32 = unsafe { ffi::tiledb_stats_disable() };

    if c_ret == ffi::TILEDB_OK {
        Ok(())
    } else {
        Err(Error::from("TileDB stats were not successfully disabled."))
    }
}

pub fn stats_reset() -> TileDBResult<()> {
    let c_ret: i32 = unsafe { ffi::tiledb_stats_reset() };

    if c_ret == ffi::TILEDB_OK {
        Ok(())
    } else {
        Err(Error::from("TileDB stats were not successfully reset."))
    }
}

pub fn stats_dump_str() -> TileDBResult<String> {
    let mut stats_dump_c_str = std::ptr::null_mut::<std::os::raw::c_char>();
    let res = unsafe {
        ffi::tiledb_stats_dump_str(
            &mut stats_dump_c_str as *mut *mut std::os::raw::c_char,
        )
    };
    if res == ffi::TILEDB_OK {
        assert!(!stats_dump_c_str.is_null());
        let stats_dump = unsafe { std::ffi::CStr::from_ptr(stats_dump_c_str) };
        Ok(String::from(stats_dump.to_string_lossy()))
    } else {
        Err(Error::from("TileDB stats were unable to be retrieved."))
    }
}

pub fn stats_raw_dump_str() -> TileDBResult<String> {
    let mut stats_dump_c_str = std::ptr::null_mut::<std::os::raw::c_char>();
    let res = unsafe {
        ffi::tiledb_stats_raw_dump_str(
            &mut stats_dump_c_str as *mut *mut std::os::raw::c_char,
        )
    };
    if res == ffi::TILEDB_OK {
        assert!(!stats_dump_c_str.is_null());
        let stats_dump = unsafe { std::ffi::CStr::from_ptr(stats_dump_c_str) };
        Ok(String::from(stats_dump.to_string_lossy()))
    } else {
        Err(Error::from("TileDB stats were unable to be retrieved."))
    }
}
