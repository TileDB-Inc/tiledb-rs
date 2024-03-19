use crate::constants::TILEDB_OK;
use crate::types::capi_return_t;

extern "C" {
    pub fn tiledb_filesystem_to_str(
        filesystem: u32,
        str_: *mut *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_filesystem_from_str(
        str_: *const ::std::os::raw::c_char,
        filesystem: *mut u32,
    ) -> capi_return_t;
}

#[derive(Clone, Debug, PartialEq)]
pub enum Filesystem {
    HDFS = 0,
    S3 = 1,
    AZURE = 2,
    GCS = 3,
    MEMFS = 4,
}

impl Filesystem {
    pub fn to_string(&self) -> Option<String> {
        let copy = (*self).clone();
        let c_fs: u32 = copy as u32;
        let mut c_str = std::ptr::null::<std::os::raw::c_char>();
        let res = unsafe { tiledb_filesystem_to_str(c_fs, &mut c_str) };
        if res == TILEDB_OK {
            let c_msg = unsafe { std::ffi::CStr::from_ptr(c_str) };
            Some(String::from(c_msg.to_string_lossy()))
        } else {
            None
        }
    }

    pub fn from_string(fs: &str) -> Option<Filesystem> {
        let c_fs = std::ffi::CString::new(fs).expect("Error creating CString");
        let mut c_ret: u32 = 0;
        let res = unsafe {
            tiledb_filesystem_from_str(c_fs.as_c_str().as_ptr(), &mut c_ret)
        };

        if res == TILEDB_OK {
            Filesystem::from_u32(c_ret)
        } else {
            None
        }
    }

    pub fn from_u32(fs: u32) -> Option<Filesystem> {
        match fs {
            0 => Some(Filesystem::HDFS),
            1 => Some(Filesystem::S3),
            2 => Some(Filesystem::AZURE),
            3 => Some(Filesystem::GCS),
            4 => Some(Filesystem::MEMFS),
            _ => None,
        }
    }
}
