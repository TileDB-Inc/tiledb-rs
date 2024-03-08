use crate::constants::TILEDB_OK;
use crate::types::capi_return_t;

extern "C" {
    pub fn tiledb_filter_type_to_str(
        filter_type: u32,
        str_: *mut *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_filter_type_from_str(
        str_: *const ::std::os::raw::c_char,
        filter_type: *mut u32,
    ) -> capi_return_t;
}

// TileDB declares 17 as DEPRECATED, I've just elided that enum member here.
#[derive(Clone, Debug, PartialEq)]
pub enum FilterType {
    NONE = 0,
    GZIP = 1,
    ZSTD = 2,
    LZ4 = 3,
    RLE = 4,
    BZIP2 = 5,
    DOUBLE_DELTA = 6,
    BIT_WIDTH_REDUCTION = 7,
    BITSHUFFLE = 8,
    BYTESHUFFLE = 9,
    POSITIVE_DELTA = 10,
    CHECKSUM_MD5 = 12,
    CHECKSUM_SHA256 = 13,
    DICTIONARY = 14,
    SCALE_FLOAT = 15,
    XOR = 16,
    WEBP = 18,
    DELTA = 19,
}

impl FilterType {
    pub fn to_string(&self) -> Option<String> {
        let copy = (*self).clone();
        let c_ftype: u32 = copy as u32;
        let mut c_str = std::ptr::null::<std::os::raw::c_char>();
        let res = unsafe { tiledb_filter_type_to_str(c_ftype, &mut c_str) };
        if res == TILEDB_OK {
            let c_msg = unsafe { std::ffi::CStr::from_ptr(c_str) };
            Some(String::from(c_msg.to_string_lossy()))
        } else {
            None
        }
    }

    pub fn from_string(fs: &str) -> Option<FilterType> {
        let c_ftype =
            std::ffi::CString::new(fs).expect("Error creating CString");
        let mut c_ret: u32 = 0;
        let res = unsafe {
            tiledb_filter_type_from_str(c_ftype.as_c_str().as_ptr(), &mut c_ret)
        };

        if res == TILEDB_OK {
            FilterType::from_u32(c_ret)
        } else {
            None
        }
    }

    pub fn from_u32(ft: u32) -> Option<FilterType> {
        match ft {
            0 => Some(FilterType::NONE),
            1 => Some(FilterType::GZIP),
            2 => Some(FilterType::ZSTD),
            3 => Some(FilterType::LZ4),
            4 => Some(FilterType::RLE),
            5 => Some(FilterType::BZIP2),
            6 => Some(FilterType::DOUBLE_DELTA),
            7 => Some(FilterType::BIT_WIDTH_REDUCTION),
            8 => Some(FilterType::BITSHUFFLE),
            9 => Some(FilterType::BYTESHUFFLE),
            10 => Some(FilterType::POSITIVE_DELTA),
            12 => Some(FilterType::CHECKSUM_MD5),
            13 => Some(FilterType::CHECKSUM_SHA256),
            14 => Some(FilterType::DICTIONARY),
            15 => Some(FilterType::SCALE_FLOAT),
            16 => Some(FilterType::XOR),
            18 => Some(FilterType::WEBP),
            19 => Some(FilterType::DELTA),
            _ => None,
        }
    }
}
