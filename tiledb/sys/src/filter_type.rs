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
    None = 0,
    Gzip = 1,
    Zstd = 2,
    Lz4 = 3,
    Rle = 4,
    Bzip2 = 5,
    DoubleDelta = 6,
    BitWidthReduction = 7,
    BitShuffle = 8,
    ByteShuffle = 9,
    PositiveDelta = 10,
    ChecksumMD5 = 12,
    ChecksumSHA256 = 13,
    Dictionary = 14,
    ScaleFloat = 15,
    Xor = 16,
    WebP = 18,
    Delta = 19,
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
            0 => Some(FilterType::None),
            1 => Some(FilterType::Gzip),
            2 => Some(FilterType::Zstd),
            3 => Some(FilterType::Lz4),
            4 => Some(FilterType::Rle),
            5 => Some(FilterType::Bzip2),
            6 => Some(FilterType::DoubleDelta),
            7 => Some(FilterType::BitWidthReduction),
            8 => Some(FilterType::BitShuffle),
            9 => Some(FilterType::ByteShuffle),
            10 => Some(FilterType::PositiveDelta),
            12 => Some(FilterType::ChecksumMD5),
            13 => Some(FilterType::ChecksumSHA256),
            14 => Some(FilterType::Dictionary),
            15 => Some(FilterType::ScaleFloat),
            16 => Some(FilterType::Xor),
            18 => Some(FilterType::WebP),
            19 => Some(FilterType::Delta),
            _ => None,
        }
    }
}
