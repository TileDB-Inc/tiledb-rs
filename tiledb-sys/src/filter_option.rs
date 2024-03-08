use crate::constants::TILEDB_OK;
use crate::types::capi_return_t;

extern "C" {
    pub fn tiledb_filter_option_to_str(
        filter_option: u32,
        str_: *mut *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_filter_option_from_str(
        str_: *const ::std::os::raw::c_char,
        filter_option: *mut u32,
    ) -> capi_return_t;
}

#[derive(Clone, Debug, PartialEq)]
pub enum FilterOption {
    COMPRESSION_LEVEL = 0,
    BIT_WIDTH_MAX_WINDOW = 1,
    POSITIVE_DELTA_MAX_WINDOW = 2,
    FLOAT_BYTEWIDTH = 3,
    FLOAT_FACTOR = 4,
    FLOAT_OFFSET = 5,
    WEBP_QUALITY = 6,
    WEBP_INPUT_FORMAT = 7,
    WEBP_LOSSLESS = 8,
    COMPRESSION_REINTERPRET_DATATYPE = 9,
}

impl FilterOption {
    pub fn to_string(&self) -> Option<String> {
        let copy = (*self).clone();
        let c_fopt: u32 = copy as u32;
        let mut c_str = std::ptr::null::<std::os::raw::c_char>();
        let res = unsafe { tiledb_filter_option_to_str(c_fopt, &mut c_str) };
        if res == TILEDB_OK {
            let c_msg = unsafe { std::ffi::CStr::from_ptr(c_str) };
            Some(String::from(c_msg.to_string_lossy()))
        } else {
            None
        }
    }

    pub fn from_string(fs: &str) -> Option<FilterOption> {
        let c_fopt =
            std::ffi::CString::new(fs).expect("Error creating CString");
        let mut c_ret: u32 = 0;
        let res = unsafe {
            tiledb_filter_option_from_str(
                c_fopt.as_c_str().as_ptr(),
                &mut c_ret,
            )
        };

        if res == TILEDB_OK {
            FilterOption::from_u32(c_ret)
        } else {
            None
        }
    }

    pub fn from_u32(ft: u32) -> Option<FilterOption> {
        match ft {
            0 => Some(FilterOption::COMPRESSION_LEVEL),
            1 => Some(FilterOption::BIT_WIDTH_MAX_WINDOW),
            2 => Some(FilterOption::POSITIVE_DELTA_MAX_WINDOW),
            3 => Some(FilterOption::FLOAT_BYTEWIDTH),
            4 => Some(FilterOption::FLOAT_FACTOR),
            5 => Some(FilterOption::FLOAT_OFFSET),
            6 => Some(FilterOption::WEBP_QUALITY),
            7 => Some(FilterOption::WEBP_INPUT_FORMAT),
            8 => Some(FilterOption::WEBP_LOSSLESS),
            9 => Some(FilterOption::COMPRESSION_REINTERPRET_DATATYPE),
            _ => None,
        }
    }
}
