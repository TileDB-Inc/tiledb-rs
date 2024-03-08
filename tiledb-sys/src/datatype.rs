use crate::constants::TILEDB_OK;
use crate::types::capi_return_t;

extern "C" {
    pub fn tiledb_datatype_to_str(
        datatype: u32,
        str_: *mut *const ::std::os::raw::c_char,
    ) -> capi_return_t;

    pub fn tiledb_datatype_from_str(
        str_: *const ::std::os::raw::c_char,
        datatype: *mut u32,
    ) -> capi_return_t;

    pub fn tiledb_datatype_size(type_: u32) -> u64;
}

// When I find the time, I should come back and turn these into a macro
// so that we can auto-generate the Datatype::from_u32 generation.

#[derive(Clone, Debug, PartialEq)]
pub enum Datatype {
    INT32 = 0,
    INT64 = 1,
    FLOAT32 = 2,
    FLOAT64 = 3,
    CHAR = 4,
    INT8 = 5,
    UINT8 = 6,
    INT16 = 7,
    UINT16 = 8,
    UINT32 = 9,
    UINT64 = 10,
    STRING_ASCII = 11,
    STRING_UTF8 = 12,
    STRING_UTF16 = 13,
    STRING_UTF32 = 14,
    STRING_UCS2 = 15,
    STRING_UCS4 = 16,
    ANY = 17,
    DATETIME_YEAR = 18,
    DATETIME_MONTH = 19,
    DATETIME_WEEK = 20,
    DATETIME_DAY = 21,
    DATETIME_HR = 22,
    DATETIME_MIN = 23,
    DATETIME_SEC = 24,
    DATETIME_MS = 25,
    DATETIME_US = 26,
    DATETIME_NS = 27,
    DATETIME_PS = 28,
    DATETIME_FS = 29,
    DATETIME_AS = 30,
    TIME_HR = 31,
    TIME_MIN = 32,
    TIME_SEC = 33,
    TIME_MS = 34,
    TIME_US = 35,
    TIME_NS = 36,
    TIME_PS = 37,
    TIME_FS = 38,
    TIME_AS = 39,
    BLOB = 40,
    BOOL = 41,
    GEOM_WKB = 42,
    GEOM_WKT = 43,
}

impl Datatype {
    pub fn size(&self) -> u64 {
        let copy = (*self).clone();
        unsafe { tiledb_datatype_size(copy as u32) }
    }

    pub fn to_string(&self) -> Option<String> {
        let copy = (*self).clone();
        let c_dtype: u32 = copy as u32;
        let mut c_str = std::ptr::null::<std::os::raw::c_char>();
        let res = unsafe { tiledb_datatype_to_str(c_dtype, &mut c_str) };
        if res == TILEDB_OK {
            let c_msg = unsafe { std::ffi::CStr::from_ptr(c_str) };
            Some(String::from(c_msg.to_string_lossy()))
        } else {
            None
        }
    }

    pub fn from_string(dtype: &str) -> Option<Datatype> {
        let c_dtype =
            std::ffi::CString::new(dtype).expect("Error creating CString");
        let mut c_ret: u32 = 0;
        let res = unsafe {
            tiledb_datatype_from_str(c_dtype.as_c_str().as_ptr(), &mut c_ret)
        };

        if res == TILEDB_OK {
            Datatype::from_u32(c_ret)
        } else {
            None
        }
    }

    pub fn from_u32(dtype: u32) -> Option<Datatype> {
        match dtype {
            0 => Some(Datatype::INT32),
            1 => Some(Datatype::INT64),
            2 => Some(Datatype::FLOAT32),
            3 => Some(Datatype::FLOAT64),
            4 => Some(Datatype::CHAR),
            5 => Some(Datatype::INT8),
            6 => Some(Datatype::UINT8),
            7 => Some(Datatype::INT16),
            8 => Some(Datatype::UINT16),
            9 => Some(Datatype::UINT32),
            10 => Some(Datatype::UINT64),
            11 => Some(Datatype::STRING_ASCII),
            12 => Some(Datatype::STRING_UTF8),
            13 => Some(Datatype::STRING_UTF16),
            14 => Some(Datatype::STRING_UTF32),
            15 => Some(Datatype::STRING_UCS2),
            16 => Some(Datatype::STRING_UCS4),
            17 => Some(Datatype::ANY),
            18 => Some(Datatype::DATETIME_YEAR),
            19 => Some(Datatype::DATETIME_MONTH),
            20 => Some(Datatype::DATETIME_WEEK),
            21 => Some(Datatype::DATETIME_DAY),
            22 => Some(Datatype::DATETIME_HR),
            23 => Some(Datatype::DATETIME_MIN),
            24 => Some(Datatype::DATETIME_SEC),
            25 => Some(Datatype::DATETIME_MS),
            26 => Some(Datatype::DATETIME_US),
            27 => Some(Datatype::DATETIME_NS),
            28 => Some(Datatype::DATETIME_PS),
            29 => Some(Datatype::DATETIME_FS),
            30 => Some(Datatype::DATETIME_AS),
            31 => Some(Datatype::TIME_HR),
            32 => Some(Datatype::TIME_MIN),
            33 => Some(Datatype::TIME_SEC),
            34 => Some(Datatype::TIME_MS),
            35 => Some(Datatype::TIME_US),
            36 => Some(Datatype::TIME_NS),
            37 => Some(Datatype::TIME_PS),
            38 => Some(Datatype::TIME_FS),
            39 => Some(Datatype::TIME_AS),
            40 => Some(Datatype::BLOB),
            41 => Some(Datatype::BOOL),
            42 => Some(Datatype::GEOM_WKB),
            43 => Some(Datatype::GEOM_WKT),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datatype_test() {
        for i in 0..256 {
            prinln!("{}", i);
        }
    }
}
