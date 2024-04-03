use crate::error::Error;
use crate::Result as TileDBResult;

#[derive(Clone, Debug, PartialEq)]
pub enum FilterType {
    None,
    Gzip,
    Zstd,
    Lz4,
    Rle,
    Bzip2,
    DoubleDelta,
    BitWidthReduction,
    BitShuffle,
    ByteShuffle,
    PositiveDelta,
    ChecksumMD5,
    ChecksumSHA256,
    Dictionary,
    ScaleFloat,
    Xor,
    WebP,
    Delta,
}

impl FilterType {
    pub(crate) fn capi_enum(&self) -> ffi::tiledb_filter_type_t {
        match *self {
            FilterType::None => ffi::tiledb_filter_type_t_TILEDB_FILTER_NONE,
            FilterType::Gzip => ffi::tiledb_filter_type_t_TILEDB_FILTER_GZIP,
            FilterType::Zstd => ffi::tiledb_filter_type_t_TILEDB_FILTER_ZSTD,
            FilterType::Lz4 => ffi::tiledb_filter_type_t_TILEDB_FILTER_LZ4,
            FilterType::Rle => ffi::tiledb_filter_type_t_TILEDB_FILTER_RLE,
            FilterType::Bzip2 => ffi::tiledb_filter_type_t_TILEDB_FILTER_BZIP2,
            FilterType::DoubleDelta => {
                ffi::tiledb_filter_type_t_TILEDB_FILTER_DOUBLE_DELTA
            }
            FilterType::BitWidthReduction => {
                ffi::tiledb_filter_type_t_TILEDB_FILTER_BIT_WIDTH_REDUCTION
            }
            FilterType::BitShuffle => {
                ffi::tiledb_filter_type_t_TILEDB_FILTER_BITSHUFFLE
            }
            FilterType::ByteShuffle => {
                ffi::tiledb_filter_type_t_TILEDB_FILTER_BYTESHUFFLE
            }
            FilterType::PositiveDelta => {
                ffi::tiledb_filter_type_t_TILEDB_FILTER_POSITIVE_DELTA
            }
            FilterType::ChecksumMD5 => {
                ffi::tiledb_filter_type_t_TILEDB_FILTER_CHECKSUM_MD5
            }
            FilterType::ChecksumSHA256 => {
                ffi::tiledb_filter_type_t_TILEDB_FILTER_CHECKSUM_SHA256
            }
            FilterType::Dictionary => {
                ffi::tiledb_filter_type_t_TILEDB_FILTER_DICTIONARY
            }
            FilterType::ScaleFloat => {
                ffi::tiledb_filter_type_t_TILEDB_FILTER_SCALE_FLOAT
            }
            FilterType::Xor => ffi::tiledb_filter_type_t_TILEDB_FILTER_XOR,
            FilterType::WebP => ffi::tiledb_filter_type_t_TILEDB_FILTER_WEBP,
            FilterType::Delta => ffi::tiledb_filter_type_t_TILEDB_FILTER_DELTA,
        }
    }

    pub fn to_string(&self) -> TileDBResult<String> {
        let mut c_str = std::ptr::null::<std::os::raw::c_char>();
        let res = unsafe {
            ffi::tiledb_filter_type_to_str(self.capi_enum(), &mut c_str)
        };
        if res == ffi::TILEDB_OK {
            let c_msg = unsafe { std::ffi::CStr::from_ptr(c_str) };
            Ok(String::from(c_msg.to_string_lossy()))
        } else {
            Err(Error::LibTileDB(format!(
                "Error converting filter type: {:?} to string",
                self
            )))
        }
    }

    pub fn from_string(fs: &str) -> TileDBResult<FilterType> {
        let c_ftype =
            std::ffi::CString::new(fs).expect("Error creating CString");
        std::ffi::CString::new(fs).expect("Error creating CString");
        let mut c_ret: u32 = 0;
        let res = unsafe {
            ffi::tiledb_filter_type_from_str(
                c_ftype.as_c_str().as_ptr(),
                &mut c_ret,
            )
        };

        if res == ffi::TILEDB_OK {
            FilterType::try_from(c_ret)
        } else {
            Err(Error::LibTileDB(format!("Invalid filter type: {}", fs)))
        }
    }
}

impl TryFrom<u32> for FilterType {
    type Error = crate::error::Error;
    fn try_from(value: u32) -> TileDBResult<FilterType> {
        match value {
            ffi::tiledb_filter_type_t_TILEDB_FILTER_NONE => {
                Ok(FilterType::None)
            }
            ffi::tiledb_filter_type_t_TILEDB_FILTER_GZIP => {
                Ok(FilterType::Gzip)
            }
            ffi::tiledb_filter_type_t_TILEDB_FILTER_ZSTD => {
                Ok(FilterType::Zstd)
            }
            ffi::tiledb_filter_type_t_TILEDB_FILTER_LZ4 => Ok(FilterType::Lz4),
            ffi::tiledb_filter_type_t_TILEDB_FILTER_RLE => Ok(FilterType::Rle),
            ffi::tiledb_filter_type_t_TILEDB_FILTER_BZIP2 => {
                Ok(FilterType::Bzip2)
            }
            ffi::tiledb_filter_type_t_TILEDB_FILTER_DOUBLE_DELTA => {
                Ok(FilterType::DoubleDelta)
            }
            ffi::tiledb_filter_type_t_TILEDB_FILTER_BIT_WIDTH_REDUCTION => {
                Ok(FilterType::BitWidthReduction)
            }
            ffi::tiledb_filter_type_t_TILEDB_FILTER_BITSHUFFLE => {
                Ok(FilterType::BitShuffle)
            }
            ffi::tiledb_filter_type_t_TILEDB_FILTER_BYTESHUFFLE => {
                Ok(FilterType::ByteShuffle)
            }
            ffi::tiledb_filter_type_t_TILEDB_FILTER_POSITIVE_DELTA => {
                Ok(FilterType::PositiveDelta)
            }
            ffi::tiledb_filter_type_t_TILEDB_FILTER_CHECKSUM_MD5 => {
                Ok(FilterType::ChecksumMD5)
            }
            ffi::tiledb_filter_type_t_TILEDB_FILTER_CHECKSUM_SHA256 => {
                Ok(FilterType::ChecksumSHA256)
            }
            ffi::tiledb_filter_type_t_TILEDB_FILTER_DICTIONARY => {
                Ok(FilterType::Dictionary)
            }
            ffi::tiledb_filter_type_t_TILEDB_FILTER_SCALE_FLOAT => {
                Ok(FilterType::ScaleFloat)
            }
            ffi::tiledb_filter_type_t_TILEDB_FILTER_XOR => Ok(FilterType::Xor),
            ffi::tiledb_filter_type_t_TILEDB_FILTER_WEBP => {
                Ok(FilterType::WebP)
            }
            ffi::tiledb_filter_type_t_TILEDB_FILTER_DELTA => {
                Ok(FilterType::Delta)
            }
            _ => Err(Self::Error::LibTileDB(format!(
                "Invalid filter type: {}",
                value
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_type_roundtrips() {
        for i in 0..256 {
            let maybe_ftype = FilterType::try_from(i);
            if maybe_ftype.is_ok() {
                let ftype = maybe_ftype.unwrap();
                let ftype_str =
                    ftype.to_string().expect("Error creating string.");
                let str_ftype = FilterType::from_string(&ftype_str)
                    .expect("Error round tripping filter type string.");
                assert_eq!(str_ftype, ftype);
            }
        }
    }
}
