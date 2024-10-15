use std::fmt::{Display, Formatter, Result as FmtResult};

use thiserror::Error;
use tiledb_common::filter::{
    ChecksumType, CompressionData, CompressionType, FilterData,
};

use crate::{FromStringCore, ToStringCore};

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Invalid discriminant for {}: {0}", std::any::type_name::<FilterType>())]
    InvalidDiscriminant(u64),
    #[error("Internal error formatting {0}")]
    InternalString(FilterType),
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug, PartialEq)]
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

impl ToStringCore for FilterType {
    type Error = Error;

    fn to_string_core(&self) -> Result<String> {
        let mut c_str = std::ptr::null::<std::os::raw::c_char>();
        let res = unsafe {
            ffi::tiledb_filter_type_to_str((*self).into(), &mut c_str)
        };
        if res == ffi::TILEDB_OK {
            let c_msg = unsafe { std::ffi::CStr::from_ptr(c_str) };
            Ok(String::from(c_msg.to_string_lossy()))
        } else {
            Err(Error::InternalString(*self))
        }
    }
}

impl FromStringCore for FilterType {
    fn from_string_core(s: &str) -> Option<Self> {
        let c_ftype =
            std::ffi::CString::new(s).expect("Error creating CString");
        std::ffi::CString::new(s).expect("Error creating CString");
        let mut c_ret: u32 = 0;
        let res = unsafe {
            ffi::tiledb_filter_type_from_str(
                c_ftype.as_c_str().as_ptr(),
                &mut c_ret,
            )
        };

        if res == ffi::TILEDB_OK {
            FilterType::try_from(c_ret).ok()
        } else {
            None
        }
    }
}

impl Display for FilterType {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self.to_string_core() {
            Ok(s) => write!(f, "{}", s),
            Err(e) => write!(f, "<FilterType: {}>", e),
        }
    }
}

impl From<FilterType> for ffi::tiledb_filter_type_t {
    fn from(value: FilterType) -> Self {
        match value {
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
}

impl TryFrom<u32> for FilterType {
    type Error = Error;
    fn try_from(value: u32) -> std::result::Result<Self, Self::Error> {
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
            _ => Err(Error::InvalidDiscriminant(value as u64)),
        }
    }
}

impl From<&FilterData> for FilterType {
    fn from(value: &FilterData) -> Self {
        match value {
            FilterData::None => FilterType::None,
            FilterData::BitShuffle { .. } => FilterType::BitShuffle,
            FilterData::ByteShuffle { .. } => FilterType::ByteShuffle,
            FilterData::BitWidthReduction { .. } => {
                FilterType::BitWidthReduction
            }
            FilterData::Checksum(ChecksumType::Md5) => FilterType::ChecksumMD5,
            FilterData::Checksum(ChecksumType::Sha256) => {
                FilterType::ChecksumSHA256
            }
            FilterData::Compression(CompressionData {
                kind: CompressionType::Bzip2,
                ..
            }) => FilterType::Bzip2,
            FilterData::Compression(CompressionData {
                kind: CompressionType::Delta { .. },
                ..
            }) => FilterType::Delta,
            FilterData::Compression(CompressionData {
                kind: CompressionType::Dictionary,
                ..
            }) => FilterType::Dictionary,
            FilterData::Compression(CompressionData {
                kind: CompressionType::DoubleDelta { .. },
                ..
            }) => FilterType::DoubleDelta,
            FilterData::Compression(CompressionData {
                kind: CompressionType::Gzip,
                ..
            }) => FilterType::Gzip,
            FilterData::Compression(CompressionData {
                kind: CompressionType::Lz4,
                ..
            }) => FilterType::Lz4,
            FilterData::Compression(CompressionData {
                kind: CompressionType::Rle,
                ..
            }) => FilterType::Rle,
            FilterData::Compression(CompressionData {
                kind: CompressionType::Zstd,
                ..
            }) => FilterType::Zstd,
            FilterData::PositiveDelta { .. } => FilterType::PositiveDelta,
            FilterData::ScaleFloat { .. } => FilterType::ScaleFloat,
            FilterData::WebP { .. } => FilterType::WebP,
            FilterData::Xor => FilterType::Xor,
        }
    }
}

impl From<FilterData> for FilterType {
    fn from(value: FilterData) -> Self {
        FilterType::from(&value)
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
                    ftype.to_string_core().expect("Error creating string.");
                let str_ftype = FilterType::from_string_core(&ftype_str)
                    .expect("Error round tripping filter type string.");
                assert_eq!(str_ftype, ftype);
            }
        }
    }
}
