use thiserror::Error;
use tiledb_common::filter::{
    ChecksumType, CompressionData, CompressionType, FilterData,
};

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Invalid discriminant for {}: {0}", std::any::type_name::<FilterType>())]
    InvalidDiscriminant(u64),
}

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
            FilterData::BitShuffle => FilterType::BitShuffle,
            FilterData::ByteShuffle => FilterType::ByteShuffle,
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
