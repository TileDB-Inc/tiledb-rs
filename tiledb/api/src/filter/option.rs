use thiserror::Error;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Invalid discriminant for {}: {0}", std::any::type_name::<FilterOption>())]
    InvalidDiscriminant(u64),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum FilterOption {
    CompressionLevel,
    BitWidthMaxWindow,
    PositiveDeltaMaxWindow,
    ScaleFloatByteWidth,
    ScaleFloatFactor,
    ScaleFloatOffset,
    WebPQuality,
    WebPInputFormat,
    WebPLossless,
    CompressionReinterpretDatatype,
}

impl From<FilterOption> for ffi::tiledb_filter_option_t {
    fn from(value: FilterOption) -> Self {
        let ffi_enum = match value {
            FilterOption::CompressionLevel => {
                ffi::tiledb_filter_option_t_TILEDB_COMPRESSION_LEVEL
            },
            FilterOption::BitWidthMaxWindow => {
                ffi::tiledb_filter_option_t_TILEDB_BIT_WIDTH_MAX_WINDOW
            },
            FilterOption::PositiveDeltaMaxWindow => {
                ffi::tiledb_filter_option_t_TILEDB_POSITIVE_DELTA_MAX_WINDOW
            },
            FilterOption::ScaleFloatByteWidth => {
                ffi::tiledb_filter_option_t_TILEDB_SCALE_FLOAT_BYTEWIDTH
            },
            FilterOption::ScaleFloatFactor => {
                ffi::tiledb_filter_option_t_TILEDB_SCALE_FLOAT_FACTOR
            },
            FilterOption::ScaleFloatOffset => {
                ffi::tiledb_filter_option_t_TILEDB_SCALE_FLOAT_OFFSET
            },
            FilterOption::WebPQuality => {
                ffi::tiledb_filter_option_t_TILEDB_WEBP_QUALITY
            },
            FilterOption::WebPInputFormat => {
                ffi::tiledb_filter_option_t_TILEDB_WEBP_INPUT_FORMAT
            },
            FilterOption::WebPLossless => {
                ffi::tiledb_filter_option_t_TILEDB_WEBP_LOSSLESS
            },
            FilterOption::CompressionReinterpretDatatype => {
                ffi::tiledb_filter_option_t_TILEDB_COMPRESSION_REINTERPRET_DATATYPE
            },
        };
        ffi_enum as ffi::tiledb_filter_option_t
    }
}

impl TryFrom<u32> for FilterOption {
    type Error = Error;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            ffi::tiledb_filter_option_t_TILEDB_COMPRESSION_LEVEL => {
                Ok(FilterOption::CompressionLevel)
            }
            ffi::tiledb_filter_option_t_TILEDB_BIT_WIDTH_MAX_WINDOW => {
                Ok(FilterOption::BitWidthMaxWindow)
            }
            ffi::tiledb_filter_option_t_TILEDB_POSITIVE_DELTA_MAX_WINDOW => {
                Ok(FilterOption::PositiveDeltaMaxWindow)
            }
            ffi::tiledb_filter_option_t_TILEDB_SCALE_FLOAT_BYTEWIDTH => Ok(FilterOption::ScaleFloatByteWidth),
            ffi::tiledb_filter_option_t_TILEDB_SCALE_FLOAT_FACTOR => Ok(FilterOption::ScaleFloatFactor),
            ffi::tiledb_filter_option_t_TILEDB_SCALE_FLOAT_OFFSET => Ok(FilterOption::ScaleFloatOffset),
            ffi::tiledb_filter_option_t_TILEDB_WEBP_QUALITY => Ok(FilterOption::WebPQuality),
            ffi::tiledb_filter_option_t_TILEDB_WEBP_INPUT_FORMAT => Ok(FilterOption::WebPInputFormat),
            ffi::tiledb_filter_option_t_TILEDB_WEBP_LOSSLESS => Ok(FilterOption::WebPLossless),
            ffi::tiledb_filter_option_t_TILEDB_COMPRESSION_REINTERPRET_DATATYPE => Ok(FilterOption::CompressionReinterpretDatatype),
            _ => Err(Error::InvalidDiscriminant(value as u64))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_option_roundtrips() {
        let mut ok = 0;
        for i in 0..256 {
            let fopt = FilterOption::try_from(i);
            if let Ok(fopt) = fopt {
                ok += 1;
                assert_eq!(i, fopt.into());
            }
        }
        assert_eq!(ok, 10);
    }
}
