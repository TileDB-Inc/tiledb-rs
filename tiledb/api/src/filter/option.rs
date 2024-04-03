use crate::Result as TileDBResult;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
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

impl FilterOption {
    pub(crate) fn capi_enum(&self) -> ffi::tiledb_filter_option_t {
        let ffi_enum = match *self {
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
    type Error = crate::error::Error;
    fn try_from(value: u32) -> TileDBResult<Self> {
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
            _ => Err(Self::Error::LibTileDB(format!(
                "Invalid filter option type: {}",
                value
            ))),
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
            if fopt.is_ok() {
                ok += 1;
            }
        }
        assert_eq!(ok, 10);
    }
}
