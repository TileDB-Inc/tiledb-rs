use thiserror::Error;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "option-subset")]
use tiledb_utils::option::OptionSubset;

#[derive(Clone, Debug, Error)]
pub enum WebPFilterError {
    #[error("Invalid discriminant for {}: {0}", std::any::type_name::<WebPFilterInputFormat>())]
    InvalidDiscriminant(u64),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum WebPFilterInputFormat {
    Rgb,
    Bgr,
    Rgba,
    Bgra,
}

impl WebPFilterInputFormat {
    pub fn pixel_depth(&self) -> usize {
        match *self {
            WebPFilterInputFormat::Rgb | WebPFilterInputFormat::Bgr => 3,
            WebPFilterInputFormat::Rgba | WebPFilterInputFormat::Bgra => 4,
        }
    }
}

impl From<WebPFilterInputFormat> for ffi::tiledb_filter_webp_format_t {
    fn from(value: WebPFilterInputFormat) -> Self {
        let ffi_enum = match value {
            WebPFilterInputFormat::Rgb => {
                ffi::tiledb_filter_webp_format_t_TILEDB_WEBP_RGB
            }
            WebPFilterInputFormat::Bgr => {
                ffi::tiledb_filter_webp_format_t_TILEDB_WEBP_BGR
            }
            WebPFilterInputFormat::Rgba => {
                ffi::tiledb_filter_webp_format_t_TILEDB_WEBP_RGBA
            }
            WebPFilterInputFormat::Bgra => {
                ffi::tiledb_filter_webp_format_t_TILEDB_WEBP_BGRA
            }
        };
        ffi_enum as ffi::tiledb_filter_webp_format_t
    }
}

impl TryFrom<u32> for WebPFilterInputFormat {
    type Error = WebPFilterError;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            ffi::tiledb_filter_webp_format_t_TILEDB_WEBP_RGB => {
                Ok(WebPFilterInputFormat::Rgb)
            }
            ffi::tiledb_filter_webp_format_t_TILEDB_WEBP_BGR => {
                Ok(WebPFilterInputFormat::Bgr)
            }
            ffi::tiledb_filter_webp_format_t_TILEDB_WEBP_RGBA => {
                Ok(WebPFilterInputFormat::Rgba)
            }
            ffi::tiledb_filter_webp_format_t_TILEDB_WEBP_BGRA => {
                Ok(WebPFilterInputFormat::Bgra)
            }
            _ => Err(WebPFilterError::InvalidDiscriminant(value as u64)),
        }
    }
}
