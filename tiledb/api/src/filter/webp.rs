use serde::{Deserialize, Serialize};
use util::option::OptionSubset;

use crate::Result as TileDBResult;

#[derive(
    Copy, Clone, Debug, Deserialize, Eq, OptionSubset, PartialEq, Serialize,
)]
pub enum WebPFilterInputFormat {
    Rgb,
    Bgr,
    Rgba,
    Bgra,
}

impl WebPFilterInputFormat {
    pub(crate) fn capi_enum(&self) -> ffi::tiledb_filter_webp_format_t {
        let ffi_enum = match *self {
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

    pub fn pixel_depth(&self) -> usize {
        match *self {
            WebPFilterInputFormat::Rgb | WebPFilterInputFormat::Bgr => 3,
            WebPFilterInputFormat::Rgba | WebPFilterInputFormat::Bgra => 4,
        }
    }
}

impl TryFrom<u32> for WebPFilterInputFormat {
    type Error = crate::error::Error;
    fn try_from(value: u32) -> TileDBResult<WebPFilterInputFormat> {
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
            _ => Err(Self::Error::LibTileDB(format!(
                "Invalid WebP filter format type: {}",
                value
            ))),
        }
    }
}
