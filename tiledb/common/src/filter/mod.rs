mod webp;

use thiserror::Error;

#[cfg(feature = "option-subset")]
use tiledb_utils::option::OptionSubset;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::datatype::Datatype;

pub use self::webp::*;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum CompressionType {
    Bzip2,
    Dictionary,
    Gzip,
    Lz4,
    Rle,
    Zstd,
    Delta {
        reinterpret_datatype: Option<Datatype>,
    },
    DoubleDelta {
        reinterpret_datatype: Option<Datatype>,
    },
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum ChecksumType {
    Md5,
    Sha256,
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct CompressionData {
    pub kind: CompressionType,
    pub level: Option<i32>,
}

impl CompressionData {
    pub fn new(kind: CompressionType) -> Self {
        CompressionData { kind, level: None }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum ScaleFloatByteWidth {
    I8,
    I16,
    I32,
    #[default] // keep in sync with tiledb/sm/filter/float_scaling_filter.h
    I64,
}

#[derive(Clone, Debug, Error)]
pub enum ScaleFloatByteWidthError {
    #[error("Invalid byte width: {0}")]
    InvalidByteWidth(usize),
}

impl ScaleFloatByteWidth {
    pub fn output_datatype(&self) -> Datatype {
        match *self {
            Self::I8 => Datatype::Int8,
            Self::I16 => Datatype::Int16,
            Self::I32 => Datatype::Int32,
            Self::I64 => Datatype::Int64,
        }
    }
}

impl From<ScaleFloatByteWidth> for std::ffi::c_ulonglong {
    fn from(value: ScaleFloatByteWidth) -> Self {
        let c = match value {
            ScaleFloatByteWidth::I8 => std::mem::size_of::<i8>(),
            ScaleFloatByteWidth::I16 => std::mem::size_of::<i16>(),
            ScaleFloatByteWidth::I32 => std::mem::size_of::<i32>(),
            ScaleFloatByteWidth::I64 => std::mem::size_of::<i64>(),
        };
        c as Self
    }
}

impl TryFrom<std::ffi::c_ulonglong> for ScaleFloatByteWidth {
    type Error = ScaleFloatByteWidthError;
    fn try_from(value: std::ffi::c_ulonglong) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::I8),
            2 => Ok(Self::I16),
            4 => Ok(Self::I32),
            8 => Ok(Self::I64),
            v => Err(ScaleFloatByteWidthError::InvalidByteWidth(v as usize)),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum FilterData {
    None,
    BitShuffle,
    ByteShuffle,
    BitWidthReduction {
        max_window: Option<u32>,
    },
    Checksum(ChecksumType),
    Compression(CompressionData),
    PositiveDelta {
        max_window: Option<u32>,
    },
    ScaleFloat {
        byte_width: Option<ScaleFloatByteWidth>,
        factor: Option<f64>,
        offset: Option<f64>,
    },
    WebP {
        input_format: WebPFilterInputFormat,
        lossless: Option<bool>,
        quality: Option<f32>,
    },
    Xor,
}

impl FilterData {
    /// Returns the output datatype when this filter is applied to the input type.
    /// If the filter cannot accept the requested input type, None is returned.
    pub fn transform_datatype(&self, input: &Datatype) -> Option<Datatype> {
        /*
         * Note to developers, this code should be kept in sync with
         * tiledb/sm/filters/filter/ functions
         * - `accepts_input_datatype`
         * - `output_datatype`
         *
         * Those functions are not part of the external C API.
         */
        match *self {
            FilterData::None => Some(*input),
            FilterData::BitShuffle => Some(*input),
            FilterData::ByteShuffle => Some(*input),
            FilterData::Checksum(_) => Some(*input),
            FilterData::BitWidthReduction { .. }
            | FilterData::PositiveDelta { .. } => {
                if input.is_integral_type()
                    || input.is_datetime_type()
                    || input.is_time_type()
                    || input.is_byte_type()
                {
                    Some(*input)
                } else {
                    None
                }
            }
            FilterData::Compression(CompressionData { kind, .. }) => match kind
            {
                CompressionType::Delta {
                    reinterpret_datatype,
                }
                | CompressionType::DoubleDelta {
                    reinterpret_datatype,
                } => reinterpret_datatype.map_or(Some(*input), |dtype| {
                    if !dtype.is_real_type() {
                        Some(dtype)
                    } else {
                        None
                    }
                }),
                _ => Some(*input),
            },
            FilterData::ScaleFloat { byte_width, .. } => {
                let input_size = input.size() as usize;
                if input_size == std::mem::size_of::<f32>()
                    || input_size == std::mem::size_of::<f64>()
                {
                    Some(
                        byte_width
                            .unwrap_or(ScaleFloatByteWidth::default())
                            .output_datatype(),
                    )
                } else {
                    None
                }
            }
            FilterData::WebP { .. } => {
                if *input == Datatype::UInt8 {
                    Some(Datatype::UInt8)
                } else {
                    None
                }
            }
            FilterData::Xor => match input.size() {
                0 => Some(Datatype::Int8),
                2 => Some(Datatype::Int16),
                4 => Some(Datatype::Int32),
                8 => Some(Datatype::Int64),
                _ => None,
            },
        }
    }
}
