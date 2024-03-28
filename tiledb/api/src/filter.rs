use std::borrow::Borrow;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::context::Context;
use crate::error::Error;
use crate::{Datatype, Result as TileDBResult};

pub use crate::filter_list::{Builder as FilterListBuilder, FilterList};

#[derive(Copy, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CompressionType {
    Bzip2,
    Delta,
    Dictionary,
    DoubleDelta,
    Gzip,
    Lz4,
    Rle,
    Zstd,
}

#[derive(Copy, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ChecksumType {
    Md5,
    Sha256,
}

#[derive(Copy, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum WebPFilterInputFormat {
    None,
    Rgb,
    Bgr,
    Rgba,
    Bgra,
}

impl WebPFilterInputFormat {
    pub(crate) fn capi_enum(&self) -> u32 {
        let ffi_enum = match *self {
            WebPFilterInputFormat::None => ffi::WebPFilterInputFormat::NONE,
            WebPFilterInputFormat::Rgb => ffi::WebPFilterInputFormat::RGB,
            WebPFilterInputFormat::Bgr => ffi::WebPFilterInputFormat::BGR,
            WebPFilterInputFormat::Rgba => ffi::WebPFilterInputFormat::RGBA,
            WebPFilterInputFormat::Bgra => ffi::WebPFilterInputFormat::BGRA,
        };
        ffi_enum as u32
    }
}

impl TryFrom<u32> for WebPFilterInputFormat {
    type Error = crate::error::Error;
    fn try_from(value: u32) -> TileDBResult<WebPFilterInputFormat> {
        match value {
            0 => Ok(WebPFilterInputFormat::None),
            1 => Ok(WebPFilterInputFormat::Rgb),
            2 => Ok(WebPFilterInputFormat::Bgr),
            3 => Ok(WebPFilterInputFormat::Rgba),
            4 => Ok(WebPFilterInputFormat::Bgra),
            _ => Err(Self::Error::from(format!(
                "Invalid webp filter type: {}",
                value
            ))),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CompressionData {
    pub kind: CompressionType,
    pub level: Option<i32>,
    pub reinterpret_datatype: Option<Datatype>,
}

impl CompressionData {
    pub fn new(kind: CompressionType) -> Self {
        CompressionData {
            kind,
            level: None,
            reinterpret_datatype: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Serialize)]
pub enum ScaleFloatByteWidth {
    I8,
    I16,
    I32,
    #[default] // keep in sync with tiledb/sm/filter/float_scaling_filter.h
    I64,
}

impl ScaleFloatByteWidth {
    pub(crate) fn capi_enum(&self) -> usize {
        match *self {
            Self::I8 => std::mem::size_of::<i8>(),
            Self::I16 => std::mem::size_of::<i16>(),
            Self::I32 => std::mem::size_of::<i32>(),
            Self::I64 => std::mem::size_of::<i64>(),
        }
    }

    pub fn output_datatype(&self) -> Datatype {
        match *self {
            Self::I8 => Datatype::Int8,
            Self::I16 => Datatype::Int16,
            Self::I32 => Datatype::Int32,
            Self::I64 => Datatype::Int64,
        }
    }
}

impl TryFrom<std::ffi::c_ulonglong> for ScaleFloatByteWidth {
    type Error = crate::error::Error;
    fn try_from(value: std::ffi::c_ulonglong) -> TileDBResult<Self> {
        match value {
            1 => Ok(Self::I8),
            2 => Ok(Self::I16),
            4 => Ok(Self::I32),
            8 => Ok(Self::I64),
            v => Err(Self::Error::from(format!(
                "Invalid scale float byte width: {}",
                v
            ))),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
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
        input_format: Option<WebPFilterInputFormat>,
        lossless: Option<bool>,
        quality: Option<f32>,
    },
    Xor,
}

impl FilterData {
    pub fn construct<'ctx>(
        &self,
        context: &'ctx Context,
    ) -> TileDBResult<Filter<'ctx>> {
        Filter::create(context, self)
    }

    pub fn capi_enum(&self) -> ffi::FilterType {
        match *self {
            FilterData::None => ffi::FilterType::None,
            FilterData::BitShuffle { .. } => ffi::FilterType::BitShuffle,
            FilterData::ByteShuffle { .. } => ffi::FilterType::ByteShuffle,
            FilterData::BitWidthReduction { .. } => {
                ffi::FilterType::BitWidthReduction
            }
            FilterData::Checksum(ChecksumType::Md5) => {
                ffi::FilterType::ChecksumMD5
            }
            FilterData::Checksum(ChecksumType::Sha256) => {
                ffi::FilterType::ChecksumSHA256
            }
            FilterData::Compression(CompressionData {
                kind: CompressionType::Bzip2,
                ..
            }) => ffi::FilterType::Bzip2,
            FilterData::Compression(CompressionData {
                kind: CompressionType::Delta,
                ..
            }) => ffi::FilterType::Delta,
            FilterData::Compression(CompressionData {
                kind: CompressionType::Dictionary,
                ..
            }) => ffi::FilterType::Dictionary,
            FilterData::Compression(CompressionData {
                kind: CompressionType::DoubleDelta,
                ..
            }) => ffi::FilterType::DoubleDelta,
            FilterData::Compression(CompressionData {
                kind: CompressionType::Gzip,
                ..
            }) => ffi::FilterType::Gzip,
            FilterData::Compression(CompressionData {
                kind: CompressionType::Lz4,
                ..
            }) => ffi::FilterType::Lz4,
            FilterData::Compression(CompressionData {
                kind: CompressionType::Rle,
                ..
            }) => ffi::FilterType::Rle,
            FilterData::Compression(CompressionData {
                kind: CompressionType::Zstd,
                ..
            }) => ffi::FilterType::Zstd,
            FilterData::PositiveDelta { .. } => ffi::FilterType::PositiveDelta,
            FilterData::ScaleFloat { .. } => ffi::FilterType::ScaleFloat,
            FilterData::WebP { .. } => ffi::FilterType::WebP,
            FilterData::Xor => ffi::FilterType::Xor,
        }
    }

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
            FilterData::Compression(CompressionData {
                kind,
                reinterpret_datatype,
                ..
            }) => match kind {
                CompressionType::Delta | CompressionType::DoubleDelta => {
                    // these filters do not accept floating point
                    let check_type =
                        if let Some(Datatype::Any) = reinterpret_datatype {
                            *input
                        } else if let Some(reinterpret_datatype) =
                            reinterpret_datatype
                        {
                            reinterpret_datatype
                        } else {
                            return None;
                        };
                    if check_type.is_real_type() {
                        None
                    } else {
                        Some(check_type)
                    }
                }
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
                1 => Some(Datatype::Int8),
                2 => Some(Datatype::Int16),
                4 => Some(Datatype::Int32),
                8 => Some(Datatype::Int64),
                _ => None,
            },
        }
    }
}

impl<'ctx> TryFrom<&Filter<'ctx>> for FilterData {
    type Error = crate::error::Error;

    fn try_from(filter: &Filter<'ctx>) -> TileDBResult<Self> {
        filter.filter_data()
    }
}

impl<'ctx> crate::Factory<'ctx> for FilterData {
    type Item = Filter<'ctx>;

    fn create(&self, context: &'ctx Context) -> TileDBResult<Self::Item> {
        Filter::create(context, self)
    }
}

pub(crate) enum RawFilter {
    Owned(*mut ffi::tiledb_filter_t),
}

impl Deref for RawFilter {
    type Target = *mut ffi::tiledb_filter_t;
    fn deref(&self) -> &Self::Target {
        match *self {
            RawFilter::Owned(ref ffi) => ffi,
        }
    }
}

impl Drop for RawFilter {
    fn drop(&mut self) {
        let RawFilter::Owned(ref mut ffi) = *self;
        unsafe { ffi::tiledb_filter_free(ffi) }
    }
}

pub struct Filter<'ctx> {
    context: &'ctx Context,
    pub(crate) raw: RawFilter,
}

impl<'ctx> Filter<'ctx> {
    pub fn capi(&self) -> *mut ffi::tiledb_filter_t {
        *self.raw
    }

    pub(crate) fn new(context: &'ctx Context, raw: RawFilter) -> Self {
        Filter { context, raw }
    }

    pub fn create<F>(
        context: &'ctx Context,
        filter_data: F,
    ) -> TileDBResult<Self>
    where
        F: Borrow<FilterData>,
    {
        let filter_data = filter_data.borrow();
        let c_context = context.capi();
        let mut c_filter: *mut ffi::tiledb_filter_t = out_ptr!();
        let ftype = filter_data.capi_enum() as u32;
        let res = unsafe {
            ffi::tiledb_filter_alloc(c_context, ftype, &mut c_filter)
        };
        if res != ffi::TILEDB_OK {
            return Err(context.expect_last_error());
        }

        let raw = RawFilter::Owned(c_filter);

        match *filter_data {
            FilterData::None => (),
            FilterData::BitShuffle { .. } => (),
            FilterData::ByteShuffle { .. } => (),
            FilterData::BitWidthReduction { max_window } => {
                if let Some(max_window) = max_window {
                    let c_size = max_window as std::ffi::c_uint;
                    Self::set_option(
                        context,
                        *raw,
                        ffi::FilterOption::BIT_WIDTH_MAX_WINDOW,
                        c_size,
                    )?;
                }
            }
            FilterData::Checksum(ChecksumType::Md5) => (),
            FilterData::Checksum(ChecksumType::Sha256) => (),
            FilterData::Compression(CompressionData {
                level,
                reinterpret_datatype,
                ..
            }) => {
                if let Some(level) = level {
                    let c_level = level as std::ffi::c_int;
                    Self::set_option(
                        context,
                        *raw,
                        ffi::FilterOption::COMPRESSION_LEVEL,
                        c_level,
                    )?;
                }
                if let Some(reinterpret_datatype) = reinterpret_datatype {
                    let c_datatype =
                        reinterpret_datatype.capi_enum() as std::ffi::c_uchar;
                    Self::set_option(
                        context,
                        *raw,
                        ffi::FilterOption::COMPRESSION_REINTERPRET_DATATYPE,
                        c_datatype,
                    )?;
                }
            }
            FilterData::PositiveDelta { max_window } => {
                if let Some(max_window) = max_window {
                    let c_size = max_window as std::ffi::c_uint;
                    Self::set_option(
                        context,
                        *raw,
                        ffi::FilterOption::POSITIVE_DELTA_MAX_WINDOW,
                        c_size,
                    )?;
                }
            }
            FilterData::ScaleFloat {
                byte_width,
                factor,
                offset,
            } => {
                if let Some(byte_width) = byte_width {
                    let c_width = byte_width.capi_enum();
                    Self::set_option(
                        context,
                        *raw,
                        ffi::FilterOption::SCALE_FLOAT_BYTEWIDTH,
                        c_width,
                    )?;
                }

                if let Some(factor) = factor {
                    let c_factor = factor as std::ffi::c_double;
                    Self::set_option(
                        context,
                        *raw,
                        ffi::FilterOption::SCALE_FLOAT_FACTOR,
                        c_factor,
                    )?;
                }

                if let Some(offset) = offset {
                    let c_offset = offset as std::ffi::c_double;
                    Self::set_option(
                        context,
                        c_filter,
                        ffi::FilterOption::SCALE_FLOAT_OFFSET,
                        c_offset,
                    )?;
                }
            }
            FilterData::WebP {
                input_format,
                lossless,
                quality,
            } => {
                if let Some(input_format) = input_format {
                    let c_format =
                        input_format.capi_enum() as std::ffi::c_uchar;
                    Self::set_option(
                        context,
                        *raw,
                        ffi::FilterOption::WEBP_INPUT_FORMAT,
                        c_format,
                    )?;
                }

                if let Some(lossless) = lossless {
                    let c_lossless: std::ffi::c_uchar =
                        if lossless { 1 } else { 0 };
                    Self::set_option(
                        context,
                        *raw,
                        ffi::FilterOption::WEBP_LOSSLESS,
                        c_lossless,
                    )?;
                }

                if let Some(quality) = quality {
                    let c_quality = quality as std::ffi::c_float;
                    Self::set_option(
                        context,
                        *raw,
                        ffi::FilterOption::WEBP_QUALITY,
                        c_quality,
                    )?;
                }
            }
            FilterData::Xor => (),
        };

        Ok(Filter { context, raw })
    }

    pub fn filter_data(&self) -> TileDBResult<FilterData> {
        let mut c_ftype: u32 = 0;
        let res = unsafe {
            ffi::tiledb_filter_get_type(
                self.context.capi(),
                self.capi(),
                &mut c_ftype,
            )
        };
        if res != ffi::TILEDB_OK {
            return Err(self.context.expect_last_error());
        }

        let get_compression_data = |kind| -> TileDBResult<FilterData> {
            let level = Some(
                self.get_option::<i32>(ffi::FilterOption::COMPRESSION_LEVEL)?,
            );
            let reinterpret_datatype = Some({
                let dtype = self.get_option::<std::ffi::c_uchar>(
                    ffi::FilterOption::COMPRESSION_REINTERPRET_DATATYPE,
                )?;
                Datatype::try_from(dtype as ffi::tiledb_datatype_t).map_err(
                    |_| {
                        Error::from(format!(
                            "Invalid compression reinterpret datatype: {}",
                            dtype
                        ))
                    },
                )?
            });
            Ok(FilterData::Compression(CompressionData {
                kind,
                level,
                reinterpret_datatype,
            }))
        };

        match ffi::FilterType::from_u32(c_ftype) {
            None => Err(crate::error::Error::from(format!(
                "Invalid filter type: {}",
                c_ftype
            ))),
            Some(ffi::FilterType::None) => Ok(FilterData::None),
            Some(ffi::FilterType::Gzip) => {
                get_compression_data(CompressionType::Gzip)
            }
            Some(ffi::FilterType::Zstd) => {
                get_compression_data(CompressionType::Zstd)
            }
            Some(ffi::FilterType::Lz4) => {
                get_compression_data(CompressionType::Lz4)
            }
            Some(ffi::FilterType::Rle) => {
                get_compression_data(CompressionType::Rle)
            }
            Some(ffi::FilterType::Bzip2) => {
                get_compression_data(CompressionType::Bzip2)
            }
            Some(ffi::FilterType::Dictionary) => {
                get_compression_data(CompressionType::Dictionary)
            }
            Some(ffi::FilterType::DoubleDelta) => {
                get_compression_data(CompressionType::DoubleDelta)
            }
            Some(ffi::FilterType::Delta) => {
                get_compression_data(CompressionType::Delta)
            }
            Some(ffi::FilterType::BitShuffle) => Ok(FilterData::BitShuffle),
            Some(ffi::FilterType::ByteShuffle) => Ok(FilterData::ByteShuffle),
            Some(ffi::FilterType::Xor) => Ok(FilterData::Xor),
            Some(ffi::FilterType::BitWidthReduction) => {
                Ok(FilterData::BitWidthReduction {
                    max_window: Some(self.get_option::<u32>(
                        ffi::FilterOption::BIT_WIDTH_MAX_WINDOW,
                    )?),
                })
            }
            Some(ffi::FilterType::PositiveDelta) => {
                Ok(FilterData::PositiveDelta {
                    max_window: Some(self.get_option::<std::ffi::c_uint>(
                        ffi::FilterOption::POSITIVE_DELTA_MAX_WINDOW,
                    )?),
                })
            }
            Some(ffi::FilterType::ChecksumMD5) => {
                Ok(FilterData::Checksum(ChecksumType::Md5))
            }
            Some(ffi::FilterType::ChecksumSHA256) => {
                Ok(FilterData::Checksum(ChecksumType::Sha256))
            }
            Some(ffi::FilterType::ScaleFloat) => Ok(FilterData::ScaleFloat {
                byte_width: Some(ScaleFloatByteWidth::try_from(
                    self.get_option::<std::ffi::c_ulonglong>(
                        ffi::FilterOption::SCALE_FLOAT_BYTEWIDTH,
                    )?,
                )?),
                factor: Some(self.get_option::<std::ffi::c_double>(
                    ffi::FilterOption::SCALE_FLOAT_FACTOR,
                )?),
                offset: Some(self.get_option::<std::ffi::c_double>(
                    ffi::FilterOption::SCALE_FLOAT_OFFSET,
                )?),
            }),
            Some(ffi::FilterType::WebP) => Ok(FilterData::WebP {
                input_format: Some(WebPFilterInputFormat::try_from(
                    self.get_option::<u32>(
                        ffi::FilterOption::WEBP_INPUT_FORMAT,
                    )?,
                )?),
                lossless: Some(
                    self.get_option::<std::ffi::c_uchar>(
                        ffi::FilterOption::WEBP_LOSSLESS,
                    )? != 0,
                ),
                quality: Some(self.get_option::<std::ffi::c_float>(
                    ffi::FilterOption::WEBP_QUALITY,
                )?),
            }),
        }
    }

    fn get_option<T>(&self, fopt: ffi::FilterOption) -> TileDBResult<T> {
        let mut val: T = out_ptr!();
        let res = unsafe {
            ffi::tiledb_filter_get_option(
                self.context.capi(),
                self.capi(),
                fopt as u32,
                &mut val as *mut T as *mut std::ffi::c_void,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(val)
        } else {
            Err(self.context.expect_last_error())
        }
    }

    fn set_option<T>(
        context: &Context,
        raw: *mut ffi::tiledb_filter_t,
        fopt: ffi::FilterOption,
        val: T,
    ) -> TileDBResult<()> {
        let c_val = &val as *const T as *const std::ffi::c_void;
        let res = unsafe {
            ffi::tiledb_filter_set_option(
                context.capi(),
                raw,
                fopt as u32,
                c_val,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(context.expect_last_error())
        }
    }
}

impl<'ctx> Debug for Filter<'ctx> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self.filter_data() {
            Ok(data) => write!(f, "{:?}", data),
            Err(e) => write!(f, "<error reading filter data: {}", e),
        }
    }
}

impl<'c1, 'c2> PartialEq<Filter<'c2>> for Filter<'c1> {
    fn eq(&self, other: &Filter<'c2>) -> bool {
        match (self.filter_data(), other.filter_data()) {
            (Ok(mine), Ok(theirs)) => mine == theirs,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Ensure that we can construct a filter from all options using default settings
    #[test]
    fn filter_default_construct() {
        let ctx = Context::new().expect("Error creating context");

        // bit width reduction
        {
            let f = Filter::create(
                &ctx,
                FilterData::BitWidthReduction { max_window: None },
            )
            .expect("Error creating bit width filter");
            assert!(matches!(
                f.filter_data(),
                Ok(FilterData::BitWidthReduction { .. })
            ));
        }

        // compression
        {
            let f = Filter::create(
                &ctx,
                FilterData::Compression(CompressionData::new(
                    CompressionType::Lz4,
                )),
            )
            .expect("Error creating compression filter");

            assert!(matches!(
                f.filter_data(),
                Ok(FilterData::Compression(CompressionData {
                    kind: CompressionType::Lz4,
                    ..
                }))
            ));
        }

        // positive delta
        {
            let f = Filter::create(
                &ctx,
                FilterData::PositiveDelta { max_window: None },
            )
            .expect("Error creating positive delta filter");

            assert!(matches!(
                f.filter_data(),
                Ok(FilterData::PositiveDelta { .. })
            ));
        }

        // scale float
        {
            let f = Filter::create(
                &ctx,
                FilterData::ScaleFloat {
                    byte_width: None,
                    factor: None,
                    offset: None,
                },
            )
            .expect("Error creating scale float filter");

            assert!(matches!(
                f.filter_data(),
                Ok(FilterData::ScaleFloat { .. })
            ));
        }

        // webp
        {
            let f = Filter::create(
                &ctx,
                FilterData::WebP {
                    input_format: None,
                    lossless: None,
                    quality: None,
                },
            )
            .expect("Error creating webp filter");

            assert!(matches!(f.filter_data(), Ok(FilterData::WebP { .. })));
        }
    }

    #[test]
    fn filter_get_set_compression_options() {
        let ctx = Context::new().expect("Error creating context instance.");
        let f = Filter::create(
            &ctx,
            FilterData::Compression(CompressionData {
                kind: CompressionType::Lz4,
                level: Some(23),
                reinterpret_datatype: Some(Datatype::UInt16),
            }),
        )
        .expect("Error creating compression filter");

        match f.filter_data().expect("Error reading filter data") {
            FilterData::Compression(CompressionData {
                kind,
                level,
                reinterpret_datatype,
            }) => {
                assert_eq!(CompressionType::Lz4, kind);
                assert_eq!(Some(23), level);
                assert_eq!(Some(Datatype::UInt16), reinterpret_datatype);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn filter_get_set_bit_width_reduction_options() {
        let ctx = Context::new().expect("Error creating context instance.");
        let f = Filter::create(
            &ctx,
            FilterData::BitWidthReduction {
                max_window: Some(75),
            },
        )
        .expect("Error creating bit width reduction filter.");

        match f.filter_data().expect("Error reading filter data") {
            FilterData::BitWidthReduction { max_window } => {
                assert_eq!(Some(75), max_window);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn filter_get_set_positive_delta_options() {
        let ctx = Context::new().expect("Error creating context instance.");
        let f = Filter::create(
            &ctx,
            FilterData::PositiveDelta {
                max_window: Some(75),
            },
        )
        .expect("Error creating positive delta filter.");

        match f.filter_data().expect("Error reading filter data") {
            FilterData::PositiveDelta { max_window } => {
                assert_eq!(Some(75), max_window)
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn filter_get_set_scale_float_options() {
        let ctx = Context::new().expect("Error creating context instance.");
        let f = Filter::create(
            &ctx,
            FilterData::ScaleFloat {
                byte_width: Some(ScaleFloatByteWidth::I16),
                factor: Some(0.643),
                offset: Some(0.24),
            },
        )
        .expect("Error creating scale float filter");

        match f.filter_data().expect("Error reading filter data") {
            FilterData::ScaleFloat {
                byte_width,
                factor,
                offset,
            } => {
                assert_eq!(Some(ScaleFloatByteWidth::I16), byte_width);
                assert_eq!(Some(0.643), factor);
                assert_eq!(Some(0.24), offset);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn filter_get_set_wep_options() {
        let ctx = Context::new().expect("Error creating context instance.");
        let f = Filter::create(
            &ctx,
            FilterData::WebP {
                input_format: Some(WebPFilterInputFormat::Bgra),
                lossless: Some(true),
                quality: Some(0.712),
            },
        )
        .expect("Error creating webp filter");

        match f.filter_data().expect("Error reading filter data") {
            FilterData::WebP {
                input_format,
                lossless,
                quality,
            } => {
                assert_eq!(Some(0.712), quality);
                assert_eq!(Some(WebPFilterInputFormat::Bgra), input_format);
                assert_eq!(Some(true), lossless);
            }
            _ => unreachable!(),
        }
    }
}
