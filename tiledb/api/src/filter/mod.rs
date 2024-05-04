use std::borrow::Borrow;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::ops::Deref;

use serde::{Deserialize, Serialize};
use util::option::OptionSubset;

use crate::context::{CApiInterface, Context, ContextBound};
use crate::error::{DatatypeErrorKind, Error};
use crate::{Datatype, Result as TileDBResult};

pub mod list;
pub mod webp;

pub use crate::filter::list::{Builder as FilterListBuilder, FilterList};
pub use crate::filter::webp::WebPFilterInputFormat;

mod ftype;
mod option;

use crate::filter::ftype::FilterType;
use crate::filter::option::FilterOption;

#[derive(
    Copy, Clone, Debug, Deserialize, Eq, OptionSubset, PartialEq, Serialize,
)]
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

#[derive(
    Copy, Clone, Debug, Deserialize, Eq, OptionSubset, PartialEq, Serialize,
)]
pub enum ChecksumType {
    Md5,
    Sha256,
}

#[derive(Clone, Debug, Deserialize, OptionSubset, PartialEq, Serialize)]
pub struct CompressionData {
    pub kind: CompressionType,
    pub level: Option<i32>,
}

impl CompressionData {
    pub fn new(kind: CompressionType) -> Self {
        CompressionData { kind, level: None }
    }
}

#[derive(
    Clone, Copy, Debug, Default, Deserialize, OptionSubset, PartialEq, Serialize,
)]
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
            v => Err(Self::Error::LibTileDB(format!(
                "Invalid scale float byte width: {}",
                v
            ))),
        }
    }
}

#[derive(Clone, Debug, Deserialize, OptionSubset, PartialEq, Serialize)]
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
    pub fn construct<'ctx>(
        &self,
        context: &'ctx Context,
    ) -> TileDBResult<Filter<'ctx>> {
        Filter::create(context, self)
    }

    pub fn get_type(&self) -> FilterType {
        match *self {
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
                } => {
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

impl<'ctx> TryFrom<Filter<'ctx>> for FilterData {
    type Error = crate::error::Error;

    fn try_from(filter: Filter<'ctx>) -> TileDBResult<Self> {
        Self::try_from(&filter)
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

// impl<'ctx> ContextBoundBase<'ctx> for Filter<'ctx> {}

impl<'ctx> ContextBound<'ctx> for Filter<'ctx> {
    fn context(&self) -> &'ctx Context {
        self.context
    }
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
        let mut c_filter: *mut ffi::tiledb_filter_t = out_ptr!();
        let ftype = filter_data.get_type().capi_enum();
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_filter_alloc(ctx, ftype, &mut c_filter)
        })?;

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
                        FilterOption::BitWidthMaxWindow,
                        c_size,
                    )?;
                }
            }
            FilterData::Checksum(ChecksumType::Md5) => (),
            FilterData::Checksum(ChecksumType::Sha256) => (),
            FilterData::Compression(CompressionData {
                kind, level, ..
            }) => {
                if let Some(level) = level {
                    let c_level = level as std::ffi::c_int;
                    Self::set_option(
                        context,
                        *raw,
                        FilterOption::CompressionLevel,
                        c_level,
                    )?;
                }
                match kind {
                    CompressionType::Delta {
                        reinterpret_datatype: Some(reinterpret_datatype),
                    }
                    | CompressionType::DoubleDelta {
                        reinterpret_datatype: Some(reinterpret_datatype),
                    } => {
                        let c_datatype = reinterpret_datatype.capi_enum()
                            as std::ffi::c_uchar;
                        Self::set_option(
                            context,
                            *raw,
                            FilterOption::CompressionReinterpretDatatype,
                            c_datatype,
                        )?;
                    }
                    _ => (),
                }
            }
            FilterData::PositiveDelta { max_window } => {
                if let Some(max_window) = max_window {
                    let c_size = max_window as std::ffi::c_uint;
                    Self::set_option(
                        context,
                        *raw,
                        FilterOption::PositiveDeltaMaxWindow,
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
                        FilterOption::ScaleFloatByteWidth,
                        c_width,
                    )?;
                }

                if let Some(factor) = factor {
                    let c_factor = factor as std::ffi::c_double;
                    Self::set_option(
                        context,
                        *raw,
                        FilterOption::ScaleFloatFactor,
                        c_factor,
                    )?;
                }

                if let Some(offset) = offset {
                    let c_offset = offset as std::ffi::c_double;
                    Self::set_option(
                        context,
                        c_filter,
                        FilterOption::ScaleFloatOffset,
                        c_offset,
                    )?;
                }
            }
            FilterData::WebP {
                input_format,
                lossless,
                quality,
            } => {
                {
                    let c_format =
                        input_format.capi_enum() as std::ffi::c_uchar;
                    Self::set_option(
                        context,
                        *raw,
                        FilterOption::WebPInputFormat,
                        c_format,
                    )?;
                }

                if let Some(lossless) = lossless {
                    let c_lossless: std::ffi::c_uchar =
                        if lossless { 1 } else { 0 };
                    Self::set_option(
                        context,
                        *raw,
                        FilterOption::WebPLossless,
                        c_lossless,
                    )?;
                }

                if let Some(quality) = quality {
                    let c_quality = quality as std::ffi::c_float;
                    Self::set_option(
                        context,
                        *raw,
                        FilterOption::WebPQuality,
                        c_quality,
                    )?;
                }
            }
            FilterData::Xor => (),
        };

        Ok(Filter { context, raw })
    }

    pub fn filter_data(&self) -> TileDBResult<FilterData> {
        let c_filter = self.capi();
        let mut c_ftype: u32 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_filter_get_type(ctx, c_filter, &mut c_ftype)
        })?;

        let get_compression_data = |kind| -> TileDBResult<FilterData> {
            let level =
                Some(self.get_option::<i32>(FilterOption::CompressionLevel)?);
            Ok(FilterData::Compression(CompressionData { kind, level }))
        };

        match FilterType::try_from(c_ftype)? {
            FilterType::None => Ok(FilterData::None),
            FilterType::Gzip => get_compression_data(CompressionType::Gzip),
            FilterType::Zstd => get_compression_data(CompressionType::Zstd),
            FilterType::Lz4 => get_compression_data(CompressionType::Lz4),
            FilterType::Rle => get_compression_data(CompressionType::Rle),
            FilterType::Bzip2 => get_compression_data(CompressionType::Bzip2),
            FilterType::Dictionary => {
                get_compression_data(CompressionType::Dictionary)
            }
            FilterType::Delta | FilterType::DoubleDelta => {
                let reinterpret_datatype = Some({
                    let dtype = self.get_option::<std::ffi::c_uchar>(
                        FilterOption::CompressionReinterpretDatatype,
                    )?;
                    Datatype::try_from(dtype as ffi::tiledb_datatype_t)
                        .map_err(|_| {
                            Error::Datatype(
                                DatatypeErrorKind::InvalidDiscriminant(
                                    dtype as u64,
                                ),
                            )
                        })?
                });
                if FilterType::try_from(c_ftype).unwrap() == FilterType::Delta {
                    get_compression_data(CompressionType::Delta {
                        reinterpret_datatype,
                    })
                } else {
                    get_compression_data(CompressionType::DoubleDelta {
                        reinterpret_datatype,
                    })
                }
            }
            FilterType::BitShuffle => Ok(FilterData::BitShuffle),
            FilterType::ByteShuffle => Ok(FilterData::ByteShuffle),
            FilterType::Xor => Ok(FilterData::Xor),
            FilterType::BitWidthReduction => {
                Ok(FilterData::BitWidthReduction {
                    max_window: Some(
                        self.get_option::<u32>(
                            FilterOption::BitWidthMaxWindow,
                        )?,
                    ),
                })
            }
            FilterType::PositiveDelta => Ok(FilterData::PositiveDelta {
                max_window: Some(self.get_option::<std::ffi::c_uint>(
                    FilterOption::PositiveDeltaMaxWindow,
                )?),
            }),
            FilterType::ChecksumMD5 => {
                Ok(FilterData::Checksum(ChecksumType::Md5))
            }
            FilterType::ChecksumSHA256 => {
                Ok(FilterData::Checksum(ChecksumType::Sha256))
            }
            FilterType::ScaleFloat => Ok(FilterData::ScaleFloat {
                byte_width: Some(ScaleFloatByteWidth::try_from(
                    self.get_option::<std::ffi::c_ulonglong>(
                        FilterOption::ScaleFloatByteWidth,
                    )?,
                )?),
                factor: Some(self.get_option::<std::ffi::c_double>(
                    FilterOption::ScaleFloatFactor,
                )?),
                offset: Some(self.get_option::<std::ffi::c_double>(
                    FilterOption::ScaleFloatOffset,
                )?),
            }),
            FilterType::WebP => Ok(FilterData::WebP {
                input_format: WebPFilterInputFormat::try_from(
                    self.get_option::<u32>(FilterOption::WebPInputFormat)?,
                )?,
                lossless: Some(
                    self.get_option::<std::ffi::c_uchar>(
                        FilterOption::WebPLossless,
                    )? != 0,
                ),
                quality: Some(self.get_option::<std::ffi::c_float>(
                    FilterOption::WebPQuality,
                )?),
            }),
        }
    }

    fn get_option<T>(&self, fopt: FilterOption) -> TileDBResult<T> {
        let c_filter = self.capi();
        let c_opt = fopt.capi_enum();
        let mut val: T = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_filter_get_option(
                ctx,
                c_filter,
                c_opt,
                &mut val as *mut T as *mut std::ffi::c_void,
            )
        })?;
        Ok(val)
    }

    fn set_option<T>(
        context: &Context,
        raw: *mut ffi::tiledb_filter_t,
        fopt: FilterOption,
        val: T,
    ) -> TileDBResult<()> {
        let c_opt = fopt.capi_enum();
        let c_val = &val as *const T as *const std::ffi::c_void;
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_filter_set_option(ctx, raw, c_opt, c_val)
        })?;
        Ok(())
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

#[cfg(feature = "arrow")]
pub mod arrow;

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;

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
                    input_format: WebPFilterInputFormat::Rgba,
                    lossless: None,
                    quality: None,
                },
            )
            .expect("Error creating webp filter");

            assert!(matches!(
                f.filter_data().expect("Error reading webp filter data"),
                FilterData::WebP { .. }
            ));
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
            }),
        )
        .expect("Error creating compression filter");

        match f.filter_data().expect("Error reading filter data") {
            FilterData::Compression(CompressionData { kind, level }) => {
                assert_eq!(CompressionType::Lz4, kind);
                assert_eq!(Some(23), level);
            }
            _ => unreachable!(),
        };

        let delta_in = FilterData::Compression(CompressionData {
            kind: CompressionType::DoubleDelta {
                reinterpret_datatype: Some(Datatype::UInt16),
            },
            level: Some(5),
        });
        let delta_c = Filter::create(&ctx, &delta_in)
            .expect("Error creating double delta compression filter");
        let delta_out =
            delta_c.filter_data().expect("Error reading filter data");
        assert_eq!(delta_in, delta_out);
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
                input_format: WebPFilterInputFormat::Bgra,
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
                assert_eq!(WebPFilterInputFormat::Bgra, input_format);
                assert_eq!(Some(true), lossless);
            }
            _ => unreachable!(),
        }
    }
}
