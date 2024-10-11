mod ftype;
pub mod list;
mod option;

use std::borrow::Borrow;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::ops::Deref;

use self::ftype::FilterType;
use self::option::FilterOption;
use crate::context::{CApiInterface, Context, ContextBound};
use crate::{Datatype, Result as TileDBResult};

pub use self::ftype::Error as FilterTypeError;
pub use self::list::{Builder as FilterListBuilder, FilterList};
pub use self::option::Error as FilterOptionError;

pub use tiledb_common::filter::{
    ChecksumType, CompressionData, CompressionType, FilterData,
    ScaleFloatByteWidth, ScaleFloatByteWidthError, WebPFilterError,
    WebPFilterInputFormat,
};

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

pub struct Filter {
    context: Context,
    pub(crate) raw: RawFilter,
}

impl ContextBound for Filter {
    fn context(&self) -> Context {
        self.context.clone()
    }
}

impl Filter {
    pub fn capi(&self) -> *mut ffi::tiledb_filter_t {
        *self.raw
    }

    pub(crate) fn new(context: &Context, raw: RawFilter) -> Self {
        Filter {
            context: context.clone(),
            raw,
        }
    }

    pub fn create<F>(context: &Context, filter_data: F) -> TileDBResult<Self>
    where
        F: Borrow<FilterData>,
    {
        let filter_data = filter_data.borrow();
        let mut c_filter: *mut ffi::tiledb_filter_t = out_ptr!();
        let ftype =
            ffi::tiledb_filter_type_t::from(FilterType::from(filter_data));
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
                        let c_datatype =
                            ffi::tiledb_datatype_t::from(reinterpret_datatype)
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
                    let c_width = std::ffi::c_ulonglong::from(byte_width);
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
                        ffi::tiledb_filter_webp_format_t::from(input_format)
                            as std::ffi::c_uchar;
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

        Ok(Filter {
            context: context.clone(),
            raw,
        })
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
                    Datatype::try_from(dtype as ffi::tiledb_datatype_t)?
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
        let c_opt = ffi::tiledb_filter_option_t::from(fopt);
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
        let c_opt = ffi::tiledb_filter_option_t::from(fopt);
        let c_val = &val as *const T as *const std::ffi::c_void;
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_filter_set_option(ctx, raw, c_opt, c_val)
        })?;
        Ok(())
    }
}

impl Debug for Filter {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self.filter_data() {
            Ok(data) => write!(f, "{:?}", data),
            Err(e) => write!(f, "<error reading filter data: {}", e),
        }
    }
}

impl PartialEq<Filter> for Filter {
    fn eq(&self, other: &Filter) -> bool {
        match (self.filter_data(), other.filter_data()) {
            (Ok(mine), Ok(theirs)) => mine == theirs,
            _ => false,
        }
    }
}

#[cfg(feature = "arrow")]
pub mod arrow;

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
