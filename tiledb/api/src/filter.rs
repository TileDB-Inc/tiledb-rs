use std::ops::Deref;

use crate::context::Context;
use crate::error::Error;
use crate::Result as TileDBResult;

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

pub enum ChecksumType {
    Md5,
    Sha256,
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

    fn create(
        context: &'ctx Context,
        filter_type: ffi::FilterType,
    ) -> TileDBResult<Self> {
        let mut c_filter: *mut ffi::tiledb_filter_t = out_ptr!();
        let ftype = filter_type as u32;
        let res = unsafe {
            ffi::tiledb_filter_alloc(context.capi(), ftype, &mut c_filter)
        };
        if res == ffi::TILEDB_OK {
            Ok(Filter {
                context,
                raw: RawFilter::Owned(c_filter),
            })
        } else {
            Err(context.expect_last_error())
        }
    }

    pub fn get_type(&self) -> TileDBResult<ffi::FilterType> {
        let mut c_ftype: u32 = 0;
        let res = unsafe {
            ffi::tiledb_filter_get_type(
                self.context.capi(),
                self.capi(),
                &mut c_ftype,
            )
        };
        if res == ffi::TILEDB_OK {
            let ftype = ffi::FilterType::from_u32(c_ftype);
            match ftype {
                Some(ft) => Ok(ft),
                None => Err(Error::from("Unknown filter type.")),
            }
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn get_bit_width_max_window(&self) -> TileDBResult<u32> {
        let mut c_width: std::ffi::c_uint = 0;
        self.get_option(
            ffi::FilterOption::BIT_WIDTH_MAX_WINDOW,
            &mut c_width as *mut std::ffi::c_uint as *mut std::ffi::c_void,
        )
        .map(|_| c_width as u32)
    }

    pub fn get_compression_level(&self) -> TileDBResult<i32> {
        let mut c_level: std::ffi::c_int = 0;
        self.get_option(
            ffi::FilterOption::COMPRESSION_LEVEL,
            &mut c_level as *mut std::ffi::c_int as *mut std::ffi::c_void,
        )
        .map(|_| c_level as i32)
    }

    pub fn get_compression_reinterpret_datatype(
        &self,
    ) -> TileDBResult<ffi::Datatype> {
        let mut c_fmt: std::ffi::c_uchar = 0;
        let res = self.get_option(
            ffi::FilterOption::COMPRESSION_REINTERPRET_DATATYPE,
            &mut c_fmt as *mut std::ffi::c_uchar as *mut std::ffi::c_void,
        );
        match res {
            Ok(()) => match ffi::Datatype::from_u32(c_fmt as u32) {
                Some(dtype) => Ok(dtype),
                None => Err(Error::from("Invalid compression reinterpret datatype returned from core."))
            },
            Err(msg) => Err(msg),
        }
    }

    pub fn get_float_bytewidth(&self) -> TileDBResult<u64> {
        let mut c_width: std::ffi::c_ulonglong = 0;
        self.get_option(
            ffi::FilterOption::SCALE_FLOAT_BYTEWIDTH,
            &mut c_width as *mut std::ffi::c_ulonglong as *mut std::ffi::c_void,
        )
        .map(|_| c_width as u64)
    }

    pub fn get_float_factor(&self) -> TileDBResult<f64> {
        let mut c_factor: std::ffi::c_double = 0.0;
        self.get_option(
            ffi::FilterOption::SCALE_FLOAT_FACTOR,
            &mut c_factor as *mut std::ffi::c_double as *mut std::ffi::c_void,
        )
        .map(|_| c_factor as f64)
    }

    pub fn get_float_offset(&self) -> TileDBResult<f64> {
        let mut c_factor: std::ffi::c_double = 0.0;
        self.get_option(
            ffi::FilterOption::SCALE_FLOAT_OFFSET,
            &mut c_factor as *mut std::ffi::c_double as *mut std::ffi::c_void,
        )
        .map(|_| c_factor as f64)
    }

    pub fn get_positive_delta_max_window(&self) -> TileDBResult<u32> {
        let mut c_width: std::ffi::c_uint = 0;
        self.get_option(
            ffi::FilterOption::POSITIVE_DELTA_MAX_WINDOW,
            &mut c_width as *mut std::ffi::c_uint as *mut std::ffi::c_void,
        )
        .map(|_| c_width as u32)
    }

    pub fn get_webp_input_format(
        &self,
    ) -> TileDBResult<ffi::WebPFilterInputFormat> {
        let mut c_fmt: std::ffi::c_uchar = 0;
        let res = self.get_option(
            ffi::FilterOption::WEBP_INPUT_FORMAT,
            &mut c_fmt as *mut std::ffi::c_uchar as *mut std::ffi::c_void,
        );
        match res {
            Ok(()) => {
                match ffi::WebPFilterInputFormat::from_u32(c_fmt as u32) {
                    Some(fmt) => Ok(fmt),
                    None => Err(Error::from(
                        "Invalid WebP input filter format returned from core.",
                    )),
                }
            }
            Err(msg) => Err(msg),
        }
    }

    pub fn get_webp_lossless(&self) -> TileDBResult<bool> {
        let mut c_lossless: std::ffi::c_uchar = 0;
        self.get_option(
            ffi::FilterOption::WEBP_LOSSLESS,
            &mut c_lossless as *mut std::ffi::c_uchar as *mut std::ffi::c_void,
        )
        .map(|_| c_lossless != 0)
    }

    pub fn get_webp_quality(&self) -> TileDBResult<f32> {
        let mut c_factor: std::ffi::c_float = 0.0;
        self.get_option(
            ffi::FilterOption::WEBP_QUALITY,
            &mut c_factor as *mut std::ffi::c_float as *mut std::ffi::c_void,
        )
        .map(|_| c_factor as f32)
    }

    fn get_option(
        &self,
        fopt: ffi::FilterOption,
        val: *mut std::ffi::c_void,
    ) -> TileDBResult<()> {
        let res = unsafe {
            ffi::tiledb_filter_get_option(
                self.context.capi(),
                self.capi(),
                fopt as u32,
                val,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(self.context.expect_last_error())
        }
    }

    fn set_option(
        &self,
        fopt: ffi::FilterOption,
        val: *const std::ffi::c_void,
    ) -> TileDBResult<()> {
        let res = unsafe {
            ffi::tiledb_filter_set_option(
                self.context.capi(),
                self.capi(),
                fopt as u32,
                val,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(self.context.expect_last_error())
        }
    }
}

pub struct NoopFilterBuilder<'ctx> {
    filter: Filter<'ctx>,
}

impl<'ctx> NoopFilterBuilder<'ctx> {
    pub fn new(context: &'ctx Context) -> TileDBResult<Self> {
        Ok(Self {
            filter: Filter::create(context, ffi::FilterType::None)?,
        })
    }

    pub fn build(self) -> Filter<'ctx> {
        self.filter
    }
}

pub struct CompressionFilterBuilder<'ctx> {
    filter: Filter<'ctx>,
}

impl<'ctx> CompressionFilterBuilder<'ctx> {
    pub fn new(
        context: &'ctx Context,
        comp_type: CompressionType,
    ) -> TileDBResult<Self> {
        let ftype: ffi::FilterType = match comp_type {
            CompressionType::Bzip2 => ffi::FilterType::Bzip2,
            CompressionType::Delta => ffi::FilterType::Delta,
            CompressionType::Dictionary => ffi::FilterType::Dictionary,
            CompressionType::DoubleDelta => ffi::FilterType::DoubleDelta,
            CompressionType::Gzip => ffi::FilterType::Gzip,
            CompressionType::Lz4 => ffi::FilterType::Lz4,
            CompressionType::Rle => ffi::FilterType::Rle,
            CompressionType::Zstd => ffi::FilterType::Zstd,
        };

        Ok(CompressionFilterBuilder {
            filter: Filter::create(context, ftype)?,
        })
    }

    pub fn set_compression_level(self, level: i32) -> TileDBResult<Self> {
        let c_level = level as std::ffi::c_int;
        self.filter.set_option(
            ffi::FilterOption::COMPRESSION_LEVEL,
            &c_level as *const std::ffi::c_int as *const std::ffi::c_void,
        )?;
        Ok(self)
    }

    pub fn set_reinterpret_datatype(
        self,
        dtype: ffi::Datatype,
    ) -> TileDBResult<Self> {
        let c_dtype = dtype as std::ffi::c_uchar;
        self.filter.set_option(
            ffi::FilterOption::COMPRESSION_REINTERPRET_DATATYPE,
            &c_dtype as *const std::ffi::c_uchar as *const std::ffi::c_void,
        )?;
        Ok(self)
    }

    pub fn build(self) -> Filter<'ctx> {
        self.filter
    }
}

pub struct BitWidthReductionFilterBuilder<'ctx> {
    filter: Filter<'ctx>,
}

impl<'ctx> BitWidthReductionFilterBuilder<'ctx> {
    pub fn new(context: &'ctx Context) -> TileDBResult<Self> {
        Ok(BitWidthReductionFilterBuilder {
            filter: Filter::create(
                context,
                ffi::FilterType::BitWidthReduction,
            )?,
        })
    }

    pub fn set_max_window(self, size: u32) -> TileDBResult<Self> {
        let c_size = size as std::ffi::c_uint;
        self.filter.set_option(
            ffi::FilterOption::BIT_WIDTH_MAX_WINDOW,
            &c_size as *const std::ffi::c_uint as *const std::ffi::c_void,
        )?;
        Ok(self)
    }

    pub fn build(self) -> Filter<'ctx> {
        self.filter
    }
}

pub struct BitShuffleFilterBuilder<'ctx> {
    filter: Filter<'ctx>,
}

impl<'ctx> BitShuffleFilterBuilder<'ctx> {
    pub fn new(context: &'ctx Context) -> TileDBResult<Self> {
        Ok(BitShuffleFilterBuilder {
            filter: Filter::create(context, ffi::FilterType::BitShuffle)?,
        })
    }

    pub fn build(self) -> Filter<'ctx> {
        self.filter
    }
}

pub struct ByteShuffleFilterBuilder<'ctx> {
    filter: Filter<'ctx>,
}

impl<'ctx> ByteShuffleFilterBuilder<'ctx> {
    pub fn new(context: &'ctx Context) -> TileDBResult<Self> {
        Ok(ByteShuffleFilterBuilder {
            filter: Filter::create(context, ffi::FilterType::ByteShuffle)?,
        })
    }

    pub fn build(self) -> Filter<'ctx> {
        self.filter
    }
}

pub struct ScaleFloatFilterBuilder<'ctx> {
    filter: Filter<'ctx>,
}

impl<'ctx> ScaleFloatFilterBuilder<'ctx> {
    pub fn new(context: &'ctx Context) -> TileDBResult<Self> {
        Ok(ScaleFloatFilterBuilder {
            filter: Filter::create(context, ffi::FilterType::ScaleFloat)?,
        })
    }

    pub fn set_bytewidth(self, width: u64) -> TileDBResult<Self> {
        let c_width = width as std::ffi::c_ulonglong;
        self.filter.set_option(
            ffi::FilterOption::SCALE_FLOAT_BYTEWIDTH,
            &c_width as *const std::ffi::c_ulonglong as *const std::ffi::c_void,
        )?;
        Ok(self)
    }

    pub fn set_factor(self, factor: f64) -> TileDBResult<Self> {
        let c_factor = factor as std::ffi::c_double;
        self.filter.set_option(
            ffi::FilterOption::SCALE_FLOAT_FACTOR,
            &c_factor as *const std::ffi::c_double as *const std::ffi::c_void,
        )?;
        Ok(self)
    }

    pub fn set_offset(self, offset: f64) -> TileDBResult<Self> {
        let c_offset = offset as std::ffi::c_double;
        self.filter.set_option(
            ffi::FilterOption::SCALE_FLOAT_OFFSET,
            &c_offset as *const std::ffi::c_double as *const std::ffi::c_void,
        )?;
        Ok(self)
    }

    pub fn build(self) -> Filter<'ctx> {
        self.filter
    }
}

pub struct PositiveDeltaFilterBuilder<'ctx> {
    filter: Filter<'ctx>,
}

impl<'ctx> PositiveDeltaFilterBuilder<'ctx> {
    pub fn new(context: &'ctx Context) -> TileDBResult<Self> {
        Ok(PositiveDeltaFilterBuilder {
            filter: Filter::create(context, ffi::FilterType::PositiveDelta)?,
        })
    }

    pub fn set_max_window(self, size: u32) -> TileDBResult<Self> {
        let c_size = size as std::ffi::c_uint;
        self.filter.set_option(
            ffi::FilterOption::POSITIVE_DELTA_MAX_WINDOW,
            &c_size as *const std::ffi::c_uint as *const std::ffi::c_void,
        )?;
        Ok(self)
    }

    pub fn build(self) -> Filter<'ctx> {
        self.filter
    }
}

pub struct ChecksumFilterBuilder<'ctx> {
    filter: Filter<'ctx>,
}

impl<'ctx> ChecksumFilterBuilder<'ctx> {
    pub fn new(
        context: &'ctx Context,
        checksum_type: ChecksumType,
    ) -> TileDBResult<Self> {
        let ftype = match checksum_type {
            ChecksumType::Md5 => ffi::FilterType::ChecksumMD5,
            ChecksumType::Sha256 => ffi::FilterType::ChecksumSHA256,
        };
        Ok(ChecksumFilterBuilder {
            filter: Filter::create(context, ftype)?,
        })
    }

    pub fn build(self) -> Filter<'ctx> {
        self.filter
    }
}

pub struct XorFilterBuilder<'ctx> {
    filter: Filter<'ctx>,
}

impl<'ctx> XorFilterBuilder<'ctx> {
    pub fn new(context: &'ctx Context) -> TileDBResult<Self> {
        Ok(XorFilterBuilder {
            filter: Filter::create(context, ffi::FilterType::Xor)?,
        })
    }

    pub fn build(self) -> Filter<'ctx> {
        self.filter
    }
}

pub struct WebPFilterBuilder<'ctx> {
    filter: Filter<'ctx>,
}

impl<'ctx> WebPFilterBuilder<'ctx> {
    pub fn new(context: &'ctx Context) -> TileDBResult<Self> {
        Ok(WebPFilterBuilder {
            filter: Filter::create(context, ffi::FilterType::WebP)?,
        })
    }

    pub fn set_input_format(
        self,
        format: ffi::WebPFilterInputFormat,
    ) -> TileDBResult<Self> {
        let c_format = format as std::ffi::c_uchar;
        self.filter.set_option(
            ffi::FilterOption::WEBP_INPUT_FORMAT,
            &c_format as *const std::ffi::c_uchar as *const std::ffi::c_void,
        )?;
        Ok(self)
    }

    pub fn set_lossless(self, lossless: bool) -> TileDBResult<Self> {
        let c_lossless: std::ffi::c_uchar = if lossless { 1 } else { 0 };
        self.filter.set_option(
            ffi::FilterOption::WEBP_LOSSLESS,
            &c_lossless as *const std::ffi::c_uchar as *const std::ffi::c_void,
        )?;
        Ok(self)
    }

    pub fn set_quality(self, quality: f32) -> TileDBResult<Self> {
        let c_quality = quality as std::ffi::c_float;
        self.filter.set_option(
            ffi::FilterOption::WEBP_QUALITY,
            &c_quality as *const std::ffi::c_float as *const std::ffi::c_void,
        )?;
        Ok(self)
    }

    pub fn build(self) -> Filter<'ctx> {
        self.filter
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_get_set_compression_options() {
        let ctx = Context::new().expect("Error creating context instance.");
        let f = CompressionFilterBuilder::new(&ctx, CompressionType::Lz4)
            .expect("Error creating builder instance.")
            .set_compression_level(23)
            .expect("Error setting compression level.")
            .set_reinterpret_datatype(ffi::Datatype::UInt16)
            .expect("Error setting compression reinterpret datatype.")
            .build();

        let level = f
            .get_compression_level()
            .expect("Error getting compression level.");
        assert_eq!(level, 23);

        let dt = f
            .get_compression_reinterpret_datatype()
            .expect("Error getting compression reinterpret datatype");
        assert_eq!(dt, ffi::Datatype::UInt16);
    }

    #[test]
    fn filter_get_set_bit_width_reduction_options() {
        let ctx = Context::new().expect("Error creating context instance.");
        let f = BitWidthReductionFilterBuilder::new(&ctx)
            .expect("Error creating bit width reduction filter.")
            .set_max_window(75)
            .expect("Error setting bit width max window.")
            .build();

        let size = f
            .get_bit_width_max_window()
            .expect("Error getting bit width max window size.");
        assert_eq!(size, 75);
    }

    #[test]
    fn filter_get_set_positive_delta_options() {
        let ctx = Context::new().expect("Error creating context instance.");
        let f = PositiveDeltaFilterBuilder::new(&ctx)
            .expect("Error creating positive delta filter.")
            .set_max_window(75)
            .expect("Error setting positive delta max window.")
            .build();

        let size = f
            .get_positive_delta_max_window()
            .expect("Error getting positive delta max window size.");
        assert_eq!(size, 75);
    }

    #[test]
    fn filter_get_set_scale_float_options() {
        let ctx = Context::new().expect("Error creating context instance.");
        let f = ScaleFloatFilterBuilder::new(&ctx)
            .expect("Error creating scale float filter.")
            .set_bytewidth(2)
            .expect("Error setting float byte width.")
            .set_factor(0.643)
            .expect("Error setting float factor.")
            .set_offset(0.24)
            .expect("Error setting float offset.")
            .build();

        let width = f
            .get_float_bytewidth()
            .expect("Error getting float bytewidth.");
        assert_eq!(width, 2);

        let factor = f.get_float_factor().expect("Error getting float factor.");
        assert_eq!(factor, 0.643);

        let offset = f.get_float_offset().expect("Error getting float offset.");
        assert_eq!(offset, 0.24);
    }

    #[test]
    fn filter_get_set_wep_options() {
        let ctx = Context::new().expect("Error creating context instance.");
        let f = WebPFilterBuilder::new(&ctx)
            .expect("Error creating webp filter.")
            .set_input_format(ffi::WebPFilterInputFormat::BGRA)
            .expect("Error setting WebP input format.")
            .set_lossless(true)
            .expect("Error setting WebP lossless.")
            .set_quality(0.712)
            .expect("Error sestting WebP quality.")
            .build();

        let quality =
            f.get_webp_quality().expect("Error getting webp quality.");
        assert_eq!(quality, 0.712);

        let fmt = f
            .get_webp_input_format()
            .expect("Error getting webp input format.");
        assert_eq!(fmt, ffi::WebPFilterInputFormat::BGRA);

        let lossless =
            f.get_webp_lossless().expect("Error getting webp lossless.");
        assert!(lossless);
    }
}
