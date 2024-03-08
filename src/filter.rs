extern crate tiledb_sys as ffi;

pub use tiledb_sys::Datatype;
pub use tiledb_sys::FilterOption;
pub use tiledb_sys::FilterType;
pub use tiledb_sys::WebPFilterInputFormat;

use crate::context::Context;

pub struct Filter {
    _wrapped: *mut ffi::tiledb_filter_t,
}

impl Filter {
    pub fn new(
        ctx: &Context,
        filter_type: FilterType,
    ) -> Result<Filter, String> {
        let mut filter = Filter {
            _wrapped: std::ptr::null_mut::<ffi::tiledb_filter_t>(),
        };
        let ftype = filter_type as u32;
        let res = unsafe {
            ffi::tiledb_filter_alloc(
                ctx.as_mut_ptr(),
                ftype,
                &mut filter._wrapped,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(filter)
        } else {
            Err(String::from("Error creating filter."))
        }
    }

    pub fn get_type(&self, ctx: &Context) -> Result<FilterType, String> {
        let mut c_ftype: u32 = 0;
        let res = unsafe {
            ffi::tiledb_filter_get_type(
                ctx.as_mut_ptr(),
                self._wrapped,
                &mut c_ftype,
            )
        };
        if res == ffi::TILEDB_OK {
            let ftype = FilterType::from_u32(c_ftype);
            match ftype {
                Some(ft) => Ok(ft),
                None => Err(String::from("Unknown filter type.")),
            }
        } else {
            Err(ctx
                .get_last_error()
                .unwrap_or_else(|| String::from("Invalid filter type")))
        }
    }

    pub fn set_compression_level(
        &self,
        ctx: &Context,
        level: i32,
    ) -> Result<(), String> {
        let c_level = level as std::ffi::c_int;
        self.set_option(
            ctx,
            FilterOption::COMPRESSION_LEVEL,
            &c_level as *const std::ffi::c_int as *const std::ffi::c_void,
        )
    }

    pub fn get_compression_level(&self, ctx: &Context) -> Result<i32, String> {
        let mut c_level: std::ffi::c_int = 0;
        self.get_option(
            ctx,
            FilterOption::COMPRESSION_LEVEL,
            &mut c_level as *mut std::ffi::c_int as *mut std::ffi::c_void,
        )
        .map(|_| c_level as i32)
    }

    pub fn set_bit_width_max_window(
        &self,
        ctx: &Context,
        size: u32,
    ) -> Result<(), String> {
        let c_size = size as std::ffi::c_uint;
        self.set_option(
            ctx,
            FilterOption::BIT_WIDTH_MAX_WINDOW,
            &c_size as *const std::ffi::c_uint as *const std::ffi::c_void,
        )
    }

    pub fn get_bit_width_max_window(
        &self,
        ctx: &Context,
    ) -> Result<u32, String> {
        let mut c_width: std::ffi::c_uint = 0;
        self.get_option(
            ctx,
            FilterOption::BIT_WIDTH_MAX_WINDOW,
            &mut c_width as *mut std::ffi::c_uint as *mut std::ffi::c_void,
        )
        .map(|_| c_width as u32)
    }

    pub fn set_positive_delta_max_window(
        &self,
        ctx: &Context,
        size: u32,
    ) -> Result<(), String> {
        let c_size = size as std::ffi::c_uint;
        self.set_option(
            ctx,
            FilterOption::POSITIVE_DELTA_MAX_WINDOW,
            &c_size as *const std::ffi::c_uint as *const std::ffi::c_void,
        )
    }

    pub fn get_positive_delta_max_window(
        &self,
        ctx: &Context,
    ) -> Result<u32, String> {
        let mut c_width: std::ffi::c_uint = 0;
        self.get_option(
            ctx,
            FilterOption::POSITIVE_DELTA_MAX_WINDOW,
            &mut c_width as *mut std::ffi::c_uint as *mut std::ffi::c_void,
        )
        .map(|_| c_width as u32)
    }

    pub fn set_float_bytewidth(
        &self,
        ctx: &Context,
        width: u64,
    ) -> Result<(), String> {
        let c_width = width as std::ffi::c_ulonglong;
        self.set_option(
            ctx,
            FilterOption::SCALE_FLOAT_BYTEWIDTH,
            &c_width as *const std::ffi::c_ulonglong as *const std::ffi::c_void,
        )
    }

    pub fn get_float_bytewidth(&self, ctx: &Context) -> Result<u64, String> {
        let mut c_width: std::ffi::c_ulonglong = 0;
        self.get_option(
            ctx,
            FilterOption::SCALE_FLOAT_BYTEWIDTH,
            &mut c_width as *mut std::ffi::c_ulonglong as *mut std::ffi::c_void,
        )
        .map(|_| c_width as u64)
    }

    pub fn set_float_factor(
        &self,
        ctx: &Context,
        factor: f64,
    ) -> Result<(), String> {
        let c_factor = factor as std::ffi::c_double;
        self.set_option(
            ctx,
            FilterOption::SCALE_FLOAT_FACTOR,
            &c_factor as *const std::ffi::c_double as *const std::ffi::c_void,
        )
    }

    pub fn get_float_factor(&self, ctx: &Context) -> Result<f64, String> {
        let mut c_factor: std::ffi::c_double = 0.0;
        self.get_option(
            ctx,
            FilterOption::SCALE_FLOAT_FACTOR,
            &mut c_factor as *mut std::ffi::c_double as *mut std::ffi::c_void,
        )
        .map(|_| c_factor as f64)
    }

    pub fn set_float_offset(
        &self,
        ctx: &Context,
        offset: f64,
    ) -> Result<(), String> {
        let c_offset = offset as std::ffi::c_double;
        self.set_option(
            ctx,
            FilterOption::SCALE_FLOAT_OFFSET,
            &c_offset as *const std::ffi::c_double as *const std::ffi::c_void,
        )
    }

    pub fn get_float_offset(&self, ctx: &Context) -> Result<f64, String> {
        let mut c_factor: std::ffi::c_double = 0.0;
        self.get_option(
            ctx,
            FilterOption::SCALE_FLOAT_OFFSET,
            &mut c_factor as *mut std::ffi::c_double as *mut std::ffi::c_void,
        )
        .map(|_| c_factor as f64)
    }

    pub fn set_webp_quality(
        &self,
        ctx: &Context,
        quality: f32,
    ) -> Result<(), String> {
        let c_quality = quality as std::ffi::c_float;
        self.set_option(
            ctx,
            FilterOption::WEBP_QUALITY,
            &c_quality as *const std::ffi::c_float as *const std::ffi::c_void,
        )
    }

    pub fn get_webp_quality(&self, ctx: &Context) -> Result<f32, String> {
        let mut c_factor: std::ffi::c_float = 0.0;
        self.get_option(
            ctx,
            FilterOption::WEBP_QUALITY,
            &mut c_factor as *mut std::ffi::c_float as *mut std::ffi::c_void,
        )
        .map(|_| c_factor as f32)
    }

    pub fn set_webp_input_format(
        &self,
        ctx: &Context,
        format: WebPFilterInputFormat,
    ) -> Result<(), String> {
        let c_format = format as std::ffi::c_uchar;
        self.set_option(
            ctx,
            FilterOption::WEBP_INPUT_FORMAT,
            &c_format as *const std::ffi::c_uchar as *const std::ffi::c_void,
        )
    }

    pub fn get_webp_input_format(
        &self,
        ctx: &Context,
    ) -> Result<WebPFilterInputFormat, String> {
        let mut c_fmt: std::ffi::c_uchar = 0;
        let res = self.get_option(
            ctx,
            FilterOption::WEBP_INPUT_FORMAT,
            &mut c_fmt as *mut std::ffi::c_uchar as *mut std::ffi::c_void,
        );
        match res {
            Ok(()) => match WebPFilterInputFormat::from_u32(c_fmt as u32) {
                Some(fmt) => Ok(fmt),
                None => Err(String::from(
                    "Invalid WebP input filter format returned from core.",
                )),
            },
            Err(msg) => Err(msg),
        }
    }

    pub fn set_webp_lossless(
        &self,
        ctx: &Context,
        lossless: bool,
    ) -> Result<(), String> {
        let c_lossless: std::ffi::c_uchar = if lossless { 1 } else { 0 };
        self.set_option(
            ctx,
            FilterOption::WEBP_LOSSLESS,
            &c_lossless as *const std::ffi::c_uchar as *const std::ffi::c_void,
        )
    }

    pub fn get_webp_lossless(&self, ctx: &Context) -> Result<bool, String> {
        let mut c_lossless: std::ffi::c_uchar = 0;
        self.get_option(
            ctx,
            FilterOption::WEBP_LOSSLESS,
            &mut c_lossless as *mut std::ffi::c_uchar as *mut std::ffi::c_void,
        )
        .map(|_| c_lossless != 0)
    }

    pub fn set_compression_reinterpret_datatype(
        &self,
        ctx: &Context,
        dtype: Datatype,
    ) -> Result<(), String> {
        let c_dtype = dtype as std::ffi::c_uchar;
        self.set_option(
            ctx,
            FilterOption::COMPRESSION_REINTERPRET_DATATYPE,
            &c_dtype as *const std::ffi::c_uchar as *const std::ffi::c_void,
        )
    }

    pub fn get_compression_reinterpret_datatype(
        &self,
        ctx: &Context,
    ) -> Result<Datatype, String> {
        let mut c_fmt: std::ffi::c_uchar = 0;
        let res = self.get_option(
            ctx,
            FilterOption::COMPRESSION_REINTERPRET_DATATYPE,
            &mut c_fmt as *mut std::ffi::c_uchar as *mut std::ffi::c_void,
        );
        match res {
            Ok(()) => match Datatype::from_u32(c_fmt as u32) {
                Some(dtype) => Ok(dtype),
                None => Err(String::from(
                    "Invalid compression reinterpret datatype returned from core.",
                )),
            },
            Err(msg) => Err(msg),
        }
    }

    fn set_option(
        &self,
        ctx: &Context,
        fopt: FilterOption,
        val: *const std::ffi::c_void,
    ) -> Result<(), String> {
        let res = unsafe {
            ffi::tiledb_filter_set_option(
                ctx.as_mut_ptr(),
                self._wrapped,
                fopt as u32,
                val,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(ctx.get_last_error().unwrap_or_else(|| {
                String::from("Error getting last context error.")
            }))
        }
    }

    fn get_option(
        &self,
        ctx: &Context,
        fopt: FilterOption,
        val: *mut std::ffi::c_void,
    ) -> Result<(), String> {
        let res = unsafe {
            ffi::tiledb_filter_get_option(
                ctx.as_mut_ptr(),
                self._wrapped,
                fopt as u32,
                val,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(ctx.get_last_error().unwrap_or_else(|| {
                String::from("Error getting last context error.")
            }))
        }
    }
}

impl Drop for Filter {
    fn drop(&mut self) {
        if self._wrapped.is_null() {
            return;
        }
        unsafe { ffi::tiledb_filter_free(&mut self._wrapped) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_alloc() {
        let ctx = Context::new().expect("Error creating context instance.");
        for i in 0..255 {
            if let Some(ft) = FilterType::from_u32(i) {
                let f = Filter::new(&ctx, ft.clone())
                    .expect("Error creating filter.");
                let ftype =
                    f.get_type(&ctx).expect("Error getting filter type.");
                assert_eq!(ft, ftype);
            }
        }
    }

    #[test]
    fn filter_get_set_compression_options() {
        let ctx = Context::new().expect("Error creating context instance.");
        let f = Filter::new(&ctx, FilterType::ZSTD)
            .expect("Error creating zstd filter.");

        f.set_compression_level(&ctx, 23)
            .expect("Error setting compression level");
        let level = f
            .get_compression_level(&ctx)
            .expect("Error getting compression level.");
        assert_eq!(level, 23);

        f.set_compression_reinterpret_datatype(&ctx, Datatype::UINT16)
            .expect("Error setting compression reinterpret datatype.");
        let dt = f
            .get_compression_reinterpret_datatype(&ctx)
            .expect("Error getting compression reinterpret datatype");
        assert_eq!(dt, Datatype::UINT16);
    }

    #[test]
    fn filter_get_set_bit_width_reduction_options() {
        let ctx = Context::new().expect("Error creating context instance.");
        let f = Filter::new(&ctx, FilterType::BIT_WIDTH_REDUCTION)
            .expect("Error creating bit width reduction filter.");

        f.set_bit_width_max_window(&ctx, 75)
            .expect("Error setting bit width max window size.");
        let size = f
            .get_bit_width_max_window(&ctx)
            .expect("Error getting bit width max window size.");
        assert_eq!(size, 75);
    }

    #[test]
    fn filter_get_set_positive_delta_options() {
        let ctx = Context::new().expect("Error creating context instance.");
        let f = Filter::new(&ctx, FilterType::POSITIVE_DELTA)
            .expect("Error creating positive delta filter.");

        f.set_positive_delta_max_window(&ctx, 75)
            .expect("Error setting positive delta max window size.");
        let size = f
            .get_positive_delta_max_window(&ctx)
            .expect("Error getting positive delta max window size.");
        assert_eq!(size, 75);
    }

    #[test]
    fn filter_get_set_scale_float_options() {
        let ctx = Context::new().expect("Error creating context instance.");
        let f = Filter::new(&ctx, FilterType::SCALE_FLOAT)
            .expect("Error creating scale float filter.");

        f.set_float_bytewidth(&ctx, 2)
            .expect("Error setting float bytewidth.");
        let width = f
            .get_float_bytewidth(&ctx)
            .expect("Error getting float bytewidth.");
        assert_eq!(width, 2);

        f.set_float_factor(&ctx, 0.643)
            .expect("Error setting float factor.");
        let factor = f
            .get_float_factor(&ctx)
            .expect("Error getting float factor.");
        assert_eq!(factor, 0.643);

        f.set_float_offset(&ctx, 0.24)
            .expect("Error setting float offset.");
        let offset = f
            .get_float_offset(&ctx)
            .expect("Error getting float offset.");
        assert_eq!(offset, 0.24);
    }

    #[test]
    fn filter_get_set_wep_options() {
        let ctx = Context::new().expect("Error creating context instance.");
        let f = Filter::new(&ctx, FilterType::WEBP)
            .expect("Error creating webp filter.");

        f.set_webp_quality(&ctx, 0.712)
            .expect("Error setting webp quality.");
        let quality = f
            .get_webp_quality(&ctx)
            .expect("Error getting webp quality.");
        assert_eq!(quality, 0.712);

        f.set_webp_input_format(&ctx, WebPFilterInputFormat::BGRA)
            .expect("Error setting webp input format.");
        let fmt = f
            .get_webp_input_format(&ctx)
            .expect("Error getting webp input format.");
        assert_eq!(fmt, WebPFilterInputFormat::BGRA);

        f.set_webp_lossless(&ctx, true)
            .expect("Error setting webp lossless.");
        let lossless = f
            .get_webp_lossless(&ctx)
            .expect("Error getting webp lossless.");
        assert!(lossless);
    }
}
