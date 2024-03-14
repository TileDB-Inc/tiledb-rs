use crate::context::Context;
use crate::datatype::DomainType;
use crate::filter_list::FilterList;
use crate::Result as TileDBResult;

pub struct Dimension<'ctx> {
    context: &'ctx Context,
    wrapped: *mut ffi::tiledb_dimension_t,
}

impl<'ctx> Dimension<'ctx> {
    pub fn filters(&self) -> FilterList {
        let mut c_fl: *mut ffi::tiledb_filter_list_t = out_ptr!();

        let c_context = self.context.as_mut_ptr();
        let c_dimension = self.wrapped;
        let c_ret = unsafe {
            ffi::tiledb_dimension_get_filter_list(
                c_context,
                c_dimension,
                &mut c_fl,
            )
        };

        // only fails if dimension is invalid, which Rust API will prevent
        assert_eq!(ffi::TILEDB_OK, c_ret);

        FilterList { _wrapped: c_fl }
    }
}

impl Drop for Dimension<'_> {
    fn drop(&mut self) {
        unsafe { ffi::tiledb_dimension_free(&mut self.wrapped) }
    }
}

pub struct Builder<'ctx> {
    dim: Dimension<'ctx>,
}

impl<'ctx> Builder<'ctx> {
    // TODO: extent might be optional?
    // and it
    pub fn new<DT: DomainType>(
        context: &'ctx Context,
        name: &str,
        domain: &[DT; 2],
        extent: &DT,
    ) -> TileDBResult<Self> {
        let c_context = context.as_mut_ptr();
        let c_datatype = DT::DATATYPE.capi_enum();

        let c_name = cstring!(name);

        let c_domain: [DT::CApiType; 2] =
            [domain[0].as_capi(), domain[1].as_capi()];
        let c_extent: DT::CApiType = extent.as_capi();

        let mut c_dimension: *mut ffi::tiledb_dimension_t =
            std::ptr::null_mut();

        if unsafe {
            ffi::tiledb_dimension_alloc(
                c_context,
                c_name.as_ptr(),
                c_datatype,
                std::mem::transmute::<&DT::CApiType, *const std::ffi::c_void>(
                    &c_domain[0],
                ),
                std::mem::transmute::<&DT::CApiType, *const std::ffi::c_void>(
                    &c_extent,
                ),
                &mut c_dimension,
            )
        } == ffi::TILEDB_OK
        {
            Ok(Builder {
                dim: Dimension {
                    context,
                    wrapped: c_dimension,
                },
            })
        } else {
            Err(context.expect_last_error())
        }
    }

    pub fn filters(self, filters: FilterList) -> TileDBResult<Self> {
        let c_context = self.dim.context.as_mut_ptr();
        let c_dimension = self.dim.wrapped;
        let c_fl = filters.as_mut_ptr();

        if unsafe {
            ffi::tiledb_dimension_set_filter_list(c_context, c_dimension, c_fl)
        } == ffi::TILEDB_OK
        {
            Ok(self)
        } else {
            Err(self.dim.context.expect_last_error())
        }
    }
}

impl<'ctx> Into<Dimension<'ctx>> for Builder<'ctx> {
    fn into(self) -> Dimension<'ctx> {
        self.dim
    }
}

#[cfg(test)]
mod tests {
    use crate::array::dimension::*;
    use crate::filter::Filter;

    #[test]
    fn test_dimension_alloc() {
        let context = Context::new().unwrap();

        // normal use case, should succeed, no memory issues
        {
            let domain: [i32; 2] = [1, 4];
            let extent: i32 = 4;
            Builder::new::<i32>(
                &context,
                "test_dimension_alloc",
                &domain,
                &extent,
            )
            .unwrap();
        }

        // bad domain, should error
        {
            let domain: [i32; 2] = [4, 1];
            let extent: i32 = 4;
            let b = Builder::new::<i32>(
                &context,
                "test_dimension_alloc",
                &domain,
                &extent,
            );
            assert!(b.is_err());
        }

        // bad extent, should error
        {
            let domain: [i32; 2] = [1, 4];
            let extent: i32 = 0;
            let b = Builder::new::<i32>(
                &context,
                "test_dimension_alloc",
                &domain,
                &extent,
            );
            assert!(b.is_err());
        }
    }

    #[test]
    fn test_dimension_filter_list() {
        let context = Context::new().unwrap();

        // none set
        {
            let domain: [i32; 2] = [1, 4];
            let extent: i32 = 4;
            let dimension: Dimension = Builder::new::<i32>(
                &context,
                "test_dimension_alloc",
                &domain,
                &extent,
            )
            .unwrap()
            .into();

            let fl = dimension.filters();
            assert_eq!(0, fl.get_num_filters(&context).unwrap());
        }

        // with some
        {
            let domain: [i32; 2] = [1, 4];
            let extent: i32 = 4;
            let lz4 = Filter::new(&context, ffi::FilterType::LZ4).unwrap();
            let mut fl = FilterList::new(&context).unwrap();
            fl.add_filter(&context, &lz4).unwrap();
            let dimension: Dimension = Builder::new::<i32>(
                &context,
                "test_dimension_alloc",
                &domain,
                &extent,
            )
            .unwrap()
            .filters(fl)
            .unwrap()
            .into();

            let fl = dimension.filters();
            assert_eq!(1, fl.get_num_filters(&context).unwrap());

            let outlz4 = fl.get_filter(&context, 0).unwrap();
            assert_eq!(
                ffi::FilterType::LZ4,
                outlz4.get_type(&context).unwrap()
            );
        }
    }
}
