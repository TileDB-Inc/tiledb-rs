use std::ops::Deref;

use crate::context::Context;
use crate::datatype::{Datatype, DomainType};
use crate::error::Error;
use crate::filter_list::FilterList;
use crate::Result as TileDBResult;

pub(crate) enum RawDimension {
    Owned(*mut ffi::tiledb_dimension_t),
    Borrowed(*mut ffi::tiledb_dimension_t),
}

impl Deref for RawDimension {
    type Target = *mut ffi::tiledb_dimension_t;
    fn deref(&self) -> &Self::Target {
        match *self {
            RawDimension::Owned(ref ffi) => ffi,
            RawDimension::Borrowed(ref ffi) => ffi,
        }
    }
}

impl Drop for RawDimension {
    fn drop(&mut self) {
        if let RawDimension::Owned(ref mut ffi) = *self {
            unsafe { ffi::tiledb_dimension_free(ffi) }
        }
    }
}

pub struct Dimension<'ctx> {
    pub(crate) context: &'ctx Context,
    pub(crate) raw: RawDimension,
}

impl<'ctx> Dimension<'ctx> {
    pub(crate) fn capi(&self) -> *mut ffi::tiledb_dimension_t {
        *self.raw
    }

    pub fn datatype(&self) -> Datatype {
        let c_context = self.context.as_mut_ptr();
        let c_dimension = self.capi();
        let mut c_datatype: ffi::tiledb_datatype_t = out_ptr!();
        let c_ret = unsafe {
            ffi::tiledb_dimension_get_type(
                c_context,
                c_dimension,
                &mut c_datatype,
            )
        };

        assert_eq!(ffi::TILEDB_OK, c_ret);

        Datatype::from_capi_enum(c_datatype)
    }

    pub fn domain<DT: DomainType>(&self) -> TileDBResult<[DT; 2]> {
        let c_context = self.context.as_mut_ptr();
        let c_dimension = self.capi();
        let mut c_domain_ptr: *const std::ffi::c_void = out_ptr!();

        if DT::DATATYPE != self.datatype() {
            return Err(Error::from(format!(
                "Dimension type mismatch: expected {}, found {}",
                self.datatype(),
                DT::DATATYPE
            )));
        }

        let c_ret = unsafe {
            ffi::tiledb_dimension_get_domain(
                c_context,
                c_dimension,
                &mut c_domain_ptr,
            )
        };

        // the only errors are possible via mis-use of the C API, which Rust prevents
        assert_eq!(ffi::TILEDB_OK, c_ret);

        let c_domain: &[DT::CApiType; 2] =
            unsafe { &*c_domain_ptr.cast::<[DT::CApiType; 2]>() };

        Ok([DT::from_capi(&c_domain[0]), DT::from_capi(&c_domain[1])])
    }

    pub fn filters(&self) -> FilterList {
        let mut c_fl: *mut ffi::tiledb_filter_list_t = out_ptr!();

        let c_context = self.context.as_mut_ptr();
        let c_dimension = self.capi();
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
                &c_domain[0] as *const <DT as DomainType>::CApiType
                    as *const std::ffi::c_void,
                &c_extent as *const <DT as DomainType>::CApiType
                    as *const std::ffi::c_void,
                &mut c_dimension,
            )
        } == ffi::TILEDB_OK
        {
            Ok(Builder {
                dim: Dimension {
                    context,
                    raw: RawDimension::Owned(c_dimension),
                },
            })
        } else {
            Err(context.expect_last_error())
        }
    }

    pub fn filters(self, filters: FilterList) -> TileDBResult<Self> {
        let c_context = self.dim.context.as_mut_ptr();
        let c_dimension = self.dim.capi();
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

    pub fn build(self) -> Dimension<'ctx> {
        self.dim
    }
}

impl<'ctx> From<Builder<'ctx>> for Dimension<'ctx> {
    fn from(builder: Builder<'ctx>) -> Dimension<'ctx> {
        builder.build()
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
    fn test_dimension_domain() {
        let context = Context::new().unwrap();

        // normal use case, should succeed, no memory issues
        {
            let domain_in: [i32; 2] = [1, 4];
            let extent: i32 = 4;
            let dim = Builder::new::<i32>(
                &context,
                "test_dimension_domain",
                &domain_in,
                &extent,
            )
            .unwrap()
            .build();

            assert_eq!(Datatype::Int32, dim.datatype());

            let domain_out = dim.domain::<i32>().unwrap();
            assert_eq!(domain_in[0], domain_out[0]);
            assert_eq!(domain_in[1], domain_out[1]);
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
