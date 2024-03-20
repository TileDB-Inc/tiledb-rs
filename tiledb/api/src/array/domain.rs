use serde_json::json;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::ops::Deref;

use crate::array::{dimension::RawDimension, Dimension};
use crate::context::Context;
use crate::Result as TileDBResult;

pub(crate) enum RawDomain {
    Owned(*mut ffi::tiledb_domain_t),
}

impl Deref for RawDomain {
    type Target = *mut ffi::tiledb_domain_t;
    fn deref(&self) -> &Self::Target {
        match *self {
            RawDomain::Owned(ref ffi) => ffi,
        }
    }
}

impl Drop for RawDomain {
    fn drop(&mut self) {
        let RawDomain::Owned(ref mut ffi) = *self;
        unsafe { ffi::tiledb_domain_free(ffi) };
    }
}

pub struct Domain<'ctx> {
    context: &'ctx Context,
    raw: RawDomain,
}

impl<'ctx> Domain<'ctx> {
    pub(crate) fn capi(&self) -> *mut ffi::tiledb_domain_t {
        *self.raw
    }

    /// Read from the C API whatever we need to use this domain from Rust
    pub(crate) fn new(context: &'ctx Context, raw: RawDomain) -> Self {
        Domain { context, raw }
    }

    pub fn ndim(&self) -> usize {
        let mut ndim: u32 = out_ptr!();
        let c_ret = unsafe {
            ffi::tiledb_domain_get_ndim(
                self.context.as_mut_ptr(),
                *self.raw,
                &mut ndim,
            )
        };
        // the only errors are possible via mis-use of the C API, which Rust prevents
        assert_eq!(ffi::TILEDB_OK, c_ret);
        ndim as usize
    }

    pub fn dimension(&self, idx: usize) -> TileDBResult<Dimension<'ctx>> {
        let c_context = self.context.as_mut_ptr();
        let c_domain = *self.raw;
        let mut c_dimension: *mut ffi::tiledb_dimension_t = out_ptr!();
        let c_idx = match idx.try_into() {
            Ok(idx) => idx,
            Err(e) => {
                return Err(crate::error::Error::from(format!(
                    "Invalid dimension: {}",
                    e
                )))
            }
        };
        let c_ret = unsafe {
            ffi::tiledb_domain_get_dimension_from_index(
                c_context,
                c_domain,
                c_idx,
                &mut c_dimension,
            )
        };
        if c_ret == ffi::TILEDB_OK {
            Ok(Dimension::new(
                self.context,
                RawDimension::Owned(c_dimension),
            ))
        } else {
            Err(self.context.expect_last_error())
        }
    }
}

impl<'ctx> Debug for Domain<'ctx> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let json = json!({
            "dimensions": (0..self.ndim())
                .map(|d| {
                    serde_json::value::Value::String(match self.dimension(d) {
                        Ok(d) => format!("{:?}", d),
                        Err(e) => format!("<{}>", e),
                    })
                })
                .collect::<Vec<_>>(),
            "raw": format!("{:p}", *self.raw)
                /* TODO: what other fields? */
        });

        write!(f, "{}", json)
    }
}

pub struct Builder<'ctx> {
    domain: Domain<'ctx>,
}

impl<'ctx> Builder<'ctx> {
    pub fn new(context: &'ctx Context) -> TileDBResult<Self> {
        let mut c_domain: *mut ffi::tiledb_domain_t = out_ptr!();
        let c_ret = unsafe {
            ffi::tiledb_domain_alloc(context.as_mut_ptr(), &mut c_domain)
        };
        if c_ret == ffi::TILEDB_OK {
            Ok(Builder {
                domain: Domain {
                    context,
                    raw: RawDomain::Owned(c_domain),
                },
            })
        } else {
            Err(context.expect_last_error())
        }
    }

    pub fn add_dimension(
        self,
        dimension: Dimension<'ctx>,
    ) -> TileDBResult<Self> {
        let c_context = self.domain.context.as_mut_ptr();
        let c_domain = *self.domain.raw;
        let c_dim = dimension.capi();

        let c_ret = unsafe {
            ffi::tiledb_domain_add_dimension(c_context, c_domain, c_dim)
        };
        if c_ret == ffi::TILEDB_OK {
            Ok(self)
        } else {
            Err(self.domain.context.expect_last_error())
        }
    }

    pub fn build(self) -> Domain<'ctx> {
        self.domain
    }
}

impl<'ctx> From<Builder<'ctx>> for Domain<'ctx> {
    fn from(builder: Builder<'ctx>) -> Domain<'ctx> {
        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use crate::array::domain::Builder;
    use crate::array::*;
    use crate::Datatype;

    #[test]
    fn test_add_dimension() {
        let context = Context::new().unwrap();

        // no dimensions
        {
            let domain = Builder::new(&context).unwrap().build();
            assert_eq!(0, domain.ndim());

            // TODO: why does this not pass?
            // assert!(domain.dimension(0).is_err());
        }

        // one dimension
        {
            let dim_domain: [i32; 2] = [1, 4];
            let dim_in: Dimension = {
                let extent: i32 = 4;
                DimensionBuilder::new::<i32>(
                    &context,
                    "d1",
                    Datatype::Int32,
                    &dim_domain,
                    &extent,
                )
                .unwrap()
                .build()
            };

            let domain = Builder::new(&context)
                .unwrap()
                .add_dimension(dim_in)
                .unwrap()
                .build();
            assert_eq!(1, domain.ndim());

            let dim_out = domain.dimension(0).unwrap();
            assert_eq!(Datatype::Int32, dim_out.datatype());
            assert_eq!(dim_domain, dim_out.domain::<i32>().unwrap());

            assert!(domain.dimension(1).is_err());
        }

        // two dimensions
        {
            let dim1_domain: [i32; 2] = [1, 4];
            let dim1_in: Dimension = {
                let extent: i32 = 4;
                DimensionBuilder::new::<i32>(
                    &context,
                    "d1",
                    Datatype::Int32,
                    &dim1_domain,
                    &extent,
                )
                .unwrap()
                .build()
            };
            let dim2_domain: [f64; 2] = [-365f64, 365f64];
            let dim2_in: Dimension = {
                let extent: f64 = 128f64;
                DimensionBuilder::new::<f64>(
                    &context,
                    "d2",
                    Datatype::Float64,
                    &dim2_domain,
                    &extent,
                )
                .unwrap()
                .build()
            };

            let domain = Builder::new(&context)
                .unwrap()
                .add_dimension(dim1_in)
                .unwrap()
                .add_dimension(dim2_in)
                .unwrap()
                .build();
            assert_eq!(2, domain.ndim());

            let dim1_out = domain.dimension(0).unwrap();
            assert_eq!(Datatype::Int32, dim1_out.datatype());
            assert_eq!(dim1_domain, dim1_out.domain::<i32>().unwrap());

            let dim2_out = domain.dimension(1).unwrap();
            assert_eq!(Datatype::Float64, dim2_out.datatype());
            assert_eq!(dim2_domain, dim2_out.domain::<f64>().unwrap());

            assert!(domain.dimension(2).is_err());
        }
    }
}
