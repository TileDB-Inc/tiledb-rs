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

pub enum DimensionKey {
    Index(usize),
    Name(String),
}

macro_rules! impl_dimension_key_for_int {
    ($($t:ty),*) => {
        $(
            impl From<$t> for DimensionKey {
                fn from(value: $t) -> Self {
                    DimensionKey::Index(value as usize)
                }
            }
        )*
    };
}
impl_dimension_key_for_int!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);

macro_rules! impl_dimension_key_for_str {
    ($($t:ty),*) => {
        $(
            impl From<$t> for DimensionKey {
                fn from(value: $t) -> Self {
                    DimensionKey::Name(String::from(value))
                }
            }
        )*
    }
}
impl_dimension_key_for_str!(&str, String);

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
                self.context.capi(),
                *self.raw,
                &mut ndim,
            )
        };
        // the only errors are possible via mis-use of the C API, which Rust prevents
        assert_eq!(ffi::TILEDB_OK, c_ret);
        ndim as usize
    }

    pub fn has_dimension<K: Into<DimensionKey>>(
        &self,
        key: K,
    ) -> TileDBResult<bool> {
        match key.into() {
            DimensionKey::Index(idx) => Ok(idx < self.ndim()),
            DimensionKey::Name(name) => {
                let c_context = self.context.capi();
                let c_domain = *self.raw;
                let c_name = cstring!(name);
                let mut c_has: i32 = out_ptr!();
                let c_ret = unsafe {
                    ffi::tiledb_domain_has_dimension(
                        c_context,
                        c_domain,
                        c_name.as_ptr(),
                        &mut c_has,
                    )
                };
                if c_ret == ffi::TILEDB_OK {
                    Ok(c_has != 0)
                } else {
                    Err(self.context.expect_last_error())
                }
            }
        }
    }

    pub fn dimension<K: Into<DimensionKey>>(
        &self,
        key: K,
    ) -> TileDBResult<Dimension<'ctx>> {
        let c_context = self.context.capi();
        let c_domain = *self.raw;
        let mut c_dimension: *mut ffi::tiledb_dimension_t = out_ptr!();

        let c_ret = match key.into() {
            DimensionKey::Index(idx) => {
                let c_idx: u32 = idx.try_into().map_err(|e| {
                    crate::error::Error::from(format!(
                        "Invalid dimension: {}",
                        e
                    ))
                })?;
                unsafe {
                    ffi::tiledb_domain_get_dimension_from_index(
                        c_context,
                        c_domain,
                        c_idx,
                        &mut c_dimension,
                    )
                }
            }
            DimensionKey::Name(name) => {
                let c_name = cstring!(name);
                unsafe {
                    ffi::tiledb_domain_get_dimension_from_name(
                        c_context,
                        c_domain,
                        c_name.as_ptr(),
                        &mut c_dimension,
                    )
                }
            }
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

impl<'c1, 'c2> PartialEq<Domain<'c2>> for Domain<'c1> {
    fn eq(&self, other: &Domain<'c2>) -> bool {
        let ndim_match = self.ndim() == other.ndim();
        if !ndim_match {
            return false;
        }

        for d in 0..self.ndim() {
            let dim_match = match (self.dimension(d), other.dimension(d)) {
                (Ok(mine), Ok(theirs)) => mine == theirs,
                _ => false,
            };
            if !dim_match {
                return false;
            }
        }

        true
    }
}

pub struct Builder<'ctx> {
    domain: Domain<'ctx>,
}

impl<'ctx> Builder<'ctx> {
    pub fn new(context: &'ctx Context) -> TileDBResult<Self> {
        let mut c_domain: *mut ffi::tiledb_domain_t = out_ptr!();
        let c_ret =
            unsafe { ffi::tiledb_domain_alloc(context.capi(), &mut c_domain) };
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
        let c_context = self.domain.context.capi();
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

            assert!(!domain.has_dimension(0).unwrap());
            assert!(!domain.has_dimension("d1").unwrap());

            // TODO: why does this not pass?
            // assert!(domain.dimension(0).is_err());
        }

        // one dimension
        {
            let dim_buildfn = || {
                let dim_domain: [i32; 2] = [1, 4];
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

            let dim_in = dim_buildfn();
            let dim_cmp = dim_buildfn();

            let domain = Builder::new(&context)
                .unwrap()
                .add_dimension(dim_in)
                .unwrap()
                .build();
            assert_eq!(1, domain.ndim());

            assert!(domain.has_dimension(0).unwrap());
            assert!(domain.has_dimension(dim_cmp.name().unwrap()).unwrap());
            assert!(!domain.has_dimension(1).unwrap());
            assert!(!domain.has_dimension("d2").unwrap());

            // by index
            {
                let dim_out = domain.dimension(0).unwrap();
                assert_eq!(dim_cmp, dim_out);

                assert!(domain.dimension(1).is_err());
            }

            // by name
            {
                let dim_out =
                    domain.dimension(dim_cmp.name().unwrap()).unwrap();
                assert_eq!(dim_cmp, dim_out);

                assert!(domain.dimension("d2").is_err());
            }
        }

        // two dimensions
        {
            let dim1_buildfn = || {
                let dim1_domain: [i32; 2] = [1, 4];
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

            let dim1_in: Dimension = dim1_buildfn();
            let dim1_cmp: Dimension = dim1_buildfn();

            let dim2_buildfn = || {
                let dim2_domain: [f64; 2] = [-365f64, 365f64];
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
            let dim2_in: Dimension = dim2_buildfn();
            let dim2_cmp: Dimension = dim2_buildfn();

            let domain = Builder::new(&context)
                .unwrap()
                .add_dimension(dim1_in)
                .unwrap()
                .add_dimension(dim2_in)
                .unwrap()
                .build();
            assert_eq!(2, domain.ndim());

            assert!(domain.has_dimension(0).unwrap());
            assert!(domain.has_dimension(1).unwrap());
            assert!(domain.has_dimension(dim1_cmp.name().unwrap()).unwrap());
            assert!(domain.has_dimension(dim2_cmp.name().unwrap()).unwrap());
            assert!(!domain.has_dimension(2).unwrap());
            assert!(!domain.has_dimension("d3").unwrap());

            // by index
            {
                let dim1_out = domain.dimension(0).unwrap();
                assert_eq!(dim1_cmp, dim1_out);

                let dim2_out = domain.dimension(1).unwrap();
                assert_eq!(dim2_cmp, dim2_out);

                assert!(domain.dimension(2).is_err());
            }

            // by name
            {
                let dim1_out =
                    domain.dimension(dim1_cmp.name().unwrap()).unwrap();
                assert_eq!(dim1_cmp, dim1_out);

                let dim2_out =
                    domain.dimension(dim2_cmp.name().unwrap()).unwrap();
                assert_eq!(dim2_cmp, dim2_out);

                assert!(domain.dimension("d3").is_err());
            }
        }
    }

    #[test]
    fn test_eq() {
        let context = Context::new().unwrap();

        let domain_d0 = Builder::new(&context).unwrap().build();
        assert_eq!(domain_d0, domain_d0);

        // adding a dimension should no longer be eq
        let domain_d1_int32 = Builder::new(&context)
            .unwrap()
            .add_dimension(
                DimensionBuilder::new::<i32>(
                    &context,
                    "d1",
                    Datatype::Int32,
                    &[0, 1000],
                    &100,
                )
                .unwrap()
                .build(),
            )
            .unwrap()
            .build();
        assert_eq!(domain_d1_int32, domain_d1_int32);
        assert_ne!(domain_d0, domain_d1_int32);

        // a different dimension should no longer be eq
        let domain_d1_float64 = Builder::new(&context)
            .unwrap()
            .add_dimension(
                DimensionBuilder::new::<f64>(
                    &context,
                    "d1",
                    Datatype::Float64,
                    &[0f64, 1000f64],
                    &100f64,
                )
                .unwrap()
                .build(),
            )
            .unwrap()
            .build();
        assert_eq!(domain_d1_float64, domain_d1_float64);
        assert_ne!(domain_d0, domain_d1_float64);
        assert_ne!(domain_d1_int32, domain_d1_float64);
    }
}
