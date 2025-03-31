use std::ops::Deref;

#[cfg(any(test, feature = "pod"))]
use std::fmt::{Debug, Formatter, Result as FmtResult};

use anyhow::anyhow;

use crate::Result as TileDBResult;
use crate::array::dimension::{Dimension, RawDimension};
use crate::context::{CApiInterface, Context, ContextBound};
use crate::error::Error;
use crate::key::LookupKey;

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

pub struct Domain {
    context: Context,
    raw: RawDomain,
}

impl ContextBound for Domain {
    fn context(&self) -> Context {
        self.context.clone()
    }
}

impl Domain {
    pub(crate) fn capi(&self) -> *mut ffi::tiledb_domain_t {
        *self.raw
    }

    /// Read from the C API whatever we need to use this domain from Rust
    pub(crate) fn new(context: &Context, raw: RawDomain) -> Self {
        Domain {
            context: context.clone(),
            raw,
        }
    }

    /// Returns the number of dimensions.
    pub fn num_dimensions(&self) -> TileDBResult<usize> {
        let mut ndim: u32 = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_domain_get_ndim(ctx, *self.raw, &mut ndim)
        })?;

        Ok(ndim as usize)
    }

    pub fn has_dimension<K: Into<LookupKey>>(
        &self,
        key: K,
    ) -> TileDBResult<bool> {
        match key.into() {
            LookupKey::Index(idx) => Ok(idx < self.num_dimensions()?),
            LookupKey::Name(name) => {
                let c_domain = *self.raw;
                let c_name = cstring!(name);
                let mut c_has: i32 = out_ptr!();
                self.capi_call(|ctx| unsafe {
                    ffi::tiledb_domain_has_dimension(
                        ctx,
                        c_domain,
                        c_name.as_ptr(),
                        &mut c_has,
                    )
                })?;

                Ok(c_has != 0)
            }
        }
    }

    pub fn dimension<K: Into<LookupKey>>(
        &self,
        key: K,
    ) -> TileDBResult<Dimension> {
        let c_domain = *self.raw;
        let mut c_dimension: *mut ffi::tiledb_dimension_t = out_ptr!();

        match key.into() {
            LookupKey::Index(idx) => {
                let c_idx: u32 = idx.try_into().map_err(
                    |e: <usize as TryInto<u32>>::Error| {
                        crate::error::Error::InvalidArgument(anyhow!(e))
                    },
                )?;
                self.capi_call(|ctx| unsafe {
                    ffi::tiledb_domain_get_dimension_from_index(
                        ctx,
                        c_domain,
                        c_idx,
                        &mut c_dimension,
                    )
                })?;
            }
            LookupKey::Name(name) => {
                let c_name = cstring!(name);
                self.capi_call(|ctx| unsafe {
                    ffi::tiledb_domain_get_dimension_from_name(
                        ctx,
                        c_domain,
                        c_name.as_ptr(),
                        &mut c_dimension,
                    )
                })?;
            }
        }

        Ok(Dimension::new(
            &self.context,
            RawDimension::Owned(c_dimension),
        ))
    }

    pub fn dimension_index<K: Into<LookupKey>>(
        &self,
        key: K,
    ) -> TileDBResult<usize> {
        let name = match key.into() {
            LookupKey::Index(idx) => return Ok(idx),
            LookupKey::Name(name) => name,
        };

        for i in 0..self.num_dimensions()? {
            let dim = self.dimension(i)?;
            if dim.name()? == name {
                return Ok(i);
            }
        }

        Err(Error::InvalidArgument(anyhow!(
            "Dimension '{}' does not exist in this domain.",
            name
        )))
    }

    pub fn dimensions(&self) -> TileDBResult<Dimensions> {
        Dimensions::new(self)
    }
}

impl PartialEq<Domain> for Domain {
    fn eq(&self, other: &Domain) -> bool {
        let ndim_match = match (self.num_dimensions(), other.num_dimensions()) {
            (Ok(mine), Ok(theirs)) => mine == theirs,
            _ => false,
        };
        if !ndim_match {
            return false;
        }

        for d in 0..self.num_dimensions().unwrap() {
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

pub struct Dimensions<'a> {
    domain: &'a Domain,
    cursor: usize,
    bound: usize,
}

impl<'a> Dimensions<'a> {
    pub fn new(domain: &'a Domain) -> TileDBResult<Self> {
        Ok(Dimensions {
            domain,
            cursor: 0,
            bound: domain.num_dimensions()?,
        })
    }
}

impl Iterator for Dimensions<'_> {
    type Item = TileDBResult<Dimension>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.bound {
            None
        } else {
            let item = self.domain.dimension(self.cursor);
            self.cursor += 1;
            Some(item)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.bound - self.cursor;
        (exact, Some(exact))
    }
}

#[cfg(any(test, feature = "pod"))]
impl Debug for Domain {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match tiledb_pod::array::domain::DomainData::try_from(self) {
            Ok(d) => Debug::fmt(&d, f),
            Err(e) => {
                let RawDomain::Owned(ptr) = self.raw;
                write!(f, "<Domain @ {:?}: serialization error: {}>", ptr, e)
            }
        }
    }
}

pub struct Builder {
    domain: Domain,
}

impl ContextBound for Builder {
    fn context(&self) -> Context {
        self.domain.context()
    }
}

impl Builder {
    pub fn new(context: &Context) -> TileDBResult<Self> {
        let mut c_domain: *mut ffi::tiledb_domain_t = out_ptr!();
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_domain_alloc(ctx, &mut c_domain)
        })?;

        Ok(Builder {
            domain: Domain {
                context: context.clone(),
                raw: RawDomain::Owned(c_domain),
            },
        })
    }

    pub fn add_dimension(self, dimension: Dimension) -> TileDBResult<Self> {
        let c_domain = *self.domain.raw;
        let c_dim = dimension.capi();

        self.capi_call(|ctx| unsafe {
            ffi::tiledb_domain_add_dimension(ctx, c_domain, c_dim)
        })?;

        Ok(self)
    }

    pub fn build(self) -> Domain {
        self.domain
    }
}

impl From<Builder> for Domain {
    fn from(builder: Builder) -> Domain {
        builder.build()
    }
}

#[cfg(any(test, feature = "pod"))]
pub mod pod;

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use tiledb_pod::array::dimension::DimensionData;
    use tiledb_pod::array::domain::DomainData;
    use utils::assert_option_subset;

    use crate::array::domain::Builder;
    use crate::array::*;
    use crate::{Context, Datatype, Factory};

    #[test]
    fn test_add_dimension() {
        let context = Context::new().unwrap();

        // no dimensions
        {
            let domain = Builder::new(&context).unwrap().build();
            assert_eq!(0, domain.num_dimensions().unwrap());

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
                DimensionBuilder::new(
                    &context,
                    "d1",
                    Datatype::Int32,
                    (dim_domain, extent),
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
            assert_eq!(1, domain.num_dimensions().unwrap());

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
                DimensionBuilder::new(
                    &context,
                    "d1",
                    Datatype::Int32,
                    (dim1_domain, extent),
                )
                .unwrap()
                .build()
            };

            let dim1_in: Dimension = dim1_buildfn();
            let dim1_cmp: Dimension = dim1_buildfn();

            let dim2_buildfn = || {
                let dim2_domain: [f64; 2] = [-365f64, 365f64];
                let extent: f64 = 128f64;
                DimensionBuilder::new(
                    &context,
                    "d2",
                    Datatype::Float64,
                    (dim2_domain, extent),
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
            assert_eq!(2, domain.num_dimensions().unwrap());

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
                DimensionBuilder::new(
                    &context,
                    "d1",
                    Datatype::Int32,
                    ([0, 1000], 100),
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
                DimensionBuilder::new(
                    &context,
                    "d1",
                    Datatype::Float64,
                    ([0f64, 1000f64], 100f64),
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

    /// Test that the arbitrary domain construction always succeeds
    #[test]
    fn domain_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_domain in any::<DomainData>())| {
            maybe_domain.create(&ctx)
                .expect("Error constructing arbitrary domain");
        });
    }

    #[test]
    fn domain_eq_reflexivity() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(domain in any::<DomainData>())| {
            assert_eq!(domain, domain);
            assert_option_subset!(domain, domain);

            let domain = domain.create(&ctx)
                .expect("Error constructing arbitrary domain");
            assert_eq!(domain, domain);
        });
    }

    /// Test iteration over [Domain] dimensions
    fn do_test_dimensions_iter(spec: DomainData) -> TileDBResult<()> {
        let context = Context::new()?;
        let domain = spec.create(&context)?;

        let num_dimensions = domain.num_dimensions()?;
        assert_eq!(num_dimensions, spec.dimension.len());
        assert_eq!(num_dimensions, domain.dimensions()?.count());

        for (dimension_spec, dimension) in
            spec.dimension.iter().zip(domain.dimensions()?)
        {
            let dimension = DimensionData::try_from(dimension?)?;
            assert_option_subset!(dimension_spec, dimension);
        }

        Ok(())
    }

    proptest! {
        #[test]
        fn test_dimensions_iter(spec in any::<DomainData>()) {
            do_test_dimensions_iter(spec).expect("Error in do_test_dimensions_iter");
        }
    }
}
