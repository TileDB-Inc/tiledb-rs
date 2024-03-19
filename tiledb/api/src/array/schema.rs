use std::convert::TryFrom;
use std::ops::Deref;

use crate::array::domain::RawDomain;
use crate::array::{Attribute, Domain};
use crate::context::Context;
use crate::Result as TileDBResult;

#[derive(Debug, PartialEq)]
pub enum ArrayType {
    Dense,
    Sparse,
}

impl ArrayType {
    pub(crate) fn capi_enum(&self) -> ffi::tiledb_array_type_t {
        match *self {
            ArrayType::Dense => ffi::tiledb_array_type_t_TILEDB_DENSE,
            ArrayType::Sparse => ffi::tiledb_array_type_t_TILEDB_SPARSE,
        }
    }
}

impl TryFrom<ffi::tiledb_array_type_t> for ArrayType {
    type Error = crate::error::Error;
    fn try_from(value: ffi::tiledb_array_type_t) -> TileDBResult<Self> {
        match value {
            ffi::tiledb_array_type_t_TILEDB_DENSE => Ok(ArrayType::Dense),
            ffi::tiledb_array_type_t_TILEDB_SPARSE => Ok(ArrayType::Sparse),
            _ => {
                Err(Self::Error::from(format!("Invalid array type: {}", value)))
            }
        }
    }
}

/// Wrapper for the CAPI handle.
/// Ensures that the CAPI structure is freed.
pub(crate) enum RawSchema {
    Owned(*mut ffi::tiledb_array_schema_t),
}

impl Deref for RawSchema {
    type Target = *mut ffi::tiledb_array_schema_t;

    fn deref(&self) -> &Self::Target {
        let RawSchema::Owned(ref ffi) = *self;
        ffi
    }
}

impl Drop for RawSchema {
    fn drop(&mut self) {
        unsafe {
            let RawSchema::Owned(ref mut ffi) = *self;
            ffi::tiledb_array_schema_free(ffi)
        }
    }
}

pub struct Schema<'ctx> {
    context: &'ctx Context,
    raw: RawSchema,
}

impl<'ctx> Schema<'ctx> {
    pub(crate) fn new(context: &'ctx Context, raw: RawSchema) -> Self {
        Schema { context, raw }
    }

    pub(crate) fn as_mut_ptr(&self) -> *mut ffi::tiledb_array_schema_t {
        *self.raw
    }

    pub fn domain(&self) -> TileDBResult<Domain<'ctx>> {
        let c_context: *mut ffi::tiledb_ctx_t = self.context.as_mut_ptr();
        let c_schema = *self.raw;
        let mut c_domain: *mut ffi::tiledb_domain_t = out_ptr!();
        let c_ret = unsafe {
            ffi::tiledb_array_schema_get_domain(
                c_context,
                c_schema,
                &mut c_domain,
            )
        };
        if c_ret == ffi::TILEDB_OK {
            Ok(Domain::new(self.context, RawDomain::Owned(c_domain)))
        } else {
            Err(self.context.expect_last_error())
        }
    }

    /// Retrieve the schema of an array from storage
    pub fn load(context: &'ctx Context, uri: &str) -> TileDBResult<Self> {
        let c_context: *mut ffi::tiledb_ctx_t = context.as_mut_ptr();
        let c_uri = cstring!(uri);
        let mut c_schema: *mut ffi::tiledb_array_schema_t = out_ptr!();

        let c_ret = unsafe {
            ffi::tiledb_array_schema_load(
                c_context,
                c_uri.as_ptr(),
                &mut c_schema,
            )
        };
        if c_ret == ffi::TILEDB_OK {
            Ok(Schema::new(context, RawSchema::Owned(c_schema)))
        } else {
            Err(context.expect_last_error())
        }
    }

    pub fn version(&self) -> i64 {
        let mut c_ret: std::os::raw::c_int = out_ptr!();
        if unsafe {
            ffi::tiledb_array_schema_get_allows_dups(
                self.context.as_mut_ptr(),
                self.as_mut_ptr(),
                &mut c_ret,
            )
        } == ffi::TILEDB_OK
        {
            c_ret as i64
        } else {
            unreachable!("Rust API design should prevent sanity check failure")
        }
    }

    pub fn allows_duplicates(&self) -> bool {
        let mut c_ret: std::os::raw::c_int = out_ptr!();
        if unsafe {
            ffi::tiledb_array_schema_get_allows_dups(
                self.context.as_mut_ptr(),
                self.as_mut_ptr(),
                &mut c_ret,
            )
        } == ffi::TILEDB_OK
        {
            c_ret != 0
        } else {
            unreachable!("Rust API design should prevent sanity check failure")
        }
    }
}

pub struct Builder<'ctx> {
    schema: Schema<'ctx>,
}

impl<'ctx> Builder<'ctx> {
    pub fn new(
        context: &'ctx Context,
        array_type: ArrayType,
        domain: Domain<'ctx>,
    ) -> TileDBResult<Self> {
        let c_context = context.as_mut_ptr();
        let c_array_type = array_type.capi_enum();
        let mut c_schema: *mut ffi::tiledb_array_schema_t =
            std::ptr::null_mut();
        let c_alloc_ret = unsafe {
            ffi::tiledb_array_schema_alloc(
                c_context,
                c_array_type,
                &mut c_schema,
            )
        };
        if c_alloc_ret != ffi::TILEDB_OK {
            return Err(context.expect_last_error());
        }

        let c_domain = domain.capi();
        let c_domain_ret = unsafe {
            ffi::tiledb_array_schema_set_domain(c_context, c_schema, c_domain)
        };
        if c_domain_ret != ffi::TILEDB_OK {
            return Err(context.expect_last_error());
        }

        Ok(Builder {
            schema: Schema {
                context,
                raw: RawSchema::Owned(c_schema),
            },
        })
    }

    pub fn allow_duplicates(self, allow: bool) -> TileDBResult<Self> {
        let c_allow = if allow { 1 } else { 0 };
        if unsafe {
            ffi::tiledb_array_schema_set_allows_dups(
                self.schema.context.as_mut_ptr(),
                *self.schema.raw,
                c_allow,
            )
        } == ffi::TILEDB_OK
        {
            Ok(self)
        } else {
            Err(self.schema.context.expect_last_error())
        }
    }

    pub fn add_attribute(self, attr: Attribute) -> TileDBResult<Self> {
        if unsafe {
            ffi::tiledb_array_schema_add_attribute(
                self.schema.context.as_mut_ptr(),
                *self.schema.raw,
                attr.as_mut_ptr(),
            )
        } == ffi::TILEDB_OK
        {
            Ok(self)
        } else {
            Err(self.schema.context.expect_last_error())
        }
    }

    pub fn build(self) -> Schema<'ctx> {
        self.schema
    }
}

impl<'ctx> From<Builder<'ctx>> for Schema<'ctx> {
    fn from(builder: Builder<'ctx>) -> Schema<'ctx> {
        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use tempdir::TempDir;

    use crate::array::schema::*;
    use crate::array::tests::*;
    use crate::array::{DimensionBuilder, DomainBuilder};
    use crate::context::Context;
    use crate::Datatype;

    /// Helper function to make a Domain which isn't needed for the purposes of the test
    fn unused_domain(c: &Context) -> Domain {
        let dim = DimensionBuilder::new::<i32>(
            c,
            "test",
            Datatype::Int32,
            &[-100, 100],
            &100,
        )
        .unwrap()
        .build();
        DomainBuilder::new(c)
            .unwrap()
            .add_dimension(dim)
            .unwrap()
            .build()
    }

    #[test]
    fn test_get_version() {
        let c: Context = Context::new().unwrap();

        let b: Builder = Builder::new(&c, ArrayType::Dense, unused_domain(&c))
            .unwrap()
            .allow_duplicates(false)
            .unwrap();

        let s: Schema = b.into();
        assert_eq!(0, s.version());
    }

    #[test]
    fn test_allow_duplicates() {
        let c: Context = Context::new().unwrap();

        // dense, no duplicates
        {
            let b: Builder =
                Builder::new(&c, ArrayType::Dense, unused_domain(&c))
                    .unwrap()
                    .allow_duplicates(false)
                    .unwrap();

            let s: Schema = b.into();
            assert!(!s.allows_duplicates());
        }
        // dense, duplicates (should error)
        {
            let e = Builder::new(&c, ArrayType::Dense, unused_domain(&c))
                .unwrap()
                .allow_duplicates(true);
            assert!(e.is_err());
        }
        // sparse, no duplicates
        {
            let b: Builder =
                Builder::new(&c, ArrayType::Sparse, unused_domain(&c))
                    .unwrap()
                    .allow_duplicates(false)
                    .unwrap();

            let s: Schema = b.into();
            assert!(!s.allows_duplicates());
        }
        // sparse, duplicates
        {
            let b: Builder =
                Builder::new(&c, ArrayType::Sparse, unused_domain(&c))
                    .unwrap()
                    .allow_duplicates(true)
                    .unwrap();

            let s: Schema = b.into();
            assert!(s.allows_duplicates());
        }
    }

    #[test]
    fn test_load() -> io::Result<()> {
        let tmp_dir = TempDir::new("tiledb_array_schema_test_load")?;

        let c: Context = Context::new().unwrap();

        let r = create_quickstart_dense(&tmp_dir, &c);
        assert!(r.is_ok());

        let schema = Schema::load(&c, &r.unwrap())
            .expect("Could not open quickstart_dense schema");

        let domain = schema.domain().expect("Error reading domain");

        let rows = domain.dimension(0).expect("Error reading rows dimension");
        assert_eq!(Datatype::Int32, rows.datatype());
        // TODO: add method to check min/max

        let cols = domain.dimension(1).expect("Error reading cols dimension");
        assert_eq!(Datatype::Int32, rows.datatype());
        // TODO: add method to check min/max

        let rows_domain = rows.domain::<i32>().unwrap();
        assert_eq!(rows_domain[0], 1);
        assert_eq!(rows_domain[1], 4);

        let cols_domain = cols.domain::<i32>().unwrap();
        assert_eq!(cols_domain[0], 1);
        assert_eq!(cols_domain[1], 4);

        // Make sure we can remove the array we created.
        tmp_dir.close()?;

        Ok(())
    }
}
