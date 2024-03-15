use std::ops::Deref;

use crate::array::{Attribute, Domain};
use crate::context::Context;
use crate::Result as TileDBResult;

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

/// Wrapper for the CAPI handle.
/// Ensures that the CAPI structure is freed.
pub(crate) struct RawSchema {
    ffi: *mut ffi::tiledb_array_schema_t,
}

impl RawSchema {
    pub fn new(ffi: *mut ffi::tiledb_array_schema_t) -> Self {
        RawSchema { ffi }
    }
}

impl Deref for RawSchema {
    type Target = *mut ffi::tiledb_array_schema_t;

    fn deref(&self) -> &Self::Target {
        &self.ffi
    }
}

impl Drop for RawSchema {
    fn drop(&mut self) {
        unsafe { ffi::tiledb_array_schema_free(&mut self.ffi) }
    }
}

pub struct Schema<'ctx> {
    context: &'ctx Context,
    raw: RawSchema,
}

impl<'ctx> Schema<'ctx> {
    pub(crate) fn as_mut_ptr(&self) -> *mut ffi::tiledb_array_schema_t {
        *self.raw
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
    ) -> TileDBResult<Self> {
        let c_array_type = array_type.capi_enum();
        let mut c_schema: *mut ffi::tiledb_array_schema_t =
            std::ptr::null_mut();
        if unsafe {
            ffi::tiledb_array_schema_alloc(
                context.as_mut_ptr(),
                c_array_type,
                &mut c_schema,
            )
        } == ffi::TILEDB_OK
        {
            Ok(Builder {
                schema: Schema {
                    context,
                    raw: RawSchema::new(c_schema),
                },
            })
        } else {
            Err(context.expect_last_error())
        }
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

    pub fn domain(self, domain: Domain) -> TileDBResult<Self> {
        let c_context = self.schema.context.as_mut_ptr();
        let c_domain = domain.capi();
        let c_ret = unsafe {
            ffi::tiledb_array_schema_set_domain(
                c_context,
                *self.schema.raw,
                c_domain,
            )
        };
        if c_ret == ffi::TILEDB_OK {
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

impl<'ctx> Into<Schema<'ctx>> for Builder<'ctx> {
    fn into(self) -> Schema<'ctx> {
        self.build()
    }
}

#[cfg(test)]
mod tests {
    use crate::array::schema::*;

    #[test]
    fn test_get_version() {
        let c: Context = Context::new().unwrap();

        let b: Builder = Builder::new(&c, ArrayType::Dense)
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
            let b: Builder = Builder::new(&c, ArrayType::Dense)
                .unwrap()
                .allow_duplicates(false)
                .unwrap();

            let s: Schema = b.into();
            assert!(!s.allows_duplicates());
        }
        // dense, duplicates (should error)
        {
            let e = Builder::new(&c, ArrayType::Dense)
                .unwrap()
                .allow_duplicates(true);
            assert!(e.is_err());
        }
        // sparse, no duplicates
        {
            let b: Builder = Builder::new(&c, ArrayType::Sparse)
                .unwrap()
                .allow_duplicates(false)
                .unwrap();

            let s: Schema = b.into();
            assert!(!s.allows_duplicates());
        }
        // sparse, duplicates
        {
            let b: Builder = Builder::new(&c, ArrayType::Sparse)
                .unwrap()
                .allow_duplicates(true)
                .unwrap();

            let s: Schema = b.into();
            assert!(s.allows_duplicates());
        }
    }
}
