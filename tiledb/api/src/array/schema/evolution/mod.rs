use std::ops::Deref;

use crate::array::{Attribute, Enumeration};
use crate::{Context, ContextBound, Result as TileDBResult};

enum RawSchemaEvolution {
    Owned(*mut ffi::tiledb_array_schema_evolution_t),
}

impl Deref for RawSchemaEvolution {
    type Target = *mut ffi::tiledb_array_schema_evolution_t;

    fn deref(&self) -> &Self::Target {
        let RawSchemaEvolution::Owned(ref ffi) = *self;
        ffi
    }
}

impl Drop for RawSchemaEvolution {
    fn drop(&mut self) {
        unsafe {
            let RawSchemaEvolution::Owned(ref mut ffi) = *self;
            ffi::tiledb_array_schema_evolution_free(ffi)
        }
    }
}

/// Packages operations for evolving the schema of an array.
pub struct SchemaEvolution {
    context: Context,
    raw: RawSchemaEvolution,
}

impl ContextBound for SchemaEvolution {
    fn context(&self) -> Context {
        self.context.clone()
    }
}

impl SchemaEvolution {
    pub(crate) fn capi(&self) -> *mut ffi::tiledb_array_schema_evolution_t {
        *self.raw
    }
}

/// Provides methods to accumulate a [`SchemaEvolution`].
pub struct Builder {
    inner: SchemaEvolution,
}

impl ContextBound for Builder {
    fn context(&self) -> Context {
        self.inner.context()
    }
}

impl Builder {
    pub fn new(context: &Context) -> TileDBResult<Self> {
        let mut c_evolution: *mut ffi::tiledb_array_schema_evolution_t =
            out_ptr!();

        context.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_evolution_alloc(ctx, &mut c_evolution)
        })?;

        Ok(Self {
            inner: SchemaEvolution {
                context: context.clone(),
                raw: RawSchemaEvolution::Owned(c_evolution),
            },
        })
    }

    /// Registers a new [Attribute] to add to the target array.
    pub fn add_attribute(self, attribute: Attribute) -> TileDBResult<Self> {
        let c_evolution = *self.inner.raw;
        let c_attribute = attribute.capi();

        self.context().capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_evolution_add_attribute(
                ctx,
                c_evolution,
                c_attribute,
            )
        })?;

        Ok(self)
    }

    /// Registers the name of an attribute to drop from the target array.
    pub fn drop_attribute(self, name: &str) -> TileDBResult<Self> {
        let c_evolution = *self.inner.raw;
        let c_name = cstring!(name);

        self.context().capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_evolution_drop_attribute(
                ctx,
                c_evolution,
                c_name.as_c_str().as_ptr(),
            )
        })?;

        Ok(self)
    }

    /// Registers an [Enumeration] to add to the target array.
    pub fn add_enumeration(
        self,
        enumeration: Enumeration,
    ) -> TileDBResult<Self> {
        let c_evolution = *self.inner.raw;
        let c_enumeration = enumeration.capi();

        self.context().capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_evolution_add_enumeration(
                ctx,
                c_evolution,
                c_enumeration,
            )
        })?;

        Ok(self)
    }

    // TODO: the C API doc says that the arg must be returned from
    // `tiledb_enumeration_extend`, we can enforce that
    pub fn extend_enumeration(
        self,
        enumeration: Enumeration,
    ) -> TileDBResult<Self> {
        let c_evolution = *self.inner.raw;
        let c_enumeration = enumeration.capi();

        self.context().capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_evolution_extend_enumeration(
                ctx,
                c_evolution,
                c_enumeration,
            )
        })?;

        Ok(self)
    }

    /// Registers an enumeration name to drop from the target array.
    pub fn drop_enumeration(self, name: &str) -> TileDBResult<Self> {
        let c_evolution = *self.inner.raw;
        let c_name = cstring!(name);

        self.context().capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_evolution_drop_enumeration(
                ctx,
                c_evolution,
                c_name.as_c_str().as_ptr(),
            )
        })?;

        Ok(self)
    }

    /// Sets the timestamp for the evolved schema.
    pub fn timestamp_range(self, t: u64) -> TileDBResult<Self> {
        let c_evolution = *self.inner.raw;

        self.context().capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_evolution_set_timestamp_range(
                ctx,
                c_evolution,
                t,
                t,
            )
        })?;

        Ok(self)
    }

    pub fn build(self) -> SchemaEvolution {
        self.inner
    }
}

#[cfg(test)]
mod tests;
