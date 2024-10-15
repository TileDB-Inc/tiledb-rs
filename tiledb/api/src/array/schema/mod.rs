use std::borrow::Borrow;
use std::num::NonZeroUsize;
use std::ops::Deref;

#[cfg(any(test, feature = "serde"))]
use std::fmt::{Debug, Formatter, Result as FmtResult};

use anyhow::anyhow;

use crate::array::attribute::RawAttribute;
use crate::array::dimension::Dimension;
use crate::array::domain::RawDomain;
use crate::array::enumeration::Enumeration;
use crate::array::{Attribute, CellOrder, Domain, TileOrder};
use crate::context::{CApiInterface, Context, ContextBound};
use crate::error::Error;
use crate::filter::list::{FilterList, RawFilterList};
use crate::key::LookupKey;
use crate::query::read::output::FieldScratchAllocator;
use crate::Datatype;
use crate::Result as TileDBResult;

pub use tiledb_common::array::{ArrayType, CellValNum};

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

/// Holds a field of the schema, which may be either a dimension or an attribute.
#[derive(PartialEq)]
pub enum Field {
    Dimension(Dimension),
    Attribute(Attribute),
}

impl Field {
    pub fn is_attribute(&self) -> bool {
        matches!(self, Self::Attribute(_))
    }

    pub fn is_dimension(&self) -> bool {
        matches!(self, Self::Dimension(_))
    }

    pub fn name(&self) -> TileDBResult<String> {
        match self {
            Field::Dimension(ref d) => d.name(),
            Field::Attribute(ref a) => a.name(),
        }
    }

    pub fn datatype(&self) -> TileDBResult<Datatype> {
        match self {
            Field::Dimension(ref d) => d.datatype(),
            Field::Attribute(ref a) => a.datatype(),
        }
    }

    pub fn nullability(&self) -> TileDBResult<bool> {
        Ok(match self {
            Field::Dimension(_) => false,
            Field::Attribute(ref a) => a.is_nullable()?,
        })
    }

    pub fn cell_val_num(&self) -> TileDBResult<CellValNum> {
        match self {
            Field::Dimension(ref d) => d.cell_val_num(),
            Field::Attribute(ref a) => a.cell_val_num(),
        }
    }

    pub fn query_scratch_allocator(
        &self,
        memory_limit: Option<usize>,
    ) -> TileDBResult<crate::query::read::output::FieldScratchAllocator> {
        /*
         * Allocate space for the largest integral number of cells
         * which fits within the memory limit.
         */
        let est_values_per_cell = match self.cell_val_num()? {
            CellValNum::Fixed(nz) => nz.get() as usize,
            CellValNum::Var => 64,
        };
        let est_cell_size = est_values_per_cell * self.datatype()?.size();

        let est_cell_capacity = memory_limit
            .unwrap_or(FieldScratchAllocator::DEFAULT_MEMORY_LIMIT)
            / est_cell_size;

        Ok(FieldScratchAllocator {
            cell_val_num: self.cell_val_num().unwrap_or_default(),
            record_capacity: NonZeroUsize::new(est_cell_capacity).unwrap(),
            is_nullable: self.nullability().unwrap_or(true),
        })
    }
}

impl From<Dimension> for Field {
    fn from(dim: Dimension) -> Field {
        Field::Dimension(dim)
    }
}

impl From<Attribute> for Field {
    fn from(attr: Attribute) -> Field {
        Field::Attribute(attr)
    }
}

type FnFilterListGet = unsafe extern "C" fn(
    *mut ffi::tiledb_ctx_t,
    *mut ffi::tiledb_array_schema_t,
    *mut *mut ffi::tiledb_filter_list_t,
) -> i32;

pub struct Schema {
    context: Context,
    raw: RawSchema,
}

impl ContextBound for Schema {
    fn context(&self) -> Context {
        self.context.clone()
    }
}

impl Schema {
    pub(crate) fn capi(&self) -> *mut ffi::tiledb_array_schema_t {
        *self.raw
    }

    pub(crate) fn new(context: &Context, raw: RawSchema) -> Self {
        Schema {
            context: context.clone(),
            raw,
        }
    }

    pub fn domain(&self) -> TileDBResult<Domain> {
        let c_schema = *self.raw;
        let mut c_domain: *mut ffi::tiledb_domain_t = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_get_domain(ctx, c_schema, &mut c_domain)
        })?;

        Ok(Domain::new(&self.context, RawDomain::Owned(c_domain)))
    }

    /// Retrieve the schema of an array from storage
    pub fn load<S>(context: &Context, uri: S) -> TileDBResult<Self>
    where
        S: AsRef<str>,
    {
        let c_uri = cstring!(uri.as_ref());
        let mut c_schema: *mut ffi::tiledb_array_schema_t = out_ptr!();

        context.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_load(ctx, c_uri.as_ptr(), &mut c_schema)
        })?;

        Ok(Schema::new(context, RawSchema::Owned(c_schema)))
    }

    pub fn version(&self) -> TileDBResult<u32> {
        let c_schema = self.capi();
        let mut c_version: u32 = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_get_version(ctx, c_schema, &mut c_version)
        })?;

        Ok(c_version)
    }

    pub fn array_type(&self) -> TileDBResult<ArrayType> {
        let c_schema = self.capi();
        let mut c_atype: ffi::tiledb_array_type_t = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_get_array_type(ctx, c_schema, &mut c_atype)
        })?;

        Ok(ArrayType::try_from(c_atype)?)
    }

    /// Returns the sparse tile capacity for this schema,
    /// i.e. the number of cells which are contained in each tile of a sparse schema.
    /// If this is a dense array schema, the value returned is
    /// not used by tiledb.
    pub fn capacity(&self) -> TileDBResult<u64> {
        let c_schema = self.capi();
        let mut c_capacity: u64 = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_get_capacity(
                ctx,
                c_schema,
                &mut c_capacity,
            )
        })?;

        Ok(c_capacity)
    }

    pub fn cell_order(&self) -> TileDBResult<CellOrder> {
        let c_schema = *self.raw;
        let mut c_cell_order: ffi::tiledb_layout_t = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_get_cell_order(
                ctx,
                c_schema,
                &mut c_cell_order,
            )
        })?;

        Ok(CellOrder::try_from(c_cell_order)?)
    }

    pub fn tile_order(&self) -> TileDBResult<TileOrder> {
        let c_schema = self.capi();
        let mut c_tile_order: ffi::tiledb_layout_t = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_get_tile_order(
                ctx,
                c_schema,
                &mut c_tile_order,
            )
        })?;

        Ok(TileOrder::try_from(c_tile_order)?)
    }

    /// Returns whether duplicate coordinates are permitted.
    ///
    /// - For a dense array schema, this is always `false`.
    /// - For a sparse array schema, if set to `true`, then any number
    ///   of cells may be written with the same coordinates.
    pub fn allows_duplicates(&self) -> TileDBResult<bool> {
        let c_schema = self.capi();
        let mut c_allows_duplicates: std::os::raw::c_int = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_get_allows_dups(
                ctx,
                c_schema,
                &mut c_allows_duplicates,
            )
        })?;
        Ok(c_allows_duplicates != 0)
    }

    pub fn num_attributes(&self) -> TileDBResult<usize> {
        let c_schema = *self.raw;
        let mut c_nattrs: u32 = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_get_attribute_num(
                ctx,
                c_schema,
                &mut c_nattrs,
            )
        })?;
        Ok(c_nattrs as usize)
    }

    pub fn attribute<K: Into<LookupKey>>(
        &self,
        key: K,
    ) -> TileDBResult<Attribute> {
        let c_schema = *self.raw;
        let mut c_attr: *mut ffi::tiledb_attribute_t = out_ptr!();

        match key.into() {
            LookupKey::Index(idx) => {
                let c_idx: u32 = idx.try_into().map_err(
                    |e: <usize as TryInto<u32>>::Error| {
                        Error::InvalidArgument(anyhow!(e))
                    },
                )?;
                self.capi_call(|ctx| unsafe {
                    ffi::tiledb_array_schema_get_attribute_from_index(
                        ctx,
                        c_schema,
                        c_idx,
                        &mut c_attr,
                    )
                })?;
            }
            LookupKey::Name(name) => {
                let c_name = cstring!(name);
                self.capi_call(|ctx| unsafe {
                    ffi::tiledb_array_schema_get_attribute_from_name(
                        ctx,
                        c_schema,
                        c_name.as_ptr(),
                        &mut c_attr,
                    )
                })?;
            }
        }

        Ok(Attribute::new(&self.context, RawAttribute::Owned(c_attr)))
    }

    pub fn num_fields(&self) -> TileDBResult<usize> {
        Ok(self.domain()?.num_dimensions()? + self.num_attributes()?)
    }

    /// Returns a reference to a field (dimension or attribute) in this schema.
    /// If the key is an index, then values `[0.. ndimensions]` will look
    /// up a dimension, and values outside that range will be adjusted by `ndimensions`
    /// to look up an attribute.
    pub fn field<K: Into<LookupKey>>(&self, key: K) -> TileDBResult<Field> {
        let domain = self.domain()?;
        match key.into() {
            LookupKey::Index(idx) => {
                let ndim = domain.num_dimensions()?;
                if idx < ndim {
                    Ok(Field::Dimension(domain.dimension(idx)?))
                } else {
                    Ok(Field::Attribute(self.attribute(idx - ndim)?))
                }
            }
            LookupKey::Name(name) => {
                if domain.has_dimension(&name)? {
                    Ok(Field::Dimension(domain.dimension(name)?))
                } else {
                    Ok(Field::Attribute(self.attribute(name)?))
                }
            }
        }
    }

    pub fn fields(&self) -> TileDBResult<Fields<'_>> {
        Fields::new(self)
    }

    fn filter_list(
        &self,
        ffi_function: FnFilterListGet,
    ) -> TileDBResult<FilterList> {
        let c_schema = *self.raw;
        let mut c_filters: *mut ffi::tiledb_filter_list_t = out_ptr!();

        self.capi_call(|ctx| unsafe {
            ffi_function(ctx, c_schema, &mut c_filters)
        })?;
        Ok(FilterList {
            context: self.context.clone(),
            raw: RawFilterList::Owned(c_filters),
        })
    }

    pub fn coordinate_filters(&self) -> TileDBResult<FilterList> {
        self.filter_list(ffi::tiledb_array_schema_get_coords_filter_list)
    }

    pub fn offsets_filters(&self) -> TileDBResult<FilterList> {
        self.filter_list(ffi::tiledb_array_schema_get_offsets_filter_list)
    }

    pub fn nullity_filters(&self) -> TileDBResult<FilterList> {
        self.filter_list(ffi::tiledb_array_schema_get_validity_filter_list)
    }
}

impl PartialEq<Schema> for Schema {
    fn eq(&self, other: &Schema) -> bool {
        eq_helper!(self.num_attributes(), other.num_attributes());
        eq_helper!(self.version(), other.version());
        eq_helper!(self.array_type(), other.array_type());
        eq_helper!(self.capacity(), other.capacity());
        eq_helper!(self.cell_order(), other.cell_order());
        eq_helper!(self.tile_order(), other.tile_order());
        eq_helper!(self.allows_duplicates(), other.allows_duplicates());
        eq_helper!(self.coordinate_filters(), other.coordinate_filters());
        eq_helper!(self.offsets_filters(), other.offsets_filters());
        eq_helper!(self.nullity_filters(), other.nullity_filters());

        for a in 0..self.num_attributes().unwrap() {
            eq_helper!(self.attribute(a), other.attribute(a));
        }

        eq_helper!(self.domain(), other.domain());

        true
    }
}

#[cfg(any(test, feature = "serde"))]
impl Debug for Schema {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match tiledb_serde::array::schema::SchemaData::try_from(self) {
            Ok(s) => Debug::fmt(&s, f),
            Err(e) => {
                let RawSchema::Owned(ptr) = self.raw;
                write!(f, "<Schema @ {:?}: serialization error: {}>", ptr, e)
            }
        }
    }
}

pub struct Fields<'a> {
    schema: &'a Schema,
    cursor: usize,
    bound: usize,
}

impl<'a> Fields<'a> {
    pub fn new(schema: &'a Schema) -> TileDBResult<Self> {
        Ok(Fields {
            schema,
            cursor: 0,
            bound: schema.num_fields()?,
        })
    }

    pub fn num_fields(&self) -> usize {
        self.bound
    }
}

impl Iterator for Fields<'_> {
    type Item = TileDBResult<Field>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor < self.bound {
            let item = self.schema.field(self.cursor);
            self.cursor += 1;
            Some(item)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.bound - self.cursor;
        (exact, Some(exact))
    }
}

impl std::iter::FusedIterator for Fields<'_> {}

type FnFilterListSet = unsafe extern "C" fn(
    *mut ffi::tiledb_ctx_t,
    *mut ffi::tiledb_array_schema_t,
    *mut ffi::tiledb_filter_list_t,
) -> i32;

pub struct Builder {
    schema: Schema,
}

impl ContextBound for Builder {
    fn context(&self) -> Context {
        self.schema.context()
    }
}

impl Builder {
    pub fn new(
        context: &Context,
        array_type: ArrayType,
        domain: Domain,
    ) -> TileDBResult<Self> {
        let c_array_type = ffi::tiledb_array_type_t::from(array_type);
        let mut c_schema: *mut ffi::tiledb_array_schema_t =
            std::ptr::null_mut();
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_alloc(ctx, c_array_type, &mut c_schema)
        })?;

        let c_domain = domain.capi();
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_set_domain(ctx, c_schema, c_domain)
        })?;

        Ok(Builder {
            schema: Schema {
                context: context.clone(),
                raw: RawSchema::Owned(c_schema),
            },
        })
    }

    /// Set the sparse tile capacity of this schema.
    ///
    /// # Errors
    ///
    /// This function is not guaranteed to error if this schema
    /// is for a dense array - this method may instead have no effect.
    pub fn capacity(self, capacity: u64) -> TileDBResult<Self> {
        let c_schema = *self.schema.raw;
        self.context().capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_set_capacity(ctx, c_schema, capacity)
        })?;
        Ok(self)
    }

    pub fn cell_order(self, order: CellOrder) -> TileDBResult<Self> {
        let c_schema = *self.schema.raw;
        let c_order = ffi::tiledb_layout_t::from(order);
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_set_cell_order(ctx, c_schema, c_order)
        })?;
        Ok(self)
    }

    pub fn tile_order(self, order: TileOrder) -> TileDBResult<Self> {
        let c_schema = *self.schema.raw;
        let c_order = ffi::tiledb_layout_t::from(order);
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_set_tile_order(ctx, c_schema, c_order)
        })?;
        Ok(self)
    }

    /// Sets whether the array schema allows duplicate coordinate values to
    /// be written.
    ///
    /// - For a dense array schema, duplicate coordinate values are not permitted and this function
    ///   returns `Err`.
    /// - For sparse array values, any setting is permitted.
    ///
    /// Returns `self` if there is not an error.
    pub fn allow_duplicates(self, allow: bool) -> TileDBResult<Self> {
        let c_schema = self.schema.capi();
        let c_allow = if allow { 1 } else { 0 };
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_set_allows_dups(ctx, c_schema, c_allow)
        })?;
        Ok(self)
    }

    pub fn add_attribute(self, attr: Attribute) -> TileDBResult<Self> {
        let c_schema = self.schema.capi();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_add_attribute(ctx, c_schema, attr.capi())
        })?;
        Ok(self)
    }

    /// Add an enumeration to the schema for use by attributes.
    ///
    /// Note that enumerations must be added to the schema before any
    /// attributes that reference them.
    pub fn add_enumeration(self, enmr: Enumeration) -> TileDBResult<Self> {
        let c_schema = self.schema.capi();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_add_enumeration(ctx, c_schema, enmr.capi())
        })?;
        Ok(self)
    }

    fn filter_list<FL>(
        self,
        filters: FL,
        ffi_function: FnFilterListSet,
    ) -> TileDBResult<Self>
    where
        FL: Borrow<FilterList>,
    {
        let c_schema = self.schema.capi();
        let filters = filters.borrow();
        self.capi_call(|ctx| unsafe {
            ffi_function(ctx, c_schema, filters.capi())
        })?;
        Ok(self)
    }

    pub fn coordinate_filters<FL>(self, filters: FL) -> TileDBResult<Self>
    where
        FL: Borrow<FilterList>,
    {
        self.filter_list(
            filters,
            ffi::tiledb_array_schema_set_coords_filter_list,
        )
    }

    pub fn offsets_filters<FL>(self, filters: FL) -> TileDBResult<Self>
    where
        FL: Borrow<FilterList>,
    {
        self.filter_list(
            filters,
            ffi::tiledb_array_schema_set_offsets_filter_list,
        )
    }

    pub fn nullity_filters<FL>(self, filters: FL) -> TileDBResult<Self>
    where
        FL: Borrow<FilterList>,
    {
        self.filter_list(
            filters,
            ffi::tiledb_array_schema_set_validity_filter_list,
        )
    }

    pub fn build(self) -> TileDBResult<Schema> {
        let c_schema = *self.schema.raw;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_check(ctx, c_schema)
        })
        .map(|_| self.schema)
    }
}

impl TryFrom<Builder> for Schema {
    type Error = crate::error::Error;

    fn try_from(builder: Builder) -> TileDBResult<Schema> {
        builder.build()
    }
}

#[cfg(feature = "arrow")]
pub mod arrow;

#[cfg(any(test, feature = "serde"))]
pub mod serde;

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use tiledb_common::physical_type_go;
    use tiledb_serde::array::attribute::AttributeData;
    use tiledb_serde::array::dimension::DimensionData;
    use tiledb_serde::array::domain::DomainData;
    use tiledb_serde::array::schema::SchemaData;
    use uri::{self, TestArrayUri};
    use utils::assert_option_subset;

    use super::*;
    use crate::array::tests::create_quickstart_dense;
    use crate::array::{
        AttributeBuilder, DimensionBuilder, DimensionConstraints, DomainBuilder,
    };
    use crate::filter::{
        CompressionData, CompressionType, FilterData, FilterListBuilder,
    };
    use crate::{Context, Factory};

    fn sample_attribute(c: &Context) -> Attribute {
        AttributeBuilder::new(c, "a1", Datatype::Int32)
            .unwrap()
            .build()
    }

    // helper function since schemata must have at least one attribute to be valid
    fn with_attribute(c: &Context, b: Builder) -> Builder {
        b.add_attribute(sample_attribute(c)).unwrap()
    }

    fn sample_domain_builder(c: &Context) -> DomainBuilder {
        let dim = DimensionBuilder::new(
            c,
            "test",
            Datatype::Int32,
            ([-100, 100], 100),
        )
        .unwrap()
        .build();
        DomainBuilder::new(c).unwrap().add_dimension(dim).unwrap()
    }

    /// Helper function to make a quick Domain
    fn sample_domain(c: &Context) -> Domain {
        sample_domain_builder(c).build()
    }

    #[test]
    fn test_get_version() {
        let c: Context = Context::new().unwrap();

        let b: Builder = with_attribute(
            &c,
            Builder::new(&c, ArrayType::Dense, sample_domain(&c)).unwrap(),
        )
        .allow_duplicates(false)
        .unwrap();

        let s: Schema = b.build().unwrap();

        let schema_version = s.version().unwrap();

        // it's a little awkward to use a constant here but this does
        // not appear to be tied to the library version
        assert_eq!(schema_version, 22);
    }

    #[test]
    fn test_array_type() -> TileDBResult<()> {
        let c: Context = Context::new()?;

        {
            let s: Schema = with_attribute(
                &c,
                Builder::new(&c, ArrayType::Dense, sample_domain(&c)).unwrap(),
            )
            .build()
            .unwrap();
            let t = s.array_type().unwrap();
            assert_eq!(ArrayType::Dense, t);
        }

        {
            let s: Schema = with_attribute(
                &c,
                Builder::new(&c, ArrayType::Sparse, sample_domain(&c)).unwrap(),
            )
            .build()
            .unwrap();
            let t = s.array_type().unwrap();
            assert_eq!(ArrayType::Sparse, t);
        }

        Ok(())
    }

    #[test]
    fn test_capacity() -> TileDBResult<()> {
        let c: Context = Context::new()?;

        {
            let cap_in = 100;
            let s: Schema = with_attribute(
                &c,
                Builder::new(&c, ArrayType::Dense, sample_domain(&c))
                    .unwrap()
                    .capacity(cap_in)
                    .unwrap(),
            )
            .build()
            .unwrap();
            let cap_out = s.capacity().unwrap();
            assert_eq!(cap_in, cap_out);
        }
        Ok(())
    }

    #[test]
    fn test_allow_duplicates() {
        let c: Context = Context::new().unwrap();

        // dense, no duplicates
        {
            let b: Builder = with_attribute(
                &c,
                Builder::new(&c, ArrayType::Dense, sample_domain(&c)).unwrap(),
            )
            .allow_duplicates(false)
            .unwrap();

            let s: Schema = b.build().unwrap();
            assert!(!s.allows_duplicates().unwrap());
        }
        // dense, duplicates (should error)
        {
            let e = with_attribute(
                &c,
                Builder::new(&c, ArrayType::Dense, sample_domain(&c)).unwrap(),
            )
            .allow_duplicates(true);
            assert!(e.is_err());
        }
        // sparse, no duplicates
        {
            let b: Builder = with_attribute(
                &c,
                Builder::new(&c, ArrayType::Sparse, sample_domain(&c)).unwrap(),
            )
            .allow_duplicates(false)
            .unwrap();

            let s: Schema = b.build().unwrap();
            assert!(!s.allows_duplicates().unwrap());
        }
        // sparse, duplicates
        {
            let b: Builder = with_attribute(
                &c,
                Builder::new(&c, ArrayType::Sparse, sample_domain(&c)).unwrap(),
            )
            .allow_duplicates(true)
            .unwrap();

            let s: Schema = b.build().unwrap();
            assert!(s.allows_duplicates().unwrap());
        }
    }

    #[test]
    fn test_load() -> TileDBResult<()> {
        let test_uri = uri::get_uri_generator()
            .map_err(|e| Error::Other(e.to_string()))?;

        let c: Context = Context::new().unwrap();

        let r = create_quickstart_dense(&test_uri, &c);
        assert!(r.is_ok());
        let uri = r.ok().unwrap();

        let schema = Schema::load(&c, uri)
            .expect("Could not open quickstart_dense schema");

        let domain = schema.domain().expect("Error reading domain");

        let rows = domain.dimension(0).expect("Error reading rows dimension");
        assert_eq!(Datatype::Int32, rows.datatype().unwrap());
        // TODO: add method to check min/max

        let cols = domain.dimension(1).expect("Error reading cols dimension");
        assert_eq!(Datatype::Int32, rows.datatype().unwrap());
        // TODO: add method to check min/max

        let rows_domain = rows.domain::<i32>().unwrap().unwrap();
        assert_eq!(rows_domain[0], 1);
        assert_eq!(rows_domain[1], 4);

        let cols_domain = cols.domain::<i32>().unwrap().unwrap();
        assert_eq!(cols_domain[0], 1);
        assert_eq!(cols_domain[1], 4);

        // Make sure we can remove the array we created.
        test_uri.close().map_err(|e| Error::Other(e.to_string()))
    }

    #[test]
    fn test_layout() -> TileDBResult<()> {
        let c: Context = Context::new()?;

        {
            let s: Schema = with_attribute(
                &c,
                Builder::new(&c, ArrayType::Dense, sample_domain(&c)).unwrap(),
            )
            .tile_order(TileOrder::RowMajor)
            .unwrap()
            .cell_order(CellOrder::RowMajor)
            .unwrap()
            .build()
            .unwrap();
            let tile = s.tile_order().unwrap();
            let cell = s.cell_order().unwrap();
            assert_eq!(TileOrder::RowMajor, tile);
            assert_eq!(CellOrder::RowMajor, cell);
        }
        {
            let s: Schema = with_attribute(
                &c,
                Builder::new(&c, ArrayType::Dense, sample_domain(&c)).unwrap(),
            )
            .tile_order(TileOrder::ColumnMajor)
            .unwrap()
            .cell_order(CellOrder::ColumnMajor)
            .unwrap()
            .build()
            .unwrap();
            let tile = s.tile_order().unwrap();
            let cell = s.cell_order().unwrap();
            assert_eq!(TileOrder::ColumnMajor, tile);
            assert_eq!(CellOrder::ColumnMajor, cell);
        }
        {
            let r = with_attribute(
                &c,
                Builder::new(&c, ArrayType::Dense, sample_domain(&c)).unwrap(),
            )
            .cell_order(CellOrder::Hilbert);
            assert!(r.is_err());
        }
        {
            let s: Schema = with_attribute(
                &c,
                Builder::new(&c, ArrayType::Sparse, sample_domain(&c)).unwrap(),
            )
            .cell_order(CellOrder::Hilbert)
            .unwrap()
            .build()
            .unwrap();
            let cell = s.cell_order().unwrap();
            assert_eq!(CellOrder::Hilbert, cell);
        }

        Ok(())
    }

    #[test]
    fn test_attributes() -> TileDBResult<()> {
        let c: Context = Context::new()?;

        {
            let e =
                Builder::new(&c, ArrayType::Dense, sample_domain(&c))?.build();
            assert!(e.is_err());
            assert!(matches!(e.unwrap_err(), Error::LibTileDB(_)));
        }
        {
            let s: Schema = {
                let a1 =
                    AttributeBuilder::new(&c, "a1", Datatype::Int32)?.build();
                Builder::new(&c, ArrayType::Dense, sample_domain(&c))?
                    .add_attribute(a1)?
                    .build()
                    .unwrap()
            };
            assert_eq!(1, s.num_attributes()?);

            let a1 = s.attribute(0)?;
            assert_eq!(Datatype::Int32, a1.datatype()?);
            assert_eq!("a1", a1.name()?);

            let a2 = s.attribute(1);
            assert!(a2.is_err());
        }
        {
            let s: Schema = {
                let a1 =
                    AttributeBuilder::new(&c, "a1", Datatype::Int32)?.build();
                let a2 =
                    AttributeBuilder::new(&c, "a2", Datatype::Float64)?.build();
                Builder::new(&c, ArrayType::Dense, sample_domain(&c))?
                    .add_attribute(a1)?
                    .add_attribute(a2)?
                    .build()
                    .unwrap()
            };
            assert_eq!(2, s.num_attributes()?);

            let a1 = s.attribute(0)?;
            assert_eq!(Datatype::Int32, a1.datatype()?);
            assert_eq!("a1", a1.name()?);

            let a2 = s.attribute(1)?;
            assert_eq!(Datatype::Float64, a2.datatype()?);
            assert_eq!("a2", a2.name()?);

            let a3 = s.attribute(2);
            assert!(a3.is_err());
        }

        Ok(())
    }

    #[test]
    fn test_fields() -> TileDBResult<()> {
        let c: Context = Context::new()?;

        let s: Schema = {
            let a1 = AttributeBuilder::new(&c, "a1", Datatype::Int64)?.build();
            let a2 =
                AttributeBuilder::new(&c, "a2", Datatype::Float64)?.build();
            Builder::new(&c, ArrayType::Dense, sample_domain(&c))?
                .add_attribute(a1)?
                .add_attribute(a2)?
                .build()
                .unwrap()
        };

        // index
        {
            let d = s.field(0)?;
            assert!(matches!(d, Field::Dimension(_)));
            assert_eq!("test", d.name()?);
            assert_eq!(Datatype::Int32, d.datatype()?);

            let a1 = s.field(1)?;
            assert!(matches!(a1, Field::Attribute(_)));
            assert_eq!("a1", a1.name()?);
            assert_eq!(Datatype::Int64, a1.datatype()?);

            let a2 = s.field(2)?;
            assert!(matches!(a2, Field::Attribute(_)));
            assert_eq!("a2", a2.name()?);
            assert_eq!(Datatype::Float64, a2.datatype()?);
        }

        // name
        {
            let d = s.field("test")?;
            assert!(matches!(d, Field::Dimension(_)));
            assert_eq!("test", d.name()?);
            assert_eq!(Datatype::Int32, d.datatype()?);

            let a1 = s.field("a1")?;
            assert!(matches!(a1, Field::Attribute(_)));
            assert_eq!("a1", a1.name()?);
            assert_eq!(Datatype::Int64, a1.datatype()?);

            let a2 = s.field("a2")?;
            assert!(matches!(a2, Field::Attribute(_)));
            assert_eq!("a2", a2.name()?);
            assert_eq!(Datatype::Float64, a2.datatype()?);
        }

        Ok(())
    }

    #[test]
    fn test_filters() -> TileDBResult<()> {
        let c: Context = Context::new()?;

        // default filter lists for the schema - see tiledb/sm/misc/constants.cc
        let coordinates_default = FilterListBuilder::new(&c)?
            .add_filter_data(FilterData::Compression(CompressionData::new(
                CompressionType::Zstd,
            )))?
            .build();
        let offsets_default = FilterListBuilder::new(&c)?
            .add_filter_data(FilterData::Compression(CompressionData::new(
                CompressionType::Zstd,
            )))?
            .build();
        let nullity_default = FilterListBuilder::new(&c)?
            .add_filter_data(FilterData::Compression(CompressionData::new(
                CompressionType::Rle,
            )))?
            .build();

        let target = FilterListBuilder::new(&c)?
            .add_filter_data(FilterData::Compression(CompressionData::new(
                CompressionType::Lz4,
            )))?
            .build();

        // default (empty)
        {
            let s: Schema = {
                let a1 =
                    AttributeBuilder::new(&c, "a1", Datatype::Int32)?.build();
                Builder::new(&c, ArrayType::Dense, sample_domain(&c))?
                    .add_attribute(a1)?
                    .build()
                    .unwrap()
            };

            assert_eq!(coordinates_default, s.coordinate_filters()?);
            assert_eq!(offsets_default, s.offsets_filters()?);
            assert_eq!(nullity_default, s.nullity_filters()?);
        }

        // set coordinates filter
        {
            let s: Schema = {
                let a1 =
                    AttributeBuilder::new(&c, "a1", Datatype::Int32)?.build();
                Builder::new(&c, ArrayType::Dense, sample_domain(&c))?
                    .add_attribute(a1)?
                    .coordinate_filters(&target)?
                    .build()
                    .unwrap()
            };

            assert_eq!(offsets_default, s.offsets_filters()?);
            assert_eq!(nullity_default, s.nullity_filters()?);

            let coordinates = s.coordinate_filters()?;
            assert_eq!(target, coordinates);
        }

        // set offsets filter
        {
            let s: Schema = {
                let a1 =
                    AttributeBuilder::new(&c, "a1", Datatype::Int32)?.build();
                Builder::new(&c, ArrayType::Dense, sample_domain(&c))?
                    .add_attribute(a1)?
                    .offsets_filters(&target)?
                    .build()
                    .unwrap()
            };

            assert_eq!(coordinates_default, s.coordinate_filters()?);
            assert_eq!(nullity_default, s.nullity_filters()?);

            let offsets = s.offsets_filters()?;
            assert_eq!(target, offsets);
        }

        // set nullity filter
        {
            let s: Schema = {
                let a1 =
                    AttributeBuilder::new(&c, "a1", Datatype::Int32)?.build();
                Builder::new(&c, ArrayType::Dense, sample_domain(&c))?
                    .add_attribute(a1)?
                    .nullity_filters(&target)?
                    .build()
                    .unwrap()
            };

            assert_eq!(coordinates_default, s.coordinate_filters()?);
            assert_eq!(offsets_default, s.offsets_filters()?);

            let nullity = s.nullity_filters()?;
            assert_eq!(target, nullity);
        }

        Ok(())
    }

    #[test]
    fn test_eq() {
        let c: Context = Context::new().unwrap();

        let start_schema = |array_type| -> Builder {
            Builder::new(&c, array_type, sample_domain(&c))
                .unwrap()
                .add_attribute(
                    AttributeBuilder::new(&c, "a1", Datatype::Int32)
                        .unwrap()
                        .build(),
                )
                .unwrap()
        };

        let base = start_schema(ArrayType::Sparse).build().unwrap();

        // reflexive
        assert_eq!(base, base);

        // array type change
        {
            let cmp = start_schema(ArrayType::Dense).build().unwrap();
            assert_ne!(base, cmp);
        }

        // no version change test, requires upstream API

        // capacity change
        {
            let cmp = start_schema(base.array_type().unwrap())
                .capacity((base.capacity().unwrap() + 1) * 2)
                .unwrap()
                .build()
                .unwrap();
            assert_ne!(base, cmp);
        }

        // cell order change
        {
            let cmp = start_schema(base.array_type().unwrap())
                .cell_order(
                    if base.cell_order().unwrap() == CellOrder::RowMajor {
                        CellOrder::ColumnMajor
                    } else {
                        CellOrder::RowMajor
                    },
                )
                .unwrap()
                .build()
                .unwrap();
            assert_ne!(base, cmp);
        }

        // tile order change
        {
            let cmp = start_schema(base.array_type().unwrap())
                .tile_order(
                    if base.tile_order().unwrap() == TileOrder::RowMajor {
                        TileOrder::ColumnMajor
                    } else {
                        TileOrder::RowMajor
                    },
                )
                .unwrap()
                .build()
                .unwrap();
            assert_ne!(base, cmp);
        }

        // allow duplicates change
        {
            let cmp = start_schema(base.array_type().unwrap())
                .allow_duplicates(!base.allows_duplicates().unwrap())
                .unwrap()
                .build()
                .unwrap();
            assert_ne!(base, cmp);
        }

        // coords filters
        {
            let cmp = start_schema(base.array_type().unwrap())
                .coordinate_filters(FilterListBuilder::new(&c).unwrap().build())
                .unwrap()
                .build()
                .unwrap();
            assert_ne!(base, cmp);
        }

        // offsets filters
        {
            let cmp = start_schema(base.array_type().unwrap())
                .offsets_filters(FilterListBuilder::new(&c).unwrap().build())
                .unwrap()
                .build()
                .unwrap();
            assert_ne!(base, cmp);
        }

        // nullity filters
        {
            let cmp = start_schema(base.array_type().unwrap())
                .nullity_filters(FilterListBuilder::new(&c).unwrap().build())
                .unwrap()
                .build()
                .unwrap();
            assert_ne!(base, cmp);
        }

        // change attribute
        {
            let cmp =
                Builder::new(&c, base.array_type().unwrap(), sample_domain(&c))
                    .unwrap()
                    .add_attribute(
                        AttributeBuilder::new(&c, "a1", Datatype::Float32)
                            .unwrap()
                            .build(),
                    )
                    .unwrap()
                    .build()
                    .unwrap();
            assert_ne!(base, cmp);
        }

        // add attribute
        {
            let cmp = start_schema(base.array_type().unwrap())
                .add_attribute(
                    AttributeBuilder::new(&c, "a2", Datatype::Int64)
                        .unwrap()
                        .build(),
                )
                .unwrap()
                .build()
                .unwrap();
            assert_ne!(base, cmp);
        }

        // change domain
        {
            let domain = sample_domain_builder(&c)
                .add_dimension(
                    DimensionBuilder::new(
                        &c,
                        "d2",
                        Datatype::Float64,
                        ([-200f64, 200f64], 50f64),
                    )
                    .unwrap()
                    .build(),
                )
                .unwrap()
                .build();
            let cmp = Builder::new(&c, base.array_type().unwrap(), domain)
                .unwrap()
                .add_attribute(
                    AttributeBuilder::new(&c, "a1", Datatype::Int32)
                        .unwrap()
                        .build(),
                )
                .unwrap()
                .build()
                .unwrap();
            assert_ne!(base, cmp);
        }
    }

    /// Test our assumptions about StringAscii dimensions,
    /// if this fails then changes may be needed elsewhere.
    /// Namely we assume that StringAscii is only allowed
    /// in variable-length sparse dimensions.
    #[test]
    fn test_string_dimension() -> TileDBResult<()> {
        let ctx = Context::new().unwrap();

        let build_schema = |array_type: ArrayType| {
            Builder::new(
                &ctx,
                array_type,
                DomainBuilder::new(&ctx)?
                    .add_dimension(
                        DimensionBuilder::new(
                            &ctx,
                            "d",
                            Datatype::StringAscii,
                            DimensionConstraints::StringAscii,
                        )?
                        .build(),
                    )?
                    .build(),
            )?
            .add_attribute(sample_attribute(&ctx))?
            .build()
        };

        // creation should succeed, StringAscii is allowed for sparse CellValNum::Var
        {
            let schema =
                build_schema(ArrayType::Sparse).expect("Error creating schema");
            let cvn = schema
                .domain()
                .and_then(|d| d.dimension(0))
                .and_then(|d| d.cell_val_num())
                .unwrap();
            assert_eq!(CellValNum::Var, cvn);
        }

        // creation should fail, StringAscii is not allowed for dense CellValNum::single()
        {
            let e = build_schema(ArrayType::Dense);
            assert!(matches!(e, Err(Error::LibTileDB(_))));
        }

        Ok(())
    }

    /// Test that the arbitrary schema construction always succeeds
    #[test]
    fn schema_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_schema in any::<SchemaData>())| {
            maybe_schema.create(&ctx)
                .expect("Error constructing arbitrary schema");
        });
    }

    #[test]
    fn schema_eq_reflexivity() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(schema in any::<SchemaData>())| {
            assert_eq!(schema, schema);
            assert_option_subset!(schema, schema);

            let schema = schema.create(&ctx)
                .expect("Error constructing arbitrary schema");
            assert_eq!(schema, schema);
        });
    }

    /// Test what the default values filled in for `None` with schema data are.
    /// Mostly because if we write code which does need the default, we're expecting
    /// to match core and need to be notified if something changes or we did something
    /// wrong.
    #[test]
    fn test_defaults() {
        let ctx = Context::new().unwrap();

        let dense_spec = SchemaData {
            array_type: ArrayType::Dense,
            domain: DomainData {
                dimension: vec![DimensionData {
                    name: "d".to_string(),
                    datatype: Datatype::Int32,
                    constraints: DimensionConstraints::Int32([0, 100], None),
                    filters: None,
                }],
            },
            attributes: vec![AttributeData {
                name: "a".to_string(),
                datatype: Datatype::Int32,
                ..Default::default()
            }],
            ..Default::default()
        };

        let dense_schema = dense_spec
            .create(&ctx)
            .expect("Error creating schema from mostly-default settings");

        assert_eq!(ArrayType::Dense, dense_schema.array_type().unwrap());
        assert_eq!(10000, dense_schema.capacity().unwrap());
        assert_eq!(CellOrder::RowMajor, dense_schema.cell_order().unwrap());
        assert_eq!(TileOrder::RowMajor, dense_schema.tile_order().unwrap());
        assert!(!dense_schema.allows_duplicates().unwrap());

        let sparse_spec = SchemaData {
            array_type: ArrayType::Sparse,
            domain: DomainData {
                dimension: vec![DimensionData {
                    name: "d".to_string(),
                    datatype: Datatype::Int32,
                    constraints: DimensionConstraints::Int32([0, 100], None),
                    filters: None,
                }],
            },
            attributes: vec![AttributeData {
                name: "a".to_string(),
                datatype: Datatype::Int32,
                ..Default::default()
            }],
            ..Default::default()
        };
        let sparse_schema = sparse_spec
            .create(&ctx)
            .expect("Error creating schema from mostly-default settings");

        assert_eq!(ArrayType::Sparse, sparse_schema.array_type().unwrap());
        assert_eq!(
            SchemaData::DEFAULT_SPARSE_TILE_CAPACITY,
            sparse_schema.capacity().unwrap()
        );
        assert_eq!(CellOrder::RowMajor, sparse_schema.cell_order().unwrap());
        assert_eq!(TileOrder::RowMajor, sparse_schema.tile_order().unwrap());
        assert!(!sparse_schema.allows_duplicates().unwrap());
    }

    /// Creates a schema with a single dimension of the given `Datatype` with one attribute.
    /// Used by the test to check if the `Datatype` can be used in this way.
    fn dimension_comprehensive_schema(
        context: &Context,
        array_type: ArrayType,
        datatype: Datatype,
    ) -> TileDBResult<Schema> {
        let dim = physical_type_go!(datatype, DT, {
            if matches!(datatype, Datatype::StringAscii) {
                DimensionBuilder::new(
                    context,
                    "d",
                    datatype,
                    DimensionConstraints::StringAscii,
                )
            } else {
                let domain: [DT; 2] = [0 as DT, 127 as DT];
                let extent: DT = 16 as DT;
                DimensionBuilder::new(context, "d", datatype, (domain, extent))
            }
        })?
        .build();

        let attr = AttributeBuilder::new(context, "a", Datatype::Any)?.build();

        let domain = DomainBuilder::new(context)?.add_dimension(dim)?.build();
        Builder::new(context, array_type, domain)?
            .add_attribute(attr)?
            .build()
    }

    fn do_dense_dimension_comprehensive(datatype: Datatype) {
        let allowed = tiledb_common::datatype::DENSE_DIMENSION_DATATYPES
            .contains(&datatype);
        assert_eq!(allowed, datatype.is_allowed_dimension_type_dense());

        let context = Context::new().unwrap();
        let r = dimension_comprehensive_schema(
            &context,
            ArrayType::Dense,
            datatype,
        );
        assert_eq!(allowed, r.is_ok(), "try_construct => {:?}", r.err());
        if let Err(Error::LibTileDB(s)) = r {
            assert!(
                s.contains("not a valid Dimension Datatype")
                    || s.contains("do not support dimension datatype"),
                "Expected dimension datatype error, received: {}",
                s
            );
        } else {
            assert!(
                r.is_ok(),
                "Found error other than LibTileDB: {}",
                r.err().unwrap()
            );
        }
    }

    fn do_sparse_dimension_comprehensive(datatype: Datatype) {
        let allowed = tiledb_common::datatype::SPARSE_DIMENSION_DATATYPES
            .contains(&datatype);
        assert_eq!(allowed, datatype.is_allowed_dimension_type_sparse());

        let context = Context::new().unwrap();
        let r = dimension_comprehensive_schema(
            &context,
            ArrayType::Sparse,
            datatype,
        );
        assert_eq!(allowed, r.is_ok(), "try_construct => {:?}", r.err());
        if let Err(Error::LibTileDB(s)) = r {
            assert!(
                s.contains("not a valid Dimension Datatype")
                    || s.contains("do not support dimension datatype"),
                "Expected dimension datatype error, received: {}",
                s
            );
        } else {
            assert!(
                r.is_ok(),
                "Found error other than LibTileDB: {}",
                r.err().unwrap()
            );
        }
    }

    proptest! {
        #[test]
        fn dense_dimension_comprehensive(dt in any::<Datatype>()) {
            do_dense_dimension_comprehensive(dt)
        }

        #[test]
        fn sparse_dimension_comprehensive(dt in any::<Datatype>()) {
            do_sparse_dimension_comprehensive(dt)
        }
    }
}
