use std::borrow::Borrow;
use std::convert::TryFrom;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::ops::Deref;

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::array::attribute::{AttributeData, RawAttribute};
use crate::array::domain::{DomainData, RawDomain};
use crate::array::{Attribute, Domain, Layout};
use crate::context::Context;
use crate::filter_list::{FilterList, FilterListData, RawFilterList};
use crate::{Factory, Result as TileDBResult};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

type FnFilterListGet = unsafe extern "C" fn(
    *mut ffi::tiledb_ctx_t,
    *mut ffi::tiledb_array_schema_t,
    *mut *mut ffi::tiledb_filter_list_t,
) -> i32;

pub struct Schema<'ctx> {
    context: &'ctx Context,
    raw: RawSchema,
}

impl<'ctx> Schema<'ctx> {
    pub(crate) fn capi(&self) -> *mut ffi::tiledb_array_schema_t {
        *self.raw
    }

    pub(crate) fn new(context: &'ctx Context, raw: RawSchema) -> Self {
        Schema { context, raw }
    }

    pub fn domain(&self) -> TileDBResult<Domain<'ctx>> {
        let c_context: *mut ffi::tiledb_ctx_t = self.context.capi();
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
        let c_context: *mut ffi::tiledb_ctx_t = context.capi();
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
                self.context.capi(),
                self.capi(),
                &mut c_ret,
            )
        } == ffi::TILEDB_OK
        {
            c_ret as i64
        } else {
            unreachable!("Rust API design should prevent sanity check failure")
        }
    }

    pub fn array_type(&self) -> ArrayType {
        let c_context = self.context.capi();
        let c_schema = *self.raw;
        let mut c_atype: ffi::tiledb_array_type_t = out_ptr!();
        let c_ret = unsafe {
            ffi::tiledb_array_schema_get_array_type(
                c_context,
                c_schema,
                &mut c_atype,
            )
        };
        assert_eq!(ffi::TILEDB_OK, c_ret); // Rust API should prevent sanity check error
        ArrayType::try_from(c_atype).expect("Invalid response from C API")
    }

    pub fn capacity(&self) -> u64 {
        let c_context = self.context.capi();
        let c_schema = *self.raw;
        let mut c_capacity: u64 = out_ptr!();
        let c_ret = unsafe {
            ffi::tiledb_array_schema_get_capacity(
                c_context,
                c_schema,
                &mut c_capacity,
            )
        };
        assert_eq!(ffi::TILEDB_OK, c_ret); // Rust API should prevent sanity check error
        c_capacity
    }

    pub fn cell_order(&self) -> Layout {
        let c_context = self.context.capi();
        let c_schema = *self.raw;
        let mut c_cell_order: ffi::tiledb_layout_t = out_ptr!();
        let c_ret = unsafe {
            ffi::tiledb_array_schema_get_cell_order(
                c_context,
                c_schema,
                &mut c_cell_order,
            )
        };
        assert_eq!(ffi::TILEDB_OK, c_ret); // Rust API should prevent sanity check error
        Layout::try_from(c_cell_order).expect("Invalid response from C API")
    }

    pub fn tile_order(&self) -> Layout {
        let c_context = self.context.capi();
        let c_schema = *self.raw;
        let mut c_tile_order: ffi::tiledb_layout_t = out_ptr!();
        let c_ret = unsafe {
            ffi::tiledb_array_schema_get_tile_order(
                c_context,
                c_schema,
                &mut c_tile_order,
            )
        };
        assert_eq!(ffi::TILEDB_OK, c_ret); // Rust API should prevent sanity check error
        Layout::try_from(c_tile_order).expect("Invalid response from C API")
    }

    pub fn allows_duplicates(&self) -> bool {
        let mut c_ret: std::os::raw::c_int = out_ptr!();
        if unsafe {
            ffi::tiledb_array_schema_get_allows_dups(
                self.context.capi(),
                self.capi(),
                &mut c_ret,
            )
        } == ffi::TILEDB_OK
        {
            c_ret != 0
        } else {
            unreachable!("Rust API design should prevent sanity check failure")
        }
    }

    pub fn nattributes(&self) -> usize {
        let c_context = self.context.capi();
        let c_schema = *self.raw;
        let mut c_nattrs: u32 = out_ptr!();
        let c_ret = unsafe {
            ffi::tiledb_array_schema_get_attribute_num(
                c_context,
                c_schema,
                &mut c_nattrs,
            )
        };
        assert_eq!(ffi::TILEDB_OK, c_ret); // Rust API should prevent sanity check error
        c_nattrs as usize
    }

    pub fn attribute(&self, index: usize) -> TileDBResult<Attribute> {
        let c_context = self.context.capi();
        let c_schema = *self.raw;
        let c_index = index as u32;
        let mut c_attr: *mut ffi::tiledb_attribute_t = out_ptr!();
        let c_ret = unsafe {
            ffi::tiledb_array_schema_get_attribute_from_index(
                c_context,
                c_schema,
                c_index,
                &mut c_attr,
            )
        };
        if c_ret == ffi::TILEDB_OK {
            Ok(Attribute::new(self.context, RawAttribute::Owned(c_attr)))
        } else {
            Err(self.context.expect_last_error())
        }
    }

    fn filter_list(
        &self,
        ffi_function: FnFilterListGet,
    ) -> TileDBResult<FilterList> {
        let c_context = self.context.capi();
        let c_schema = *self.raw;
        let mut c_filters: *mut ffi::tiledb_filter_list_t = out_ptr!();

        let c_ret =
            unsafe { ffi_function(c_context, c_schema, &mut c_filters) };
        if c_ret == ffi::TILEDB_OK {
            Ok(FilterList {
                context: self.context,
                raw: RawFilterList::Owned(c_filters),
            })
        } else {
            Err(self.context.expect_last_error())
        }
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

impl<'ctx> Debug for Schema<'ctx> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let data = SchemaData::try_from(self).map_err(|_| std::fmt::Error)?;
        let mut json = json!(data);
        json["version"] = json!(self.version());
        json["raw"] = json!(format!("{:p}", *self.raw));

        write!(f, "{}", json)
    }
}

impl<'c1, 'c2> PartialEq<Schema<'c2>> for Schema<'c1> {
    fn eq(&self, other: &Schema<'c2>) -> bool {
        let nattr_matches = self.nattributes() == other.nattributes();
        if !nattr_matches {
            return false;
        }

        let version_matches = self.version() == other.version();
        if !version_matches {
            return false;
        }

        let array_type_matches = self.array_type() == other.array_type();
        if !array_type_matches {
            return false;
        }

        let capacity_matches = self.capacity() == other.capacity();
        if !capacity_matches {
            return false;
        }

        let cell_order_matches = self.cell_order() == other.cell_order();
        if !cell_order_matches {
            return false;
        }

        let tile_order_matches = self.tile_order() == other.tile_order();
        if !tile_order_matches {
            return false;
        }

        let duplicates_matches =
            self.allows_duplicates() == other.allows_duplicates();
        if !duplicates_matches {
            return false;
        }

        let coords_filters_matches =
            match (self.coordinate_filters(), other.coordinate_filters()) {
                (Ok(mine), Ok(theirs)) => mine == theirs,
                _ => false,
            };
        if !coords_filters_matches {
            return false;
        }

        let offsets_filters_matches =
            match (self.offsets_filters(), other.offsets_filters()) {
                (Ok(mine), Ok(theirs)) => mine == theirs,
                _ => false,
            };
        if !offsets_filters_matches {
            return false;
        }

        let nullity_filters_matches =
            match (self.nullity_filters(), other.nullity_filters()) {
                (Ok(mine), Ok(theirs)) => mine == theirs,
                _ => false,
            };
        if !nullity_filters_matches {
            return false;
        }

        for a in 0..self.nattributes() {
            let attr_matches = match (self.attribute(a), other.attribute(a)) {
                (Ok(mine), Ok(theirs)) => mine == theirs,
                _ => false,
            };
            if !attr_matches {
                return false;
            }
        }

        let domain_matches = match (self.domain(), other.domain()) {
            (Ok(mine), Ok(theirs)) => mine == theirs,
            _ => false,
        };
        if !domain_matches {
            return false;
        }

        true
    }
}

type FnFilterListSet = unsafe extern "C" fn(
    *mut ffi::tiledb_ctx_t,
    *mut ffi::tiledb_array_schema_t,
    *mut ffi::tiledb_filter_list_t,
) -> i32;

pub struct Builder<'ctx> {
    schema: Schema<'ctx>,
}

impl<'ctx> Builder<'ctx> {
    pub fn new(
        context: &'ctx Context,
        array_type: ArrayType,
        domain: Domain<'ctx>,
    ) -> TileDBResult<Self> {
        let c_context = context.capi();
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

    pub fn capacity(self, capacity: u64) -> TileDBResult<Self> {
        let c_context = self.schema.context.capi();
        let c_schema = *self.schema.raw;
        let c_ret = unsafe {
            ffi::tiledb_array_schema_set_capacity(c_context, c_schema, capacity)
        };
        if c_ret == ffi::TILEDB_OK {
            Ok(self)
        } else {
            Err(self.schema.context.expect_last_error())
        }
    }

    pub fn cell_order(self, order: Layout) -> TileDBResult<Self> {
        let c_context = self.schema.context.capi();
        let c_schema = *self.schema.raw;
        let c_order = order.capi_enum();
        let c_ret = unsafe {
            ffi::tiledb_array_schema_set_cell_order(
                c_context, c_schema, c_order,
            )
        };
        if c_ret == ffi::TILEDB_OK {
            Ok(self)
        } else {
            Err(self.schema.context.expect_last_error())
        }
    }

    pub fn tile_order(self, order: Layout) -> TileDBResult<Self> {
        let c_context = self.schema.context.capi();
        let c_schema = *self.schema.raw;
        let c_order = order.capi_enum();
        let c_ret = unsafe {
            ffi::tiledb_array_schema_set_tile_order(
                c_context, c_schema, c_order,
            )
        };
        if c_ret == ffi::TILEDB_OK {
            Ok(self)
        } else {
            Err(self.schema.context.expect_last_error())
        }
    }

    pub fn allow_duplicates(self, allow: bool) -> TileDBResult<Self> {
        let c_allow = if allow { 1 } else { 0 };
        if unsafe {
            ffi::tiledb_array_schema_set_allows_dups(
                self.schema.context.capi(),
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
                self.schema.context.capi(),
                *self.schema.raw,
                attr.capi(),
            )
        } == ffi::TILEDB_OK
        {
            Ok(self)
        } else {
            Err(self.schema.context.expect_last_error())
        }
    }

    fn filter_list<FL>(
        self,
        filters: FL,
        ffi_function: FnFilterListSet,
    ) -> TileDBResult<Self>
    where
        FL: Borrow<FilterList<'ctx>>,
    {
        let filters = filters.borrow();
        let c_context = self.schema.context.capi();
        let c_ret = unsafe {
            ffi_function(c_context, *self.schema.raw, filters.capi())
        };
        if c_ret == ffi::TILEDB_OK {
            Ok(self)
        } else {
            Err(self.schema.context.expect_last_error())
        }
    }

    pub fn coordinate_filters<FL>(self, filters: FL) -> TileDBResult<Self>
    where
        FL: Borrow<FilterList<'ctx>>,
    {
        self.filter_list(
            filters,
            ffi::tiledb_array_schema_set_coords_filter_list,
        )
    }

    pub fn offsets_filters<FL>(self, filters: FL) -> TileDBResult<Self>
    where
        FL: Borrow<FilterList<'ctx>>,
    {
        self.filter_list(
            filters,
            ffi::tiledb_array_schema_set_offsets_filter_list,
        )
    }

    pub fn nullity_filters<FL>(self, filters: FL) -> TileDBResult<Self>
    where
        FL: Borrow<FilterList<'ctx>>,
    {
        self.filter_list(
            filters,
            ffi::tiledb_array_schema_set_validity_filter_list,
        )
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

/// Encapsulation of data needed to construct a Schema
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SchemaData {
    array_type: ArrayType,
    domain: DomainData,
    capacity: Option<u64>,
    cell_order: Option<Layout>,
    tile_order: Option<Layout>,
    allow_duplicates: Option<bool>,
    attributes: Vec<AttributeData>,
    coordinate_filters: FilterListData,
    offsets_filters: FilterListData,
    nullity_filters: FilterListData,
}

impl<'ctx> TryFrom<&Schema<'ctx>> for SchemaData {
    type Error = crate::error::Error;

    fn try_from(schema: &Schema<'ctx>) -> TileDBResult<Self> {
        Ok(SchemaData {
            array_type: schema.array_type(),
            domain: DomainData::try_from(&schema.domain()?)?,
            capacity: Some(schema.capacity()),
            cell_order: Some(schema.cell_order()),
            tile_order: Some(schema.tile_order()),
            allow_duplicates: Some(schema.allows_duplicates()),
            attributes: (0..schema.nattributes())
                .map(|a| AttributeData::try_from(&schema.attribute(a)?))
                .collect::<TileDBResult<Vec<AttributeData>>>()?,
            coordinate_filters: FilterListData::try_from(
                &schema.coordinate_filters()?,
            )?,
            offsets_filters: FilterListData::try_from(
                &schema.coordinate_filters()?,
            )?,
            nullity_filters: FilterListData::try_from(
                &schema.coordinate_filters()?,
            )?,
        })
    }
}

impl<'ctx> Factory<'ctx> for SchemaData {
    type Item = Schema<'ctx>;

    fn create(&self, context: &'ctx Context) -> TileDBResult<Self::Item> {
        let mut b = self.attributes.iter().try_fold(
            Builder::new(
                context,
                self.array_type,
                self.domain.create(context)?,
            )?
            .coordinate_filters(self.coordinate_filters.create(context)?)?
            .offsets_filters(self.offsets_filters.create(context)?)?
            .nullity_filters(self.nullity_filters.create(context)?)?,
            |b, a| b.add_attribute(a.create(context)?),
        )?;
        if let Some(c) = self.capacity {
            b = b.capacity(c)?;
        }
        if let Some(d) = self.allow_duplicates {
            b = b.allow_duplicates(d)?;
        }
        if let Some(o) = self.cell_order {
            b = b.cell_order(o)?;
        }
        if let Some(o) = self.tile_order {
            b = b.tile_order(o)?;
        }

        Ok(b.build())
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use tempdir::TempDir;

    use crate::array::schema::*;
    use crate::array::tests::*;
    use crate::array::{AttributeBuilder, DimensionBuilder, DomainBuilder};
    use crate::context::Context;
    use crate::filter::*;
    use crate::Datatype;

    fn sample_domain_builder(c: &Context) -> DomainBuilder {
        let dim = DimensionBuilder::new::<i32>(
            c,
            "test",
            Datatype::Int32,
            &[-100, 100],
            &100,
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

        let b: Builder = Builder::new(&c, ArrayType::Dense, sample_domain(&c))
            .unwrap()
            .allow_duplicates(false)
            .unwrap();

        let s: Schema = b.into();
        assert_eq!(0, s.version());
    }

    #[test]
    fn test_array_type() -> TileDBResult<()> {
        let c: Context = Context::new()?;

        {
            let s: Schema =
                Builder::new(&c, ArrayType::Dense, sample_domain(&c))
                    .unwrap()
                    .build();
            let t = s.array_type();
            assert_eq!(ArrayType::Dense, t);
        }

        {
            let s: Schema =
                Builder::new(&c, ArrayType::Sparse, sample_domain(&c))
                    .unwrap()
                    .build();
            let t = s.array_type();
            assert_eq!(ArrayType::Sparse, t);
        }

        Ok(())
    }

    #[test]
    fn test_capacity() -> TileDBResult<()> {
        let c: Context = Context::new()?;

        {
            let cap_in = 100;
            let s: Schema =
                Builder::new(&c, ArrayType::Dense, sample_domain(&c))
                    .unwrap()
                    .capacity(cap_in)
                    .unwrap()
                    .build();
            let cap_out = s.capacity();
            assert_eq!(cap_in, cap_out);
        }
        Ok(())
    }

    #[test]
    fn test_allow_duplicates() {
        let c: Context = Context::new().unwrap();

        // dense, no duplicates
        {
            let b: Builder =
                Builder::new(&c, ArrayType::Dense, sample_domain(&c))
                    .unwrap()
                    .allow_duplicates(false)
                    .unwrap();

            let s: Schema = b.into();
            assert!(!s.allows_duplicates());
        }
        // dense, duplicates (should error)
        {
            let e = Builder::new(&c, ArrayType::Dense, sample_domain(&c))
                .unwrap()
                .allow_duplicates(true);
            assert!(e.is_err());
        }
        // sparse, no duplicates
        {
            let b: Builder =
                Builder::new(&c, ArrayType::Sparse, sample_domain(&c))
                    .unwrap()
                    .allow_duplicates(false)
                    .unwrap();

            let s: Schema = b.into();
            assert!(!s.allows_duplicates());
        }
        // sparse, duplicates
        {
            let b: Builder =
                Builder::new(&c, ArrayType::Sparse, sample_domain(&c))
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

    #[test]
    fn test_layout() -> TileDBResult<()> {
        let c: Context = Context::new()?;

        {
            let s: Schema =
                Builder::new(&c, ArrayType::Dense, sample_domain(&c))
                    .unwrap()
                    .tile_order(Layout::RowMajor)
                    .unwrap()
                    .cell_order(Layout::RowMajor)
                    .unwrap()
                    .build();
            let tile = s.tile_order();
            let cell = s.cell_order();
            assert_eq!(Layout::RowMajor, tile);
            assert_eq!(Layout::RowMajor, cell);
        }
        {
            let s: Schema =
                Builder::new(&c, ArrayType::Dense, sample_domain(&c))
                    .unwrap()
                    .tile_order(Layout::ColumnMajor)
                    .unwrap()
                    .cell_order(Layout::ColumnMajor)
                    .unwrap()
                    .build();
            let tile = s.tile_order();
            let cell = s.cell_order();
            assert_eq!(Layout::ColumnMajor, tile);
            assert_eq!(Layout::ColumnMajor, cell);
        }
        {
            let r = Builder::new(&c, ArrayType::Dense, sample_domain(&c))
                .unwrap()
                .tile_order(Layout::Hilbert);
            assert!(r.is_err());
        }
        {
            let r = Builder::new(&c, ArrayType::Sparse, sample_domain(&c))
                .unwrap()
                .tile_order(Layout::Hilbert);
            assert!(r.is_err());
        }
        {
            let r = Builder::new(&c, ArrayType::Dense, sample_domain(&c))
                .unwrap()
                .cell_order(Layout::Hilbert);
            assert!(r.is_err());
        }
        {
            let s: Schema =
                Builder::new(&c, ArrayType::Sparse, sample_domain(&c))
                    .unwrap()
                    .cell_order(Layout::Hilbert)
                    .unwrap()
                    .build();
            let cell = s.cell_order();
            assert_eq!(Layout::Hilbert, cell);
        }

        Ok(())
    }

    #[test]
    fn test_attributes() -> TileDBResult<()> {
        let c: Context = Context::new()?;

        {
            let s: Schema =
                Builder::new(&c, ArrayType::Dense, sample_domain(&c))?.build();
            assert_eq!(0, s.nattributes());
        }
        {
            let s: Schema = {
                let a1 =
                    AttributeBuilder::new(&c, "a1", Datatype::Int32)?.build();
                Builder::new(&c, ArrayType::Dense, sample_domain(&c))?
                    .add_attribute(a1)?
                    .build()
            };
            assert_eq!(1, s.nattributes());

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
            };
            assert_eq!(2, s.nattributes());

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

        let base = start_schema(ArrayType::Sparse).build();

        // reflexive
        assert_eq!(base, base);

        // array type change
        {
            let cmp = start_schema(ArrayType::Dense).build();
            assert_ne!(base, cmp);
        }

        // no version change test, requires upstream API

        // capacity change
        {
            let cmp = start_schema(base.array_type())
                .capacity((base.capacity() + 1) * 2)
                .unwrap()
                .build();
            assert_ne!(base, cmp);
        }

        // cell order change
        {
            let cmp = start_schema(base.array_type())
                .cell_order(if base.cell_order() == Layout::RowMajor {
                    Layout::ColumnMajor
                } else {
                    Layout::RowMajor
                })
                .unwrap()
                .build();
            assert_ne!(base, cmp);
        }

        // tile order change
        {
            let cmp = start_schema(base.array_type())
                .tile_order(if base.tile_order() == Layout::RowMajor {
                    Layout::ColumnMajor
                } else {
                    Layout::RowMajor
                })
                .unwrap()
                .build();
            assert_ne!(base, cmp);
        }

        // allow duplicates change
        {
            let cmp = start_schema(base.array_type())
                .allow_duplicates(!base.allows_duplicates())
                .unwrap()
                .build();
            assert_ne!(base, cmp);
        }

        // coords filters
        {
            let cmp = start_schema(base.array_type())
                .coordinate_filters(
                    &FilterListBuilder::new(&c).unwrap().build(),
                )
                .unwrap()
                .build();
            assert_ne!(base, cmp);
        }

        // offsets filters
        {
            let cmp = start_schema(base.array_type())
                .offsets_filters(&FilterListBuilder::new(&c).unwrap().build())
                .unwrap()
                .build();
            assert_ne!(base, cmp);
        }

        // nullity filters
        {
            let cmp = start_schema(base.array_type())
                .nullity_filters(&FilterListBuilder::new(&c).unwrap().build())
                .unwrap()
                .build();
            assert_ne!(base, cmp);
        }

        // change attribute
        {
            let cmp = Builder::new(&c, base.array_type(), sample_domain(&c))
                .unwrap()
                .add_attribute(
                    AttributeBuilder::new(&c, "a1", Datatype::Float32)
                        .unwrap()
                        .build(),
                )
                .unwrap()
                .build();
            assert_ne!(base, cmp);
        }

        // add attribute
        {
            let cmp = start_schema(base.array_type())
                .add_attribute(
                    AttributeBuilder::new(&c, "a2", Datatype::Int64)
                        .unwrap()
                        .build(),
                )
                .unwrap()
                .build();
            assert_ne!(base, cmp);
        }

        // change domain
        {
            let domain = sample_domain_builder(&c)
                .add_dimension(
                    DimensionBuilder::new::<f64>(
                        &c,
                        "d2",
                        Datatype::Float64,
                        &[-200f64, 200f64],
                        &50f64,
                    )
                    .unwrap()
                    .build(),
                )
                .unwrap()
                .build();
            let cmp = Builder::new(&c, base.array_type(), domain)
                .unwrap()
                .add_attribute(
                    AttributeBuilder::new(&c, "a1", Datatype::Int32)
                        .unwrap()
                        .build(),
                )
                .unwrap()
                .build();
            assert_ne!(base, cmp);
        }
    }
}
