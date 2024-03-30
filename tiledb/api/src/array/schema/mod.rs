use std::borrow::Borrow;
use std::convert::TryFrom;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::ops::Deref;

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::array::attribute::{AttributeData, RawAttribute};
use crate::array::domain::{DomainData, RawDomain};
use crate::array::{Attribute, Domain, Layout};
use crate::context::{CApiInterface, Context, ContextBound};
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
            _ => Err(Self::Error::LibTileDB(format!(
                "Invalid array type: {}",
                value
            ))),
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

impl<'ctx> ContextBound<'ctx> for Schema<'ctx> {
    fn context(&self) -> &'ctx Context {
        self.context
    }
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
        self.capi_return(unsafe {
            ffi::tiledb_array_schema_get_domain(
                c_context,
                c_schema,
                &mut c_domain,
            )
        })?;

        Ok(Domain::new(self.context, RawDomain::Owned(c_domain)))
    }

    /// Retrieve the schema of an array from storage
    pub fn load(context: &'ctx Context, uri: &str) -> TileDBResult<Self> {
        let c_context: *mut ffi::tiledb_ctx_t = context.capi();
        let c_uri = cstring!(uri);
        let mut c_schema: *mut ffi::tiledb_array_schema_t = out_ptr!();

        context.capi_return(unsafe {
            ffi::tiledb_array_schema_load(
                c_context,
                c_uri.as_ptr(),
                &mut c_schema,
            )
        })?;

        Ok(Schema::new(context, RawSchema::Owned(c_schema)))
    }

    pub fn version(&self) -> TileDBResult<i64> {
        let mut c_version: std::os::raw::c_int = out_ptr!();
        self.capi_return(unsafe {
            ffi::tiledb_array_schema_get_allows_dups(
                self.context.capi(),
                self.capi(),
                &mut c_version,
            )
        })?;

        Ok(c_version as i64)
    }

    pub fn array_type(&self) -> TileDBResult<ArrayType> {
        let c_context = self.context.capi();
        let c_schema = *self.raw;
        let mut c_atype: ffi::tiledb_array_type_t = out_ptr!();
        self.capi_return(unsafe {
            ffi::tiledb_array_schema_get_array_type(
                c_context,
                c_schema,
                &mut c_atype,
            )
        })?;

        ArrayType::try_from(c_atype)
    }

    pub fn capacity(&self) -> TileDBResult<u64> {
        let c_context = self.context.capi();
        let c_schema = *self.raw;
        let mut c_capacity: u64 = out_ptr!();
        self.capi_return(unsafe {
            ffi::tiledb_array_schema_get_capacity(
                c_context,
                c_schema,
                &mut c_capacity,
            )
        })?;

        Ok(c_capacity)
    }

    pub fn cell_order(&self) -> TileDBResult<Layout> {
        let c_context = self.context.capi();
        let c_schema = *self.raw;
        let mut c_cell_order: ffi::tiledb_layout_t = out_ptr!();
        self.capi_return(unsafe {
            ffi::tiledb_array_schema_get_cell_order(
                c_context,
                c_schema,
                &mut c_cell_order,
            )
        })?;

        Layout::try_from(c_cell_order)
    }

    pub fn tile_order(&self) -> TileDBResult<Layout> {
        let c_context = self.context.capi();
        let c_schema = *self.raw;
        let mut c_tile_order: ffi::tiledb_layout_t = out_ptr!();
        self.capi_return(unsafe {
            ffi::tiledb_array_schema_get_tile_order(
                c_context,
                c_schema,
                &mut c_tile_order,
            )
        })?;

        Layout::try_from(c_tile_order)
    }

    pub fn allows_duplicates(&self) -> TileDBResult<bool> {
        let mut c_allows_duplicates: std::os::raw::c_int = out_ptr!();
        self.capi_return(unsafe {
            ffi::tiledb_array_schema_get_allows_dups(
                self.context.capi(),
                self.capi(),
                &mut c_allows_duplicates,
            )
        })?;
        Ok(c_allows_duplicates != 0)
    }

    pub fn nattributes(&self) -> TileDBResult<usize> {
        let c_context = self.context.capi();
        let c_schema = *self.raw;
        let mut c_nattrs: u32 = out_ptr!();
        self.capi_return(unsafe {
            ffi::tiledb_array_schema_get_attribute_num(
                c_context,
                c_schema,
                &mut c_nattrs,
            )
        })?;
        Ok(c_nattrs as usize)
    }

    pub fn attribute(&self, index: usize) -> TileDBResult<Attribute> {
        let c_context = self.context.capi();
        let c_schema = *self.raw;
        let c_index = index as u32;
        let mut c_attr: *mut ffi::tiledb_attribute_t = out_ptr!();
        self.capi_return(unsafe {
            ffi::tiledb_array_schema_get_attribute_from_index(
                c_context,
                c_schema,
                c_index,
                &mut c_attr,
            )
        })?;

        Ok(Attribute::new(self.context, RawAttribute::Owned(c_attr)))
    }

    fn filter_list(
        &self,
        ffi_function: FnFilterListGet,
    ) -> TileDBResult<FilterList> {
        let c_context = self.context.capi();
        let c_schema = *self.raw;
        let mut c_filters: *mut ffi::tiledb_filter_list_t = out_ptr!();

        self.capi_return(unsafe {
            ffi_function(c_context, c_schema, &mut c_filters)
        })?;
        Ok(FilterList {
            context: self.context,
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
        eq_helper!(self.nattributes(), other.nattributes());
        eq_helper!(self.version(), other.version());
        eq_helper!(self.array_type(), other.array_type());
        eq_helper!(self.capacity(), other.capacity());
        eq_helper!(self.cell_order(), other.cell_order());
        eq_helper!(self.tile_order(), other.tile_order());
        eq_helper!(self.allows_duplicates(), other.allows_duplicates());
        eq_helper!(self.coordinate_filters(), other.coordinate_filters());
        eq_helper!(self.offsets_filters(), other.offsets_filters());
        eq_helper!(self.nullity_filters(), other.nullity_filters());

        for a in 0..self.nattributes().unwrap() {
            eq_helper!(self.attribute(a), other.attribute(a));
        }

        eq_helper!(self.domain(), other.domain());

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

impl<'ctx> ContextBound<'ctx> for Builder<'ctx> {
    fn context(&self) -> &'ctx Context {
        self.schema.context
    }
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
        context.capi_return(unsafe {
            ffi::tiledb_array_schema_alloc(
                c_context,
                c_array_type,
                &mut c_schema,
            )
        })?;

        let c_domain = domain.capi();
        context.capi_return(unsafe {
            ffi::tiledb_array_schema_set_domain(c_context, c_schema, c_domain)
        })?;

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
        self.context().capi_return(unsafe {
            ffi::tiledb_array_schema_set_capacity(c_context, c_schema, capacity)
        })?;
        Ok(self)
    }

    pub fn cell_order(self, order: Layout) -> TileDBResult<Self> {
        let c_context = self.schema.context.capi();
        let c_schema = *self.schema.raw;
        let c_order = order.capi_enum();
        self.capi_return(unsafe {
            ffi::tiledb_array_schema_set_cell_order(
                c_context, c_schema, c_order,
            )
        })?;
        Ok(self)
    }

    pub fn tile_order(self, order: Layout) -> TileDBResult<Self> {
        let c_context = self.schema.context.capi();
        let c_schema = *self.schema.raw;
        let c_order = order.capi_enum();
        self.capi_return(unsafe {
            ffi::tiledb_array_schema_set_tile_order(
                c_context, c_schema, c_order,
            )
        })?;
        Ok(self)
    }

    pub fn allow_duplicates(self, allow: bool) -> TileDBResult<Self> {
        let c_allow = if allow { 1 } else { 0 };
        self.capi_return(unsafe {
            ffi::tiledb_array_schema_set_allows_dups(
                self.schema.context.capi(),
                *self.schema.raw,
                c_allow,
            )
        })?;
        Ok(self)
    }

    pub fn add_attribute(self, attr: Attribute) -> TileDBResult<Self> {
        self.capi_return(unsafe {
            ffi::tiledb_array_schema_add_attribute(
                self.schema.context.capi(),
                *self.schema.raw,
                attr.capi(),
            )
        })?;
        Ok(self)
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
        self.capi_return(unsafe {
            ffi_function(c_context, *self.schema.raw, filters.capi())
        })?;
        Ok(self)
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
    pub array_type: ArrayType,
    pub domain: DomainData,
    pub capacity: Option<u64>,
    pub cell_order: Option<Layout>,
    pub tile_order: Option<Layout>,
    pub allow_duplicates: Option<bool>,
    pub attributes: Vec<AttributeData>,
    pub coordinate_filters: FilterListData,
    pub offsets_filters: FilterListData,
    pub nullity_filters: FilterListData,
}

impl<'ctx> TryFrom<&Schema<'ctx>> for SchemaData {
    type Error = crate::error::Error;

    fn try_from(schema: &Schema<'ctx>) -> TileDBResult<Self> {
        Ok(SchemaData {
            array_type: schema.array_type()?,
            domain: DomainData::try_from(&schema.domain()?)?,
            capacity: Some(schema.capacity()?),
            cell_order: Some(schema.cell_order()?),
            tile_order: Some(schema.tile_order()?),
            allow_duplicates: Some(schema.allows_duplicates()?),
            attributes: (0..schema.nattributes()?)
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

#[cfg(feature = "proptest-strategies")]
pub mod strategy;

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
        assert_eq!(0, s.version().unwrap());
    }

    #[test]
    fn test_array_type() -> TileDBResult<()> {
        let c: Context = Context::new()?;

        {
            let s: Schema =
                Builder::new(&c, ArrayType::Dense, sample_domain(&c))
                    .unwrap()
                    .build();
            let t = s.array_type().unwrap();
            assert_eq!(ArrayType::Dense, t);
        }

        {
            let s: Schema =
                Builder::new(&c, ArrayType::Sparse, sample_domain(&c))
                    .unwrap()
                    .build();
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
            let s: Schema =
                Builder::new(&c, ArrayType::Dense, sample_domain(&c))
                    .unwrap()
                    .capacity(cap_in)
                    .unwrap()
                    .build();
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
            let b: Builder =
                Builder::new(&c, ArrayType::Dense, sample_domain(&c))
                    .unwrap()
                    .allow_duplicates(false)
                    .unwrap();

            let s: Schema = b.into();
            assert!(!s.allows_duplicates().unwrap());
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
            assert!(!s.allows_duplicates().unwrap());
        }
        // sparse, duplicates
        {
            let b: Builder =
                Builder::new(&c, ArrayType::Sparse, sample_domain(&c))
                    .unwrap()
                    .allow_duplicates(true)
                    .unwrap();

            let s: Schema = b.into();
            assert!(s.allows_duplicates().unwrap());
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
        assert_eq!(Datatype::Int32, rows.datatype().unwrap());
        // TODO: add method to check min/max

        let cols = domain.dimension(1).expect("Error reading cols dimension");
        assert_eq!(Datatype::Int32, rows.datatype().unwrap());
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
            let tile = s.tile_order().unwrap();
            let cell = s.cell_order().unwrap();
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
            let tile = s.tile_order().unwrap();
            let cell = s.cell_order().unwrap();
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
            let cell = s.cell_order().unwrap();
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
            assert_eq!(0, s.nattributes()?);
        }
        {
            let s: Schema = {
                let a1 =
                    AttributeBuilder::new(&c, "a1", Datatype::Int32)?.build();
                Builder::new(&c, ArrayType::Dense, sample_domain(&c))?
                    .add_attribute(a1)?
                    .build()
            };
            assert_eq!(1, s.nattributes()?);

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
            assert_eq!(2, s.nattributes()?);

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
            let cmp = start_schema(base.array_type().unwrap())
                .capacity((base.capacity().unwrap() + 1) * 2)
                .unwrap()
                .build();
            assert_ne!(base, cmp);
        }

        // cell order change
        {
            let cmp = start_schema(base.array_type().unwrap())
                .cell_order(if base.cell_order().unwrap() == Layout::RowMajor {
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
            let cmp = start_schema(base.array_type().unwrap())
                .tile_order(if base.tile_order().unwrap() == Layout::RowMajor {
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
            let cmp = start_schema(base.array_type().unwrap())
                .allow_duplicates(!base.allows_duplicates().unwrap())
                .unwrap()
                .build();
            assert_ne!(base, cmp);
        }

        // coords filters
        {
            let cmp = start_schema(base.array_type().unwrap())
                .coordinate_filters(
                    &FilterListBuilder::new(&c).unwrap().build(),
                )
                .unwrap()
                .build();
            assert_ne!(base, cmp);
        }

        // offsets filters
        {
            let cmp = start_schema(base.array_type().unwrap())
                .offsets_filters(&FilterListBuilder::new(&c).unwrap().build())
                .unwrap()
                .build();
            assert_ne!(base, cmp);
        }

        // nullity filters
        {
            let cmp = start_schema(base.array_type().unwrap())
                .nullity_filters(&FilterListBuilder::new(&c).unwrap().build())
                .unwrap()
                .build();
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
                    .build();
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
            let cmp = Builder::new(&c, base.array_type().unwrap(), domain)
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
