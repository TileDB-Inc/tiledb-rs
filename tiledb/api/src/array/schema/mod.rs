use std::borrow::Borrow;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::num::{NonZeroU32, NonZeroUsize};
use std::ops::Deref;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use serde_json::json;
use util::option::OptionSubset;

use crate::array::attribute::{AttributeData, RawAttribute};
use crate::array::dimension::{Dimension, DimensionData};
use crate::array::domain::{DomainData, RawDomain};
use crate::array::{Attribute, CellOrder, Domain, TileOrder};
use crate::context::{CApiInterface, Context, ContextBound};
use crate::error::Error;
use crate::filter::list::{FilterList, FilterListData, RawFilterList};
use crate::key::LookupKey;
use crate::query::read::output::FieldScratchAllocator;
use crate::Datatype;
use crate::{Factory, Result as TileDBResult};

#[derive(
    Clone, Copy, Debug, Deserialize, Eq, OptionSubset, PartialEq, Serialize,
)]
#[cfg_attr(
    any(test, feature = "proptest-strategies"),
    derive(proptest_derive::Arbitrary)
)]
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

/// Represents the number of values carried within a single cell of an attribute or dimension.
#[derive(
    Copy, Clone, Debug, Deserialize, Eq, OptionSubset, PartialEq, Serialize,
)]
pub enum CellValNum {
    /// The number of values per cell is a specific fixed number.
    Fixed(std::num::NonZeroU32),
    /// The number of values per cell varies.
    /// When this option is used for a dimension or attribute, queries must allocate additional
    /// space to hold structural information about each cell. The values will be concatenated
    /// together in a single buffer, and the structural data buffer contains the offset
    /// of each record into the values buffer.
    Var,
}

impl CellValNum {
    pub(crate) fn capi(&self) -> u32 {
        match self {
            CellValNum::Fixed(c) => c.get(),
            CellValNum::Var => u32::MAX,
        }
    }

    pub fn single() -> Self {
        CellValNum::Fixed(NonZeroU32::new(1).unwrap())
    }

    pub fn is_var_sized(&self) -> bool {
        matches!(self, CellValNum::Var)
    }

    /// Return the fixed number of values per cell, if not variable.
    pub fn fixed(&self) -> Option<NonZeroU32> {
        if let CellValNum::Fixed(nz) = self {
            Some(*nz)
        } else {
            None
        }
    }
}

impl Default for CellValNum {
    fn default() -> Self {
        Self::single()
    }
}

impl Display for CellValNum {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        <Self as Debug>::fmt(self, f)
    }
}

impl PartialEq<u32> for CellValNum {
    fn eq(&self, other: &u32) -> bool {
        match self {
            CellValNum::Fixed(val) => val.get() == *other,
            CellValNum::Var => *other == u32::MAX,
        }
    }
}

impl TryFrom<u32> for CellValNum {
    type Error = crate::error::Error;
    fn try_from(value: u32) -> TileDBResult<Self> {
        match value {
            0 => Err(Error::InvalidArgument(anyhow!(
                "Cell val num cannot be zero"
            ))),
            u32::MAX => Ok(CellValNum::Var),
            v => Ok(CellValNum::Fixed(NonZeroU32::new(v).unwrap())),
        }
    }
}

impl From<CellValNum> for u32 {
    fn from(value: CellValNum) -> Self {
        match value {
            CellValNum::Fixed(nz) => nz.get(),
            CellValNum::Var => u32::MAX,
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

/// Holds a field of the schema, which may be either a dimension or an attribute.
#[derive(ContextBound)]
pub enum Field<'ctx> {
    Dimension(Dimension<'ctx>),
    Attribute(Attribute<'ctx>),
}

impl<'ctx> Field<'ctx> {
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
    ) -> TileDBResult<crate::query::read::output::FieldScratchAllocator> {
        Ok(FieldData::try_from(self)?.query_scratch_allocator())
    }
}

#[derive(Clone, Debug, Deserialize, OptionSubset, Serialize, PartialEq)]
pub struct FieldData {
    pub name: String,
    pub datatype: Datatype,
    pub nullability: Option<bool>,
    pub cell_val_num: Option<CellValNum>,
}

impl FieldData {
    pub fn query_scratch_allocator(
        &self,
    ) -> crate::query::read::output::FieldScratchAllocator {
        /*
         * TODO: a hint from the schema would be good to use in some way,
         * this number is super made up and should be improved
         * (especially if there is a large fixed cell val num).
         * The user can use a custom allocator if they want, of course,
         * but they probably aren't going to, so we ought to come up
         * with something good by default.
         */
        let record_capacity = 1024 * 1024;

        FieldScratchAllocator {
            cell_val_num: self.cell_val_num.unwrap_or_default(),
            record_capacity: NonZeroUsize::new(record_capacity).unwrap(),
            is_nullable: self.nullability.unwrap_or(true),
        }
    }
}

impl Display for FieldData {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}", json!(*self))
    }
}

impl From<&AttributeData> for FieldData {
    fn from(attr: &AttributeData) -> Self {
        FieldData {
            name: attr.name.clone(),
            cell_val_num: attr.cell_val_num,
            datatype: attr.datatype,
            nullability: attr.nullability,
        }
    }
}

impl From<&DimensionData> for FieldData {
    fn from(dim: &DimensionData) -> Self {
        FieldData {
            name: dim.name.clone(),
            cell_val_num: dim.cell_val_num,
            datatype: dim.datatype,
            nullability: Some(false),
        }
    }
}

impl<'ctx> TryFrom<&Field<'ctx>> for FieldData {
    type Error = crate::error::Error;

    fn try_from(field: &Field<'ctx>) -> TileDBResult<Self> {
        Ok(FieldData {
            name: field.name()?,
            cell_val_num: Some(field.cell_val_num()?),
            datatype: field.datatype()?,
            nullability: Some(field.nullability()?),
        })
    }
}

impl<'ctx> TryFrom<Field<'ctx>> for FieldData {
    type Error = crate::error::Error;

    fn try_from(field: Field<'ctx>) -> TileDBResult<Self> {
        Self::try_from(&field)
    }
}

type FnFilterListGet = unsafe extern "C" fn(
    *mut ffi::tiledb_ctx_t,
    *mut ffi::tiledb_array_schema_t,
    *mut *mut ffi::tiledb_filter_list_t,
) -> i32;

#[derive(ContextBound)]
pub struct Schema<'ctx> {
    #[context]
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
        let c_schema = *self.raw;
        let mut c_domain: *mut ffi::tiledb_domain_t = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_get_domain(ctx, c_schema, &mut c_domain)
        })?;

        Ok(Domain::new(self.context, RawDomain::Owned(c_domain)))
    }

    /// Retrieve the schema of an array from storage
    pub fn load<S>(context: &'ctx Context, uri: S) -> TileDBResult<Self>
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

    pub fn version(&self) -> TileDBResult<i64> {
        let c_schema = self.capi();
        let mut c_version: std::os::raw::c_int = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_get_allows_dups(
                ctx,
                c_schema,
                &mut c_version,
            )
        })?;

        Ok(c_version as i64)
    }

    pub fn array_type(&self) -> TileDBResult<ArrayType> {
        let c_schema = self.capi();
        let mut c_atype: ffi::tiledb_array_type_t = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_get_array_type(ctx, c_schema, &mut c_atype)
        })?;

        ArrayType::try_from(c_atype)
    }

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

        CellOrder::try_from(c_cell_order)
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

        TileOrder::try_from(c_tile_order)
    }

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

    pub fn nattributes(&self) -> TileDBResult<usize> {
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
    ) -> TileDBResult<Attribute<'ctx>> {
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

        Ok(Attribute::new(self.context, RawAttribute::Owned(c_attr)))
    }

    /// Returns a reference to a field (dimension or attribute) in this schema.
    /// If the key is an index, then values `[0.. ndimensions]` will look
    /// up a dimension, and values outside that range will be adjusted by `ndimensions`
    /// to look up an attribute.
    pub fn field<K: Into<LookupKey>>(
        &self,
        key: K,
    ) -> TileDBResult<Field<'ctx>> {
        let domain = self.domain()?;
        match key.into() {
            LookupKey::Index(idx) => {
                let ndim = domain.ndim()?;
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

#[derive(ContextBound)]
pub struct Builder<'ctx> {
    #[base(ContextBound)]
    schema: Schema<'ctx>,
}

impl<'ctx> Builder<'ctx> {
    pub fn new(
        context: &'ctx Context,
        array_type: ArrayType,
        domain: Domain<'ctx>,
    ) -> TileDBResult<Self> {
        let c_array_type = array_type.capi_enum();
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
                context,
                raw: RawSchema::Owned(c_schema),
            },
        })
    }

    pub fn capacity(self, capacity: u64) -> TileDBResult<Self> {
        let c_schema = *self.schema.raw;
        self.context().capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_set_capacity(ctx, c_schema, capacity)
        })?;
        Ok(self)
    }

    pub fn cell_order(self, order: CellOrder) -> TileDBResult<Self> {
        let c_schema = *self.schema.raw;
        let c_order = order.capi_enum();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_set_cell_order(ctx, c_schema, c_order)
        })?;
        Ok(self)
    }

    pub fn tile_order(self, order: TileOrder) -> TileDBResult<Self> {
        let c_schema = *self.schema.raw;
        let c_order = order.capi_enum();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_set_tile_order(ctx, c_schema, c_order)
        })?;
        Ok(self)
    }

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

    fn filter_list<FL>(
        self,
        filters: FL,
        ffi_function: FnFilterListSet,
    ) -> TileDBResult<Self>
    where
        FL: Borrow<FilterList<'ctx>>,
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

    pub fn build(self) -> TileDBResult<Schema<'ctx>> {
        let c_schema = *self.schema.raw;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_array_schema_check(ctx, c_schema)
        })
        .map(|_| self.schema)
    }
}

impl<'ctx> TryFrom<Builder<'ctx>> for Schema<'ctx> {
    type Error = crate::error::Error;

    fn try_from(builder: Builder<'ctx>) -> TileDBResult<Schema<'ctx>> {
        builder.build()
    }
}

/// Encapsulation of data needed to construct a Schema
#[derive(Clone, Debug, Deserialize, OptionSubset, PartialEq, Serialize)]
pub struct SchemaData {
    pub array_type: ArrayType,
    pub domain: DomainData,
    pub capacity: Option<u64>,
    pub cell_order: Option<CellOrder>,
    pub tile_order: Option<TileOrder>,
    pub allow_duplicates: Option<bool>,
    pub attributes: Vec<AttributeData>,
    pub coordinate_filters: FilterListData,
    pub offsets_filters: FilterListData,
    pub nullity_filters: FilterListData,
}

impl SchemaData {
    pub fn field(&self, idx: usize) -> FieldData {
        if idx < self.domain.dimension.len() {
            FieldData::from(&self.domain.dimension[idx])
        } else {
            FieldData::from(&self.attributes[idx - self.domain.dimension.len()])
        }
    }
}

impl Display for SchemaData {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}", json!(*self))
    }
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
                &schema.offsets_filters()?,
            )?,
            nullity_filters: FilterListData::try_from(
                &schema.nullity_filters()?,
            )?,
        })
    }
}

impl<'ctx> TryFrom<Schema<'ctx>> for SchemaData {
    type Error = crate::error::Error;

    fn try_from(schema: Schema<'ctx>) -> TileDBResult<Self> {
        Self::try_from(&schema)
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

        b.build()
    }
}

#[cfg(feature = "arrow")]
pub mod arrow;

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;

#[cfg(test)]
mod tests {
    use std::io;
    use tempfile::TempDir;

    use super::*;
    use crate::array::tests::create_quickstart_dense;
    use crate::array::{AttributeBuilder, DimensionBuilder, DomainBuilder};
    use crate::filter::{
        CompressionData, CompressionType, FilterData, FilterListBuilder,
    };

    fn sample_attribute(c: &Context) -> Attribute {
        AttributeBuilder::new(c, "a1", Datatype::Int32)
            .unwrap()
            .build()
    }

    // helper function since schemata must have at least one attribute to be valid
    fn with_attribute<'ctx>(
        c: &'ctx Context,
        b: Builder<'ctx>,
    ) -> Builder<'ctx> {
        b.add_attribute(sample_attribute(c)).unwrap()
    }

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

        let b: Builder = with_attribute(
            &c,
            Builder::new(&c, ArrayType::Dense, sample_domain(&c)).unwrap(),
        )
        .allow_duplicates(false)
        .unwrap();

        let s: Schema = b.build().unwrap();
        assert_eq!(0, s.version().unwrap());
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
    fn test_load() -> io::Result<()> {
        let tmp_dir = TempDir::new()?;

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
                    .unwrap()
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
                .coordinate_filters(
                    &FilterListBuilder::new(&c).unwrap().build(),
                )
                .unwrap()
                .build()
                .unwrap();
            assert_ne!(base, cmp);
        }

        // offsets filters
        {
            let cmp = start_schema(base.array_type().unwrap())
                .offsets_filters(&FilterListBuilder::new(&c).unwrap().build())
                .unwrap()
                .build()
                .unwrap();
            assert_ne!(base, cmp);
        }

        // nullity filters
        {
            let cmp = start_schema(base.array_type().unwrap())
                .nullity_filters(&FilterListBuilder::new(&c).unwrap().build())
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
                .build()
                .unwrap();
            assert_ne!(base, cmp);
        }
    }
}
