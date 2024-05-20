use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::ops::Deref;

use serde::{Deserialize, Serialize};
use serde_json::json;
use util::option::OptionSubset;

use crate::array::CellValNum;
use crate::context::{CApiInterface, Context, ContextBound};
use crate::datatype::{LogicalType, PhysicalType};
use crate::error::{DatatypeErrorKind, Error};
use crate::filter::list::{FilterList, FilterListData, RawFilterList};
use crate::{fn_typed, Datatype, Factory, Result as TileDBResult};

pub(crate) enum RawDimension {
    Owned(*mut ffi::tiledb_dimension_t),
}

impl Deref for RawDimension {
    type Target = *mut ffi::tiledb_dimension_t;
    fn deref(&self) -> &Self::Target {
        match *self {
            RawDimension::Owned(ref ffi) => ffi,
        }
    }
}

impl Drop for RawDimension {
    fn drop(&mut self) {
        let RawDimension::Owned(ref mut ffi) = *self;
        unsafe { ffi::tiledb_dimension_free(ffi) }
    }
}

#[derive(ContextBound)]
pub struct Dimension<'ctx> {
    #[context]
    pub(crate) context: &'ctx Context,
    pub(crate) raw: RawDimension,
}

impl<'ctx> Dimension<'ctx> {
    pub(crate) fn capi(&self) -> *mut ffi::tiledb_dimension_t {
        *self.raw
    }

    /// Read from the C API whatever we need to use this dimension from Rust
    pub(crate) fn new(context: &'ctx Context, raw: RawDimension) -> Self {
        Dimension { context, raw }
    }

    pub fn name(&self) -> TileDBResult<String> {
        let mut c_name = std::ptr::null::<std::ffi::c_char>();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_dimension_get_name(ctx, *self.raw, &mut c_name)
        })?;
        let c_name = unsafe { std::ffi::CStr::from_ptr(c_name) };
        Ok(String::from(c_name.to_string_lossy()))
    }

    pub fn datatype(&self) -> TileDBResult<Datatype> {
        let c_dimension = self.capi();
        let mut c_datatype: ffi::tiledb_datatype_t = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_dimension_get_type(ctx, c_dimension, &mut c_datatype)
        })?;

        Datatype::try_from(c_datatype)
    }

    pub fn cell_val_num(&self) -> TileDBResult<CellValNum> {
        let mut c_num: std::ffi::c_uint = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_dimension_get_cell_val_num(ctx, *self.raw, &mut c_num)
        })?;
        CellValNum::try_from(c_num)
    }

    pub fn is_var_sized(&self) -> TileDBResult<bool> {
        Ok(self.cell_val_num()?.is_var_sized())
    }

    pub fn domain<T: PhysicalType>(&self) -> TileDBResult<Option<[T; 2]>> {
        let c_dimension = self.capi();
        let mut c_domain_ptr: *const std::ffi::c_void = out_ptr!();

        self.capi_call(|ctx| unsafe {
            ffi::tiledb_dimension_get_domain(
                ctx,
                c_dimension,
                &mut c_domain_ptr,
            )
        })?;

        if c_domain_ptr.is_null() {
            Ok(None)
        } else {
            let c_domain: &[T; 2] = unsafe { &*c_domain_ptr.cast::<[T; 2]>() };
            Ok(Some(*c_domain))
        }
    }

    /// Returns the tile extent of this dimension.
    pub fn extent<T: PhysicalType>(&self) -> TileDBResult<Option<T>> {
        let c_dimension = self.capi();
        let mut c_extent_ptr: *const ::std::ffi::c_void = out_ptr!();

        self.capi_call(|ctx| unsafe {
            ffi::tiledb_dimension_get_tile_extent(
                ctx,
                c_dimension,
                &mut c_extent_ptr,
            )
        })?;

        if c_extent_ptr.is_null() {
            Ok(None)
        } else {
            Ok(Some(unsafe { *c_extent_ptr.cast::<T>() }))
        }
    }

    pub fn filters(&self) -> TileDBResult<FilterList> {
        let mut c_fl: *mut ffi::tiledb_filter_list_t = out_ptr!();

        let c_dimension = self.capi();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_dimension_get_filter_list(ctx, c_dimension, &mut c_fl)
        })?;

        Ok(FilterList {
            context: self.context,
            raw: RawFilterList::Owned(c_fl),
        })
    }
}

impl<'ctx> Debug for Dimension<'ctx> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let data =
            DimensionData::try_from(self).map_err(|_| std::fmt::Error)?;
        let mut json = json!(data);
        json["raw"] = json!(format!("{:p}", *self.raw));

        write!(f, "{}", json)
    }
}

impl<'c1, 'c2> PartialEq<Dimension<'c2>> for Dimension<'c1> {
    fn eq(&self, other: &Dimension<'c2>) -> bool {
        eq_helper!(self.name(), other.name());
        eq_helper!(self.datatype(), other.datatype());
        eq_helper!(self.cell_val_num(), other.cell_val_num());
        eq_helper!(self.filters(), other.filters());

        fn_typed!(self.datatype().unwrap(), LT, {
            type DT = <LT as LogicalType>::PhysicalType;
            eq_helper!(self.domain::<DT>(), other.domain::<DT>());
            eq_helper!(self.extent::<DT>(), other.extent::<DT>())
        });

        true
    }
}

#[derive(Clone, Debug, Deserialize, OptionSubset, PartialEq, Serialize)]
pub enum DimensionConstraints {
    Int8([i8; 2], Option<i8>),
    Int16([i16; 2], Option<i16>),
    Int32([i32; 2], Option<i32>),
    Int64([i64; 2], Option<i64>),
    UInt8([u8; 2], Option<u8>),
    UInt16([u16; 2], Option<u16>),
    UInt32([u32; 2], Option<u32>),
    UInt64([u64; 2], Option<u64>),
    Float32([f32; 2], Option<f32>),
    Float64([f64; 2], Option<f64>),
    StringAscii,
}

#[macro_export]
macro_rules! dimension_constraints_go {
    ($expr:expr, $DT:ident, $range:pat, $extent:pat, $then:expr, $string:expr) => {{
        dimension_constraints_go!(
            $expr, $DT, $range, $extent, $then, $then, $string
        )
    }};
    ($expr:expr, $DT:ident, $range:pat, $extent:pat, $integral:expr, $float:expr, $string:expr) => {{
        use $crate::array::dimension::DimensionConstraints;
        match $expr {
            #[allow(unused_variables)]
            DimensionConstraints::Int8($range, $extent) => {
                #[allow(dead_code)]
                type $DT = i8;
                $integral
            }
            #[allow(unused_variables)]
            DimensionConstraints::Int16($range, $extent) => {
                #[allow(dead_code)]
                type $DT = i16;
                $integral
            }
            #[allow(unused_variables)]
            DimensionConstraints::Int32($range, $extent) => {
                #[allow(dead_code)]
                type $DT = i32;
                $integral
            }
            #[allow(unused_variables)]
            DimensionConstraints::Int64($range, $extent) => {
                #[allow(dead_code)]
                type $DT = i64;
                $integral
            }
            #[allow(unused_variables)]
            DimensionConstraints::UInt8($range, $extent) => {
                #[allow(dead_code)]
                type $DT = u8;
                $integral
            }
            #[allow(unused_variables)]
            DimensionConstraints::UInt16($range, $extent) => {
                #[allow(dead_code)]
                type $DT = u16;
                $integral
            }
            #[allow(unused_variables)]
            DimensionConstraints::UInt32($range, $extent) => {
                #[allow(dead_code)]
                type $DT = u32;
                $integral
            }
            #[allow(unused_variables)]
            DimensionConstraints::UInt64($range, $extent) => {
                #[allow(dead_code)]
                type $DT = u64;
                $integral
            }
            #[allow(unused_variables)]
            DimensionConstraints::Float32($range, $extent) => {
                #[allow(dead_code)]
                type $DT = f32;
                $float
            }
            #[allow(unused_variables)]
            DimensionConstraints::Float64($range, $extent) => {
                #[allow(dead_code)]
                type $DT = f64;
                $float
            }
            DimensionConstraints::StringAscii => $string,
        }
    }};
}

macro_rules! dimension_constraints_impl {
    ($($V:ident : $U:ty),+) => {
        $(
            impl From<[$U; 2]> for DimensionConstraints {
                fn from(value: [$U; 2]) -> DimensionConstraints {
                    DimensionConstraints::$V(value, None)
                }
            }

            impl From<&[$U; 2]> for DimensionConstraints {
                fn from(value: &[$U; 2]) -> DimensionConstraints {
                    DimensionConstraints::$V([value[0], value[1]], None)
                }
            }

            impl From<([$U; 2], $U)> for DimensionConstraints {
                fn from(value: ([$U; 2], $U)) -> DimensionConstraints {
                    DimensionConstraints::$V([value.0[0], value.0[1]], Some(value.1))
                }
            }

            impl From<(&[$U; 2], $U)> for DimensionConstraints {
                fn from(value: (&[$U; 2], $U)) -> DimensionConstraints {
                    DimensionConstraints::$V([value.0[0], value.0[1]], Some(value.1))
                }
            }

            impl From<([$U; 2], Option<$U>)> for DimensionConstraints {
                fn from(value: ([$U; 2], Option<$U>)) -> DimensionConstraints {
                    DimensionConstraints::$V([value.0[0], value.0[1]], value.1)
                }
            }

            impl From<(&[$U; 2], Option<$U>)> for DimensionConstraints {
                fn from(value: (&[$U; 2], Option<$U>)) -> DimensionConstraints {
                    DimensionConstraints::$V([value.0[0], value.0[1]], value.1)
                }
            }
        )+
    }
}

dimension_constraints_impl!(Int8: i8, Int16: i16, Int32: i32, Int64: i64);
dimension_constraints_impl!(UInt8: u8, UInt16: u16, UInt32: u32, UInt64: u64);
dimension_constraints_impl!(Float32: f32, Float64: f64);

impl DimensionConstraints {
    pub fn verify_type_compatible(
        &self,
        datatype: Datatype,
    ) -> TileDBResult<()> {
        dimension_constraints_go!(
            self,
            DT,
            _range,
            _extent,
            {
                if !datatype.is_compatible_type::<DT>() {
                    return Err(Error::Datatype(
                        DatatypeErrorKind::TypeMismatch {
                            user_type: std::any::type_name::<DT>(),
                            tiledb_type: datatype,
                        },
                    ));
                }
            },
            {
                if !matches!(datatype, Datatype::StringAscii) {
                    return Err(Error::Datatype(
                        DatatypeErrorKind::InvalidDatatype {
                            context: Some(
                                "DimensionConstraints::StringAscii".to_owned(),
                            ),
                            found: datatype,
                            expected: Datatype::StringAscii,
                        },
                    ));
                }
            }
        );

        Ok(())
    }

    pub(crate) fn domain_ptr(&self) -> *const std::ffi::c_void {
        dimension_constraints_go!(
            self,
            DT,
            range,
            _extent,
            range.as_ptr() as *const DT as *const std::ffi::c_void,
            std::ptr::null()
        )
    }

    pub(crate) fn extent_ptr(&self) -> *const std::ffi::c_void {
        dimension_constraints_go!(
            self,
            DT,
            _range,
            extent,
            {
                if let Some(extent) = extent {
                    extent as *const DT as *const std::ffi::c_void
                } else {
                    std::ptr::null()
                }
            },
            std::ptr::null()
        )
    }

    /// Returns the number of elements spanned by this constraint, if applicable
    pub fn len(&self) -> Option<usize> {
        match self {
            Self::Int8([low, high], _) => Some((1 + high - low) as usize),
            Self::Int16([low, high], _) => Some((1 + high - low) as usize),
            Self::Int32([low, high], _) => Some((1 + high - low) as usize),
            Self::Int64([low, high], _) => Some((1 + high - low) as usize),
            Self::UInt8([low, high], _) => Some((1 + high - low) as usize),
            Self::UInt16([low, high], _) => Some((1 + high - low) as usize),
            Self::UInt32([low, high], _) => Some((1 + high - low) as usize),
            Self::UInt64([low, high], _) => Some((1 + high - low) as usize),
            _ => None,
        }
    }
}

#[derive(ContextBound)]
pub struct Builder<'ctx> {
    #[base(ContextBound)]
    dim: Dimension<'ctx>,
}

impl<'ctx> Builder<'ctx> {
    pub fn new<Constraints: Into<DimensionConstraints>>(
        context: &'ctx Context,
        name: &str,
        datatype: Datatype,
        constraints: Constraints,
    ) -> TileDBResult<Self> {
        let constraints = constraints.into();
        constraints.verify_type_compatible(datatype)?;

        let c_datatype = datatype.capi_enum();
        let c_name = cstring!(name);
        let c_domain = constraints.domain_ptr();
        let c_extent = constraints.extent_ptr();

        let mut c_dimension: *mut ffi::tiledb_dimension_t =
            std::ptr::null_mut();

        context.capi_call(|ctx| unsafe {
            ffi::tiledb_dimension_alloc(
                ctx,
                c_name.as_ptr(),
                c_datatype,
                c_domain,
                c_extent,
                &mut c_dimension,
            )
        })?;
        Ok(Builder {
            dim: Dimension {
                context,
                raw: RawDimension::Owned(c_dimension),
            },
        })
    }

    pub fn context(&self) -> &'ctx Context {
        self.dim.context
    }

    pub fn name(&self) -> TileDBResult<String> {
        self.dim.name()
    }

    pub fn cell_val_num(self, num: CellValNum) -> TileDBResult<Self> {
        let c_num = num.capi() as std::ffi::c_uint;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_dimension_set_cell_val_num(ctx, *self.dim.raw, c_num)
        })?;
        Ok(self)
    }

    pub fn filters(self, filters: FilterList) -> TileDBResult<Self> {
        let c_dimension = self.dim.capi();
        let c_fl = filters.capi();

        self.capi_call(|ctx| unsafe {
            ffi::tiledb_dimension_set_filter_list(ctx, c_dimension, c_fl)
        })?;
        Ok(self)
    }

    pub fn build(self) -> Dimension<'ctx> {
        self.dim
    }
}

impl<'ctx> From<Builder<'ctx>> for Dimension<'ctx> {
    fn from(builder: Builder<'ctx>) -> Dimension<'ctx> {
        builder.build()
    }
}

/// Encapsulation of data needed to construct a Dimension
#[derive(Clone, Debug, Deserialize, OptionSubset, PartialEq, Serialize)]
pub struct DimensionData {
    pub name: String,
    pub datatype: Datatype,
    pub constraints: DimensionConstraints,
    pub cell_val_num: Option<CellValNum>,

    /// Optional filters to apply to the dimension. If None or Some(empty),
    /// then filters will be inherited from the schema's `coordinate_filters`
    /// field when the array is constructed.
    pub filters: Option<FilterListData>,
}

impl DimensionData {
    #[cfg(any(test, feature = "proptest-strategies"))]
    pub fn value_strategy(&self) -> crate::query::strategy::FieldValueStrategy {
        use crate::query::strategy::FieldValueStrategy;
        use proptest::prelude::*;

        dimension_constraints_go!(
            self.constraints,
            DT,
            ref domain,
            _,
            FieldValueStrategy::from((domain[0]..=domain[1]).boxed()),
            {
                assert_eq!(self.datatype, Datatype::StringAscii);
                FieldValueStrategy::from(any::<u8>().boxed())
            }
        )
    }
}

impl Display for DimensionData {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}", json!(*self))
    }
}

impl<'ctx> TryFrom<&Dimension<'ctx>> for DimensionData {
    type Error = crate::error::Error;

    fn try_from(dim: &Dimension<'ctx>) -> TileDBResult<Self> {
        let datatype = dim.datatype()?;
        let constraints = fn_typed!(datatype, LT, {
            type DT = <LT as LogicalType>::PhysicalType;
            let domain = dim.domain::<DT>()?;
            let extent = dim.extent::<DT>()?;
            if let Some(domain) = domain {
                DimensionConstraints::from((domain, extent))
            } else {
                assert!(extent.is_none());
                DimensionConstraints::StringAscii
            }
        });

        Ok(DimensionData {
            name: dim.name()?,
            datatype,
            constraints,
            cell_val_num: Some(dim.cell_val_num()?),
            filters: {
                let fl = FilterListData::try_from(&dim.filters()?)?;
                if fl.is_empty() {
                    None
                } else {
                    Some(fl)
                }
            },
        })
    }
}

impl<'ctx> TryFrom<Dimension<'ctx>> for DimensionData {
    type Error = crate::error::Error;

    fn try_from(dim: Dimension<'ctx>) -> TileDBResult<Self> {
        Self::try_from(&dim)
    }
}

impl<'ctx> Factory<'ctx> for DimensionData {
    type Item = Dimension<'ctx>;

    fn create(&self, context: &'ctx Context) -> TileDBResult<Self::Item> {
        let mut b = Builder::new(
            context,
            &self.name,
            self.datatype,
            self.constraints.clone(),
        )?;

        if let Some(fl) = self.filters.as_ref() {
            b = b.filters(fl.create(context)?)?;
        }

        Ok(if let Some(c) = self.cell_val_num {
            b.cell_val_num(c)?
        } else {
            b
        }
        .build())
    }
}

#[cfg(feature = "arrow")]
pub mod arrow;

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;

#[cfg(test)]
mod tests {
    use crate::array::dimension::*;
    use crate::filter::list::Builder as FilterListBuilder;
    use crate::filter::*;

    #[test]
    fn test_dimension_alloc() {
        let context = Context::new().unwrap();

        // normal use case, should succeed, no memory issues
        {
            let name = "test_dimension_alloc";
            let dimension = Builder::new(
                &context,
                name,
                Datatype::Int32,
                ([1i32, 4], 4i32),
            )
            .unwrap()
            .build();

            assert_eq!(name, dimension.name().unwrap());
        }

        // bad domain, should error
        {
            let b = Builder::new(
                &context,
                "test_dimension_alloc",
                Datatype::Int32,
                ([4i32, 1], 4i32),
            );
            assert!(b.is_err());
        }

        // bad extent, should error
        {
            let b = Builder::new(
                &context,
                "test_dimension_alloc",
                Datatype::Int32,
                ([1i32, 4], 0i32),
            );
            assert!(b.is_err());
        }
    }

    #[test]
    fn test_dimension_domain() {
        let context = Context::new().unwrap();

        // normal use case, should succeed, no memory issues
        {
            let domain_in: [i32; 2] = [1, 4];
            let extent_in: i32 = 4;
            let dim = Builder::new(
                &context,
                "test_dimension_domain",
                Datatype::Int32,
                (domain_in, extent_in),
            )
            .unwrap()
            .build();

            assert_eq!(Datatype::Int32, dim.datatype().unwrap());

            let domain_out = dim.domain::<i32>().unwrap().unwrap();
            assert_eq!(domain_in[0], domain_out[0]);
            assert_eq!(domain_in[1], domain_out[1]);

            let extent_out = dim.extent::<i32>().unwrap().unwrap();
            assert_eq!(extent_in, extent_out);
        }
    }

    #[test]
    fn test_dimension_cell_val_num() {
        let context = Context::new().unwrap();

        // only 1 is currently supported
        {
            let cell_val_num = CellValNum::try_from(1).unwrap();
            let dimension = {
                Builder::new(
                    &context,
                    "test_dimension_cell_val_num",
                    Datatype::Int32,
                    ([1i32, 4], 4),
                )
                .unwrap()
                .cell_val_num(cell_val_num)
                .unwrap()
                .build()
            };

            assert_eq!(cell_val_num, dimension.cell_val_num().unwrap());
        }

        // anything else should error
        for cell_val_num in vec![2, 4, 8].into_iter() {
            let b = Builder::new(
                &context,
                "test_dimension_cell_val_num",
                Datatype::Int32,
                ([1i32, 4], 4),
            )
            .unwrap()
            .cell_val_num(CellValNum::try_from(cell_val_num).unwrap());
            assert!(b.is_err());
        }
    }

    #[test]
    fn test_dimension_filter_list() -> TileDBResult<()> {
        let context = Context::new().unwrap();

        // none set
        {
            let dimension: Dimension = Builder::new(
                &context,
                "test_dimension_alloc",
                Datatype::Int32,
                ([1, 4], 4),
            )
            .unwrap()
            .into();

            let fl = dimension.filters().unwrap();
            assert_eq!(0, fl.get_num_filters().unwrap());
        }

        // with some
        {
            let fl = FilterListBuilder::new(&context)?
                .add_filter(Filter::create(
                    &context,
                    FilterData::Compression(CompressionData::new(
                        CompressionType::Lz4,
                    )),
                )?)?
                .build();
            let dimension: Dimension = Builder::new(
                &context,
                "test_dimension_alloc",
                Datatype::Int32,
                ([1, 4], 4),
            )
            .unwrap()
            .filters(fl)
            .unwrap()
            .into();

            let fl = dimension.filters().unwrap();
            assert_eq!(1, fl.get_num_filters().unwrap());

            let outlz4 = fl.get_filter(0).unwrap();
            match outlz4.filter_data().expect("Error reading filter data") {
                FilterData::Compression(CompressionData {
                    kind: CompressionType::Lz4,
                    ..
                }) => (),
                _ => unreachable!(),
            }
        }

        Ok(())
    }

    #[test]
    fn test_eq() {
        let context = Context::new().unwrap();

        let base =
            Builder::new(&context, "d1", Datatype::Int32, ([0, 1000], 100))
                .unwrap()
                .build();
        assert_eq!(base, base);

        // change name
        {
            let cmp =
                Builder::new(&context, "d2", Datatype::Int32, ([0, 1000], 100))
                    .unwrap()
                    .build();
            assert_eq!(cmp, cmp);
            assert_ne!(base, cmp);
        }

        // change type
        {
            let cmp = Builder::new(
                &context,
                "d1",
                Datatype::UInt32,
                ([0u32, 1000], 100u32),
            )
            .unwrap()
            .build();
            assert_eq!(cmp, cmp);
            assert_ne!(base, cmp);
        }

        // change domain
        {
            let cmp =
                Builder::new(&context, "d1", Datatype::Int32, ([1, 1000], 100))
                    .unwrap()
                    .build();
            assert_eq!(cmp, cmp);
            assert_ne!(base, cmp);
        }

        // change extent
        {
            let cmp =
                Builder::new(&context, "d1", Datatype::Int32, ([0, 1000], 99))
                    .unwrap()
                    .build();
            assert_eq!(cmp, cmp);
            assert_ne!(base, cmp);
        }

        // change filters
        {
            let cmp =
                Builder::new(&context, "d1", Datatype::Int32, ([0, 1000], 99))
                    .unwrap()
                    .filters(
                        FilterListBuilder::new(&context)
                            .unwrap()
                            .add_filter_data(FilterData::Compression(
                                CompressionData::new(CompressionType::Lz4),
                            ))
                            .unwrap()
                            .build(),
                    )
                    .unwrap()
                    .build();
            assert_eq!(cmp, cmp);
            assert_ne!(base, cmp);
        }
    }
}
