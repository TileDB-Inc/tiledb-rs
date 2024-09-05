use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::ops::Deref;

use serde::{Deserialize, Serialize};
use serde_json::json;
use util::option::OptionSubset;

use crate::array::CellValNum;
use crate::context::{CApiInterface, Context, ContextBound};
use crate::datatype::PhysicalType;
use crate::error::{DatatypeErrorKind, Error};
use crate::filter::list::{FilterList, FilterListData, RawFilterList};
use crate::range::SingleValueRange;
use crate::{physical_type_go, Datatype, Factory, Result as TileDBResult};

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

pub struct Dimension {
    pub(crate) context: Context,
    pub(crate) raw: RawDimension,
}

impl ContextBound for Dimension {
    fn context(&self) -> Context {
        self.context.clone()
    }
}

impl Dimension {
    pub(crate) fn capi(&self) -> *mut ffi::tiledb_dimension_t {
        *self.raw
    }

    /// Read from the C API whatever we need to use this dimension from Rust
    pub(crate) fn new(context: &Context, raw: RawDimension) -> Self {
        Dimension {
            context: context.clone(),
            raw,
        }
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
            context: self.context.clone(),
            raw: RawFilterList::Owned(c_fl),
        })
    }
}

impl Debug for Dimension {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let data =
            DimensionData::try_from(self).map_err(|_| std::fmt::Error)?;
        let mut json = json!(data);
        json["raw"] = json!(format!("{:p}", *self.raw));

        write!(f, "{}", json)
    }
}

impl PartialEq<Dimension> for Dimension {
    fn eq(&self, other: &Dimension) -> bool {
        eq_helper!(self.name(), other.name());
        eq_helper!(self.datatype(), other.datatype());
        eq_helper!(self.cell_val_num(), other.cell_val_num());
        eq_helper!(self.filters(), other.filters());

        physical_type_go!(self.datatype().unwrap(), DT, {
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
    pub fn cell_val_num(&self) -> CellValNum {
        match self {
            DimensionConstraints::StringAscii => CellValNum::Var,
            _ => CellValNum::single(),
        }
    }

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
                            user_type: std::any::type_name::<DT>().to_owned(),
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

    /// Returns the number of cells spanned by this constraint, if applicable
    pub fn num_cells(&self) -> Option<u128> {
        let (low, high) = crate::dimension_constraints_go!(
            self,
            _DT,
            [low, high],
            _,
            (i128::from(*low), i128::from(*high)),
            return None,
            return None
        );

        Some(1 + (high - low) as u128)
    }

    /// Returns the number of cells spanned by a
    /// single tile under this constraint, if applicable
    pub fn num_cells_per_tile(&self) -> Option<usize> {
        crate::dimension_constraints_go!(
            self,
            _DT,
            _,
            extent,
            extent.map(|extent| {
                #[allow(clippy::unnecessary_fallible_conversions)]
                // this `unwrap` should be safe, validation will confirm nonzero
                usize::try_from(extent).unwrap()
            }),
            None,
            None
        )
    }

    /// Returns the domain of the dimension constraint, if present, as a range.
    pub fn domain(&self) -> Option<SingleValueRange> {
        crate::dimension_constraints_go!(
            self,
            _DT,
            [low, high],
            _,
            Some(SingleValueRange::from(&[*low, *high])),
            None
        )
    }
}

pub struct Builder {
    dim: Dimension,
}

impl ContextBound for Builder {
    fn context(&self) -> Context {
        self.dim.context()
    }
}

impl Builder {
    pub fn new<Constraints: Into<DimensionConstraints>>(
        context: &Context,
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
                context: context.clone(),
                raw: RawDimension::Owned(c_dimension),
            },
        })
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

    pub fn build(self) -> Dimension {
        self.dim
    }
}

impl From<Builder> for Dimension {
    fn from(builder: Builder) -> Dimension {
        builder.build()
    }
}

/// Encapsulation of data needed to construct a Dimension
#[derive(Clone, Debug, Deserialize, OptionSubset, PartialEq, Serialize)]
pub struct DimensionData {
    pub name: String,
    pub datatype: Datatype,
    pub constraints: DimensionConstraints,

    /// Optional filters to apply to the dimension. If None or Some(empty),
    /// then filters will be inherited from the schema's `coordinate_filters`
    /// field when the array is constructed.
    pub filters: Option<FilterListData>,
}

impl DimensionData {
    pub fn cell_val_num(&self) -> CellValNum {
        self.constraints.cell_val_num()
    }
}

#[cfg(any(test, feature = "proptest-strategies"))]
impl DimensionData {
    /// Returns a strategy for generating values of this dimension's type
    /// which fall within the domain of this dimension.
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

    /// Returns a strategy for generating subarray ranges which fall within
    /// the domain of this dimension.
    ///
    /// `cell_bound` is an optional restriction on the number of possible values
    /// which the strategy is allowed to return.
    ///
    /// If `cell_bound` is `None`, then this function always returns `Some`.
    pub fn subarray_strategy(
        &self,
        cell_bound: Option<usize>,
    ) -> Option<proptest::strategy::BoxedStrategy<crate::range::Range>> {
        use proptest::prelude::Just;
        use proptest::strategy::Strategy;

        use crate::range::{Range, VarValueRange};

        dimension_constraints_go!(
            self.constraints,
            DT,
            ref domain,
            _,
            {
                let cell_bound = cell_bound
                    .map(|bound| DT::try_from(bound).unwrap_or(DT::MAX))
                    .unwrap_or(DT::MAX);

                let domain_lower = domain[0];
                let domain_upper = domain[1];
                let strat =
                    (domain_lower..=domain_upper).prop_flat_map(move |lb| {
                        let ub = std::cmp::min(
                            domain_upper,
                            lb.checked_add(cell_bound).unwrap_or(DT::MAX),
                        );
                        (Just(lb), lb..=ub).prop_map(|(min, max)| {
                            Range::Single(SingleValueRange::from(&[min, max]))
                        })
                    });
                Some(strat.boxed())
            },
            {
                if cell_bound.is_some() {
                    /*
                     * This can be implemented, but there's some ambiguity about
                     * what it should mean when precision goes out the window,
                     * so wait until there's a use case to decide.
                     */
                    return None;
                }

                let domain_lower = domain[0];
                let domain_upper = domain[1];
                let strat =
                    (domain_lower..=domain_upper).prop_flat_map(move |lb| {
                        (Just(lb), (lb..=domain_upper)).prop_map(
                            |(min, max)| {
                                Range::Single(SingleValueRange::from(&[
                                    min, max,
                                ]))
                            },
                        )
                    });
                Some(strat.boxed())
            },
            {
                // DimensionConstraints::StringAscii
                let strat_bound =
                    proptest::string::string_regex("[ -~]*").unwrap().boxed();

                if cell_bound.is_some() {
                    /*
                     * This is not tractible unless there is a bound on the string length.
                     * There isn't one since `StringAscii` is only allowed as a dimension
                     * type in sparse arrays.
                     */
                    return None;
                }

                let strat = (strat_bound.clone(), strat_bound).prop_map(
                    |(ascii1, ascii2)| {
                        let (lb, ub) = if ascii1 < ascii2 {
                            (ascii1, ascii2)
                        } else {
                            (ascii2, ascii1)
                        };
                        Range::Var(VarValueRange::from((lb, ub)))
                    },
                );
                Some(strat.boxed())
            }
        )
    }
}

impl Display for DimensionData {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}", json!(*self))
    }
}

impl TryFrom<&Dimension> for DimensionData {
    type Error = crate::error::Error;

    fn try_from(dim: &Dimension) -> TileDBResult<Self> {
        let datatype = dim.datatype()?;
        let constraints = physical_type_go!(datatype, DT, {
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

impl TryFrom<Dimension> for DimensionData {
    type Error = crate::error::Error;

    fn try_from(dim: Dimension) -> TileDBResult<Self> {
        Self::try_from(&dim)
    }
}

impl Factory for DimensionData {
    type Item = Dimension;

    fn create(&self, context: &Context) -> TileDBResult<Self::Item> {
        let mut b = Builder::new(
            context,
            &self.name,
            self.datatype,
            self.constraints.clone(),
        )?;

        if let Some(fl) = self.filters.as_ref() {
            b = b.filters(fl.create(context)?)?;
        }

        Ok(b.cell_val_num(self.cell_val_num())?.build())
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

    #[test]
    fn subarray_strategy_dense() {
        use super::strategy::Requirements;
        use crate::array::ArrayType;
        use crate::range::{Range, SingleValueRange};
        use proptest::prelude::*;
        use proptest::strategy::Strategy;
        use std::rc::Rc;

        let req = Requirements {
            array_type: Some(ArrayType::Dense),
            ..Default::default()
        };
        let strat = (
            any_with::<DimensionData>(req),
            prop_oneof![Just(None), any::<usize>().prop_map(Some)],
        )
            .prop_flat_map(|(d, cell_bound)| {
                let subarray_strat = d
                    .subarray_strategy(cell_bound)
                    .expect("Dense dimension must have a subarray strategy");
                (Just(Rc::new(d)), Just(cell_bound), subarray_strat)
            });

        proptest!(|((d, cell_bound, s) in strat)| {
            if let Some(bound) = cell_bound {
                assert!(s.num_cells().unwrap() <= bound as u128);
            }
            if let Some(num_cells) = d.constraints.num_cells() {
                assert!(s.num_cells().unwrap() <= num_cells);
            }
            let Range::Single(s) = s else {
                unreachable!("Unexpected range for dense dimension: {:?}", s)
            };
            match s {
                SingleValueRange::Int8(start, end) => {
                    let DimensionConstraints::Int8([lb, ub], _) = d.constraints else { unreachable!() };
                    assert!(lb <= start);
                    assert!(end <= ub);
                    assert_eq!(Some((end - start + 1) as u128), s.num_cells());
                }
                SingleValueRange::Int16(start, end) => {
                    let DimensionConstraints::Int16([lb, ub], _) = d.constraints else { unreachable!() };
                    assert!(lb <= start);
                    assert!(end <= ub);
                    assert_eq!(Some((end - start + 1) as u128), s.num_cells());
                }
                SingleValueRange::Int32(start, end) => {
                    let DimensionConstraints::Int32([lb, ub], _) = d.constraints else { unreachable!() };
                    assert!(lb <= start);
                    assert!(end <= ub);
                    assert_eq!(Some((end - start + 1) as u128), s.num_cells());
                }
                SingleValueRange::Int64(start, end) => {
                    let DimensionConstraints::Int64([lb, ub], _) = d.constraints else { unreachable!() };
                    assert!(lb <= start);
                    assert!(end <= ub);
                    assert_eq!(Some((end - start + 1) as u128), s.num_cells());
                }
                SingleValueRange::UInt8(start, end) => {
                    let DimensionConstraints::UInt8([lb, ub], _) = d.constraints else { unreachable!() };
                    assert!(lb <= start);
                    assert!(end <= ub);
                    assert_eq!(Some((end - start + 1) as u128), s.num_cells());
                }
                SingleValueRange::UInt16(start, end) => {
                    let DimensionConstraints::UInt16([lb, ub], _) = d.constraints else { unreachable!() };
                    assert!(lb <= start);
                    assert!(end <= ub);
                    assert_eq!(Some((end - start + 1) as u128), s.num_cells());
                }
                SingleValueRange::UInt32(start, end) => {
                    let DimensionConstraints::UInt32([lb, ub], _) = d.constraints else { unreachable!() };
                    assert!(lb <= start);
                    assert!(end <= ub);
                    assert_eq!(Some((end - start + 1) as u128), s.num_cells());
                }
                SingleValueRange::UInt64(start, end) => {
                    let DimensionConstraints::UInt64([lb, ub], _) = d.constraints else { unreachable!() };
                    assert!(lb <= start);
                    assert!(end <= ub);
                    assert_eq!(Some((end - start + 1) as u128), s.num_cells());
                },
                s => unreachable!("Unexpected range type for dense dimension: {:?}", s)
            }
        });
    }
}
