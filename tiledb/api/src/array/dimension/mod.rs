use std::ops::Deref;

#[cfg(any(test, feature = "pod"))]
use std::fmt::{Debug, Formatter, Result as FmtResult};

use crate::array::CellValNum;
use crate::context::{CApiInterface, Context, ContextBound};
use crate::datatype::PhysicalType;
use crate::filter::list::{FilterList, RawFilterList};
use crate::{Datatype, Result as TileDBResult, physical_type_go};

pub use tiledb_common::array::dimension::DimensionConstraints;
pub use tiledb_common::dimension_constraints_go;

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
    pub fn capi(&self) -> *mut ffi::tiledb_dimension_t {
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

        Ok(Datatype::try_from(c_datatype)?)
    }

    pub fn cell_val_num(&self) -> TileDBResult<CellValNum> {
        let mut c_num: std::ffi::c_uint = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_dimension_get_cell_val_num(ctx, *self.raw, &mut c_num)
        })?;
        Ok(CellValNum::try_from(c_num)?)
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

#[cfg(any(test, feature = "pod"))]
impl Debug for Dimension {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match tiledb_pod::array::dimension::DimensionData::try_from(self) {
            Ok(d) => Debug::fmt(&d, f),
            Err(e) => {
                let RawDimension::Owned(ptr) = self.raw;
                write!(f, "<Dimension @ {ptr:?}: serialization error: {e}>")
            }
        }
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

        let c_datatype = ffi::tiledb_datatype_t::from(datatype);
        let c_name = cstring!(name);
        let c_domain = dimension_constraints_go!(
            constraints,
            DT,
            ref range,
            ref _extent,
            range.as_ptr() as *const DT as *const std::ffi::c_void,
            std::ptr::null()
        );
        let c_extent = dimension_constraints_go!(
            constraints,
            DT,
            ref _range,
            ref extent,
            {
                if let Some(extent) = extent {
                    extent as *const DT as *const std::ffi::c_void
                } else {
                    std::ptr::null()
                }
            },
            std::ptr::null()
        );

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
        let c_num = std::ffi::c_uint::from(num);
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

#[cfg(feature = "arrow")]
pub mod arrow;

#[cfg(any(test, feature = "pod"))]
pub mod pod;

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use tiledb_pod::array::dimension::DimensionData;
    use utils::assert_option_subset;

    use super::*;
    use crate::Factory;
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

    /// Test that the arbitrary dimension construction always succeeds
    #[test]
    fn test_prop_dimension() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_dimension in any::<DimensionData>())| {
            maybe_dimension.create(&ctx)
                .expect("Error constructing arbitrary dimension");
        });
    }

    #[test]
    fn dimension_eq_reflexivity() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(dimension in any::<DimensionData>())| {
            assert_eq!(dimension, dimension);
            assert_option_subset!(dimension, dimension);

            let dimension = dimension
                .create(&ctx).expect("Error constructing arbitrary attribute");
            assert_eq!(dimension, dimension);
        });
    }
}
