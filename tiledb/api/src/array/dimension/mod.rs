use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::ops::Deref;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use serde_json::json;
use util::option::OptionSubset;

use crate::array::CellValNum;
use crate::context::{CApiInterface, Context, ContextBound};
use crate::convert::CAPIConverter;
use crate::error::Error;
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

pub struct Dimension {
    pub(crate) context: Context,
    pub(crate) raw: RawDimension,
}

impl ContextBound for Dimension {
    fn context(&self) -> &Context {
        &self.context
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

    pub fn domain<Conv: CAPIConverter>(&self) -> TileDBResult<[Conv; 2]> {
        let c_dimension = self.capi();
        let mut c_domain_ptr: *const std::ffi::c_void = out_ptr!();

        self.capi_call(|ctx| unsafe {
            ffi::tiledb_dimension_get_domain(
                ctx,
                c_dimension,
                &mut c_domain_ptr,
            )
        })?;

        let c_domain: &[Conv::CAPIType; 2] =
            unsafe { &*c_domain_ptr.cast::<[Conv::CAPIType; 2]>() };

        Ok([Conv::to_rust(&c_domain[0]), Conv::to_rust(&c_domain[1])])
    }

    /// Returns the tile extent of this dimension.
    pub fn extent<Conv: CAPIConverter>(&self) -> TileDBResult<Conv> {
        let c_dimension = self.capi();
        let mut c_extent_ptr: *const ::std::ffi::c_void = out_ptr!();

        self.capi_call(|ctx| unsafe {
            ffi::tiledb_dimension_get_tile_extent(
                ctx,
                c_dimension,
                &mut c_extent_ptr,
            )
        })?;
        let c_extent = unsafe { &*c_extent_ptr.cast::<Conv::CAPIType>() };
        Ok(Conv::to_rust(c_extent))
    }

    pub fn filters(&self) -> TileDBResult<FilterList> {
        let mut c_fl: *mut ffi::tiledb_filter_list_t = out_ptr!();

        let c_dimension = self.capi();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_dimension_get_filter_list(ctx, c_dimension, &mut c_fl)
        })?;

        Ok(FilterList::new(&self.context, RawFilterList::Owned(c_fl)))
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

        fn_typed!(self.datatype().unwrap(), DT, {
            eq_helper!(self.domain::<DT>(), other.domain::<DT>())
        });

        fn_typed!(self.datatype().unwrap(), DT, {
            eq_helper!(self.extent::<DT>(), other.extent::<DT>())
        });

        true
    }
}

pub struct Builder {
    dim: Dimension,
}

impl ContextBound for Builder {
    fn context(&self) -> &Context {
        self.dim.context()
    }
}

impl Builder {
    // TODO: extent might be optional?
    // and it
    pub fn new<Conv: CAPIConverter>(
        context: &Context,
        name: &str,
        datatype: Datatype,
        domain: &[Conv; 2],
        extent: &Conv,
    ) -> TileDBResult<Self> {
        let c_datatype = datatype.capi_enum();

        let c_name = cstring!(name);
        let c_domain: [Conv::CAPIType; 2] =
            [domain[0].to_capi(), domain[1].to_capi()];
        let c_extent: Conv::CAPIType = extent.to_capi();

        let mut c_dimension: *mut ffi::tiledb_dimension_t =
            std::ptr::null_mut();

        context.capi_call(|ctx| unsafe {
            ffi::tiledb_dimension_alloc(
                ctx,
                c_name.as_ptr(),
                c_datatype,
                &c_domain[0] as *const <Conv>::CAPIType
                    as *const std::ffi::c_void,
                &c_extent as *const <Conv>::CAPIType as *const std::ffi::c_void,
                &mut c_dimension,
            )
        })?;
        Ok(Builder {
            dim: Dimension::new(context, RawDimension::Owned(c_dimension)),
        })
    }

    pub fn new_string(
        context: &Context,
        name: &str,
        datatype: Datatype,
    ) -> TileDBResult<Self> {
        let c_datatype = datatype.capi_enum();
        let c_name = cstring!(name);
        let mut c_dimension: *mut ffi::tiledb_dimension_t =
            std::ptr::null_mut();

        context.capi_call(|ctx| unsafe {
            ffi::tiledb_dimension_alloc(
                ctx,
                c_name.as_ptr(),
                c_datatype,
                std::ptr::null(),
                std::ptr::null(),
                &mut c_dimension,
            )
        })?;
        Ok(Builder {
            dim: Dimension::new(context, RawDimension::Owned(c_dimension)),
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
#[derive(
    Clone, Default, Debug, Deserialize, OptionSubset, PartialEq, Serialize,
)]
pub struct DimensionData {
    pub name: String,
    pub datatype: Datatype,
    pub domain: Option<[serde_json::value::Value; 2]>,
    pub extent: Option<serde_json::value::Value>,
    pub cell_val_num: Option<CellValNum>,

    /// Optional filters to apply to the dimension. If None or Some(empty),
    /// then filters will be inherited from the schema's `coordinate_filters`
    /// field when the array is constructed.
    pub filters: Option<FilterListData>,
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
        let (domain, extent) = fn_typed!(datatype, DT, {
            let domain = dim.domain::<DT>()?;
            (
                [json!(domain[0]), json!(domain[1])],
                json!(dim.extent::<DT>()?),
            )
        });
        Ok(DimensionData {
            name: dim.name()?,
            datatype,
            domain: Some(domain),
            extent: Some(extent),
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

impl TryFrom<Dimension> for DimensionData {
    type Error = crate::error::Error;

    fn try_from(dim: Dimension) -> TileDBResult<Self> {
        Self::try_from(&dim)
    }
}

impl Factory for DimensionData {
    type Item = Dimension;

    fn create(&self, context: &Context) -> TileDBResult<Self::Item> {
        let mut b = if self.datatype == Datatype::StringAscii {
            Builder::new_string(context, &self.name, self.datatype)?
        } else {
            if self.domain.is_none() {
                return Err(Error::InvalidArgument(anyhow!(
                    "Dimension '{}' is missing its required domain.",
                    self.name
                )));
            }
            if self.extent.is_none() {
                return Err(Error::InvalidArgument(anyhow!(
                    "Dimension '{}' is missing an extent.",
                    self.name
                )));
            }
            let domain = self.domain.as_ref().unwrap();
            let extent = self.extent.as_ref().unwrap();
            fn_typed!(self.datatype, DT, {
                let d0 = serde_json::from_value::<DT>(domain[0].clone())
                    .map_err(|e| {
                        Error::Deserialization(
                            format!("dimension '{}' lower bound", self.name),
                            anyhow!(e),
                        )
                    })?;
                let d1 = serde_json::from_value::<DT>(domain[1].clone())
                    .map_err(|e| {
                        Error::Deserialization(
                            format!("dimension '{}' upper bound", self.name),
                            anyhow!(e),
                        )
                    })?;
                let extent = serde_json::from_value::<DT>(extent.clone())
                    .map_err(|e| {
                        Error::Deserialization(
                            format!("dimension '{}' extent", self.name),
                            anyhow!(e),
                        )
                    })?;
                Builder::new::<DT>(
                    context,
                    &self.name,
                    self.datatype,
                    &[d0, d1],
                    &extent,
                )
            })?
        };
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
            let domain: [i32; 2] = [1, 4];
            let extent: i32 = 4;
            let dimension = Builder::new::<i32>(
                &context,
                name,
                Datatype::Int32,
                &domain,
                &extent,
            )
            .unwrap()
            .build();

            assert_eq!(name, dimension.name().unwrap());
        }

        // bad domain, should error
        {
            let domain: [i32; 2] = [4, 1];
            let extent: i32 = 4;
            let b = Builder::new::<i32>(
                &context,
                "test_dimension_alloc",
                Datatype::Int32,
                &domain,
                &extent,
            );
            assert!(b.is_err());
        }

        // bad extent, should error
        {
            let domain: [i32; 2] = [1, 4];
            let extent: i32 = 0;
            let b = Builder::new::<i32>(
                &context,
                "test_dimension_alloc",
                Datatype::Int32,
                &domain,
                &extent,
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
            let dim = Builder::new::<i32>(
                &context,
                "test_dimension_domain",
                Datatype::Int32,
                &domain_in,
                &extent_in,
            )
            .unwrap()
            .build();

            assert_eq!(Datatype::Int32, dim.datatype().unwrap());

            let domain_out = dim.domain::<i32>().unwrap();
            assert_eq!(domain_in[0], domain_out[0]);
            assert_eq!(domain_in[1], domain_out[1]);

            let extent_out = dim.extent::<i32>().unwrap();
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
                let domain_in: [i32; 2] = [1, 4];
                let extent: i32 = 4;
                Builder::new::<i32>(
                    &context,
                    "test_dimension_cell_val_num",
                    Datatype::Int32,
                    &domain_in,
                    &extent,
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
            let domain_in: [i32; 2] = [1, 4];
            let extent: i32 = 4;
            let b = Builder::new::<i32>(
                &context,
                "test_dimension_cell_val_num",
                Datatype::Int32,
                &domain_in,
                &extent,
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
            let domain: [i32; 2] = [1, 4];
            let extent: i32 = 4;
            let dimension: Dimension = Builder::new::<i32>(
                &context,
                "test_dimension_alloc",
                Datatype::Int32,
                &domain,
                &extent,
            )
            .unwrap()
            .into();

            let fl = dimension.filters().unwrap();
            assert_eq!(0, fl.get_num_filters().unwrap());
        }

        // with some
        {
            let domain: [i32; 2] = [1, 4];
            let extent: i32 = 4;
            let fl = FilterListBuilder::new(&context)?
                .add_filter(Filter::create(
                    &context,
                    &FilterData::Compression(CompressionData::new(
                        CompressionType::Lz4,
                    )),
                )?)?
                .build();
            let dimension: Dimension = Builder::new::<i32>(
                &context,
                "test_dimension_alloc",
                Datatype::Int32,
                &domain,
                &extent,
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

        let base = Builder::new::<i32>(
            &context,
            "d1",
            Datatype::Int32,
            &[0, 1000],
            &100,
        )
        .unwrap()
        .build();
        assert_eq!(base, base);

        // change name
        {
            let cmp = Builder::new::<i32>(
                &context,
                "d2",
                Datatype::Int32,
                &[0, 1000],
                &100,
            )
            .unwrap()
            .build();
            assert_eq!(cmp, cmp);
            assert_ne!(base, cmp);
        }

        // change type
        {
            let cmp = Builder::new::<i32>(
                &context,
                "d1",
                Datatype::UInt32,
                &[0, 1000],
                &100,
            )
            .unwrap()
            .build();
            assert_eq!(cmp, cmp);
            assert_ne!(base, cmp);
        }

        // change domain
        {
            let cmp = Builder::new::<i32>(
                &context,
                "d1",
                Datatype::Int32,
                &[1, 1000],
                &100,
            )
            .unwrap()
            .build();
            assert_eq!(cmp, cmp);
            assert_ne!(base, cmp);
        }

        // change extent
        {
            let cmp = Builder::new::<i32>(
                &context,
                "d1",
                Datatype::Int32,
                &[0, 1000],
                &99,
            )
            .unwrap()
            .build();
            assert_eq!(cmp, cmp);
            assert_ne!(base, cmp);
        }

        // change filters
        {
            let cmp = Builder::new::<i32>(
                &context,
                "d1",
                Datatype::Int32,
                &[0, 1000],
                &99,
            )
            .unwrap()
            .filters(
                FilterListBuilder::new(&context)
                    .unwrap()
                    .add_filter_data(&FilterData::Compression(
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
