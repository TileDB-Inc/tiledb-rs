use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::ops::Deref;

use serde_json::json;

use crate::context::Context;
use crate::convert::CAPIConverter;
use crate::filter_list::{FilterList, RawFilterList};
use crate::fn_typed;
use crate::Datatype;
use crate::Result as TileDBResult;

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

pub struct Dimension<'ctx> {
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
        let c_context = self.context.capi();
        let mut c_name = std::ptr::null::<std::ffi::c_char>();
        let res = unsafe {
            ffi::tiledb_dimension_get_name(c_context, *self.raw, &mut c_name)
        };
        if res == ffi::TILEDB_OK {
            let c_name = unsafe { std::ffi::CStr::from_ptr(c_name) };
            Ok(String::from(c_name.to_string_lossy()))
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn datatype(&self) -> Datatype {
        let c_context = self.context.capi();
        let c_dimension = self.capi();
        let mut c_datatype: ffi::tiledb_datatype_t = out_ptr!();
        let c_ret = unsafe {
            ffi::tiledb_dimension_get_type(
                c_context,
                c_dimension,
                &mut c_datatype,
            )
        };

        assert_eq!(ffi::TILEDB_OK, c_ret);

        Datatype::try_from(c_datatype).expect("Invalid dimension type")
    }

    pub fn cell_val_num(&self) -> TileDBResult<u32> {
        let c_context = self.context.capi();
        let mut c_num: std::ffi::c_uint = 0;
        let res = unsafe {
            ffi::tiledb_dimension_get_cell_val_num(
                c_context, *self.raw, &mut c_num,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(c_num as u32)
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn domain<Conv: CAPIConverter>(&self) -> TileDBResult<[Conv; 2]> {
        let c_context = self.context.capi();
        let c_dimension = self.capi();
        let mut c_domain_ptr: *const std::ffi::c_void = out_ptr!();

        let c_ret = unsafe {
            ffi::tiledb_dimension_get_domain(
                c_context,
                c_dimension,
                &mut c_domain_ptr,
            )
        };

        // the only errors are possible via mis-use of the C API, which Rust prevents
        assert_eq!(ffi::TILEDB_OK, c_ret);

        let c_domain: &[Conv::CAPIType; 2] =
            unsafe { &*c_domain_ptr.cast::<[Conv::CAPIType; 2]>() };

        Ok([Conv::to_rust(&c_domain[0]), Conv::to_rust(&c_domain[1])])
    }

    /// Returns the tile extent of this dimension.
    pub fn extent<Conv: CAPIConverter>(&self) -> TileDBResult<Conv> {
        let c_context = self.context.capi();
        let c_dimension = self.capi();
        let mut c_extent_ptr: *const ::std::ffi::c_void = out_ptr!();

        let c_ret = unsafe {
            ffi::tiledb_dimension_get_tile_extent(
                c_context,
                c_dimension,
                &mut c_extent_ptr,
            )
        };
        if c_ret == ffi::TILEDB_OK {
            let c_extent = unsafe { &*c_extent_ptr.cast::<Conv::CAPIType>() };
            Ok(Conv::to_rust(c_extent))
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn filters(&self) -> FilterList {
        let mut c_fl: *mut ffi::tiledb_filter_list_t = out_ptr!();

        let c_context = self.context.capi();
        let c_dimension = self.capi();
        let c_ret = unsafe {
            ffi::tiledb_dimension_get_filter_list(
                c_context,
                c_dimension,
                &mut c_fl,
            )
        };

        // only fails if dimension is invalid, which Rust API will prevent
        assert_eq!(ffi::TILEDB_OK, c_ret);

        FilterList {
            context: self.context,
            raw: RawFilterList::Owned(c_fl),
        }
    }
}

impl<'ctx> Debug for Dimension<'ctx> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let json = json!({
            "name": self.name(),
            "datatype": format!("{}", self.datatype()),
            "cell_val_num": self.cell_val_num(),
            "domain": fn_typed!(self.domain, self.datatype() => match domain {
                Ok(x) => json!(x),
                Err(e) => json!(e)
            }),
            "extent": fn_typed!(self.extent, self.datatype() => match extent {
                Ok(x) => json!(x),
                Err(e) => json!(e),
            }),
            "filters": format!("{:?}", self.filters()),
            "raw": format!("{:p}", *self.raw)
        });
        write!(f, "{}", json)
    }
}

impl<'c1, 'c2> PartialEq<Dimension<'c2>> for Dimension<'c1> {
    fn eq(&self, other: &Dimension<'c2>) -> bool {
        let name_match = match (self.name(), other.name()) {
            (Ok(mine), Ok(theirs)) => mine == theirs,
            _ => false,
        };
        if !name_match {
            return false;
        }

        let type_match = self.datatype() == other.datatype();
        if !type_match {
            return false;
        }

        let cell_val_match = match (self.cell_val_num(), other.cell_val_num()) {
            (Ok(mine), Ok(theirs)) => mine == theirs,
            _ => false,
        };
        if !cell_val_match {
            return false;
        }

        let domain_match = fn_typed!(
            self.datatype(),
            DT,
            match (self.domain::<DT>(), other.domain::<DT>()) {
                (Ok(mine), Ok(theirs)) => mine == theirs,
                _ => false,
            }
        );
        if !domain_match {
            return false;
        }

        let extent_match = fn_typed!(
            self.datatype(),
            DT,
            match (self.extent::<DT>(), other.extent::<DT>()) {
                (Ok(mine), Ok(theirs)) => mine == theirs,
                _ => false,
            }
        );
        if !extent_match {
            return false;
        }

        let filters_match = self.filters() == other.filters();
        if !filters_match {
            return false;
        }

        true
    }
}

pub struct Builder<'ctx> {
    dim: Dimension<'ctx>,
}

impl<'ctx> Builder<'ctx> {
    // TODO: extent might be optional?
    // and it
    pub fn new<Conv: CAPIConverter>(
        context: &'ctx Context,
        name: &str,
        datatype: Datatype,
        domain: &[Conv; 2],
        extent: &Conv,
    ) -> TileDBResult<Self> {
        let c_context = context.capi();
        let c_datatype = datatype.capi_enum();

        let c_name = cstring!(name);

        let c_domain: [Conv::CAPIType; 2] =
            [domain[0].to_capi(), domain[1].to_capi()];
        let c_extent: Conv::CAPIType = extent.to_capi();

        let mut c_dimension: *mut ffi::tiledb_dimension_t =
            std::ptr::null_mut();

        if unsafe {
            ffi::tiledb_dimension_alloc(
                c_context,
                c_name.as_ptr(),
                c_datatype,
                &c_domain[0] as *const <Conv>::CAPIType
                    as *const std::ffi::c_void,
                &c_extent as *const <Conv>::CAPIType as *const std::ffi::c_void,
                &mut c_dimension,
            )
        } == ffi::TILEDB_OK
        {
            Ok(Builder {
                dim: Dimension {
                    context,
                    raw: RawDimension::Owned(c_dimension),
                },
            })
        } else {
            Err(context.expect_last_error())
        }
    }

    pub fn context(&self) -> &'ctx Context {
        self.dim.context
    }

    pub fn name(&self) -> TileDBResult<String> {
        self.dim.name()
    }

    pub fn cell_val_num(self, num: u32) -> TileDBResult<Self> {
        let c_context = self.dim.context.capi();
        let c_num = num as std::ffi::c_uint;
        let res = unsafe {
            ffi::tiledb_dimension_set_cell_val_num(
                c_context,
                *self.dim.raw,
                c_num,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(self)
        } else {
            Err(self.dim.context.expect_last_error())
        }
    }

    pub fn filters(self, filters: FilterList) -> TileDBResult<Self> {
        let c_context = self.dim.context.capi();
        let c_dimension = self.dim.capi();
        let c_fl = filters.capi();

        if unsafe {
            ffi::tiledb_dimension_set_filter_list(c_context, c_dimension, c_fl)
        } == ffi::TILEDB_OK
        {
            Ok(self)
        } else {
            Err(self.dim.context.expect_last_error())
        }
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

#[cfg(test)]
mod tests {
    use crate::array::dimension::*;
    use crate::filter::*;
    use crate::filter_list::Builder as FilterListBuilder;

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

            assert_eq!(Datatype::Int32, dim.datatype());

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
            let cell_val_num = 1;
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
        for cell_val_num in vec![0, 2, 4].into_iter() {
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
            .cell_val_num(cell_val_num);
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

            let fl = dimension.filters();
            assert_eq!(0, fl.get_num_filters().unwrap());
        }

        // with some
        {
            let domain: [i32; 2] = [1, 4];
            let extent: i32 = 4;
            let fl = FilterListBuilder::new(&context)?
                .add_filter(Filter::create(
                    &context,
                    FilterData::Compression(CompressionData::new(
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

            let fl = dimension.filters();
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
}
