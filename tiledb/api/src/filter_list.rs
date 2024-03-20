use std::ops::Deref;

use crate::context::Context;
use crate::filter::{Filter, RawFilter};
use crate::Result as TileDBResult;

pub(crate) enum RawFilterList {
    Owned(*mut ffi::tiledb_filter_list_t),
}

impl Deref for RawFilterList {
    type Target = *mut ffi::tiledb_filter_list_t;
    fn deref(&self) -> &Self::Target {
        match *self {
            RawFilterList::Owned(ref ffi) => ffi,
        }
    }
}

impl Drop for RawFilterList {
    fn drop(&mut self) {
        let RawFilterList::Owned(ref mut ffi) = *self;
        unsafe { ffi::tiledb_filter_list_free(ffi) }
    }
}

pub struct FilterList<'ctx> {
    pub(crate) context: &'ctx Context,
    pub(crate) raw: RawFilterList,
}

impl<'ctx> FilterList<'ctx> {
    pub fn capi(&self) -> *mut ffi::tiledb_filter_list_t {
        *self.raw
    }

    pub fn get_num_filters(&self) -> TileDBResult<u32> {
        let mut num: u32 = 0;
        let res = unsafe {
            ffi::tiledb_filter_list_get_nfilters(
                self.context.capi(),
                *self.raw,
                &mut num,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(num)
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn get_filter(&self, index: u32) -> TileDBResult<Filter<'ctx>> {
        let mut c_filter: *mut ffi::tiledb_filter_t = out_ptr!();
        let res = unsafe {
            ffi::tiledb_filter_list_get_filter_from_index(
                self.context.capi(),
                *self.raw,
                index,
                &mut c_filter,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(Filter::new(self.context, RawFilter::Owned(c_filter)))
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn get_max_chunk_size(&self, ctx: &Context) -> TileDBResult<u32> {
        let mut size: u32 = 0;
        let res = unsafe {
            ffi::tiledb_filter_list_get_max_chunk_size(
                self.context.capi(),
                *self.raw,
                &mut size,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(size)
        } else {
            Err(ctx.expect_last_error())
        }
    }
}

pub struct Builder<'ctx> {
    filter_list: FilterList<'ctx>,
}

impl<'ctx> Builder<'ctx> {
    pub fn new(context: &'ctx Context) -> TileDBResult<Self> {
        let mut c_flist: *mut ffi::tiledb_filter_list_t = out_ptr!();
        let res = unsafe {
            ffi::tiledb_filter_list_alloc(context.capi(), &mut c_flist)
        };
        if res == ffi::TILEDB_OK {
            Ok(Builder {
                filter_list: FilterList {
                    context,
                    raw: RawFilterList::Owned(c_flist),
                },
            })
        } else {
            Err(context.expect_last_error())
        }
    }

    pub fn set_max_chunk_size(self, size: u32) -> TileDBResult<Self> {
        let res = unsafe {
            ffi::tiledb_filter_list_set_max_chunk_size(
                self.filter_list.context.capi(),
                *self.filter_list.raw,
                size,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(self)
        } else {
            Err(self.filter_list.context.expect_last_error())
        }
    }

    pub fn add_filter(self, filter: Filter<'ctx>) -> TileDBResult<Self> {
        let res = unsafe {
            ffi::tiledb_filter_list_add_filter(
                self.filter_list.context.capi(),
                *self.filter_list.raw,
                filter.capi(),
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(self)
        } else {
            Err(self.filter_list.context.expect_last_error())
        }
    }

    pub fn build(self) -> FilterList<'ctx> {
        self.filter_list
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::filter::*;

    #[test]
    fn filter_list_alloc() {
        let ctx = Context::new().expect("Error creating context instance.");
        let flist = Builder::new(&ctx)
            .expect("Error creating filter list instance.")
            .build();

        let nfilters = flist
            .get_num_filters()
            .expect("Error getting number of filters.");
        assert_eq!(nfilters, 0);
    }

    #[test]
    fn filter_list_add_filter() -> TileDBResult<()> {
        let ctx = Context::new().expect("Error creating context instance.");

        let flist = Builder::new(&ctx)
            .expect("Error creating filter list instance.")
            .add_filter(
                CompressionFilterBuilder::new(&ctx, CompressionType::Zstd)?
                    .build(),
            )?
            .build();

        let nfilters = flist
            .get_num_filters()
            .expect("Error getting number of filters.");
        assert_eq!(nfilters, 1);

        Ok(())
    }

    #[test]
    fn filter_list_get_filter() -> TileDBResult<()> {
        let ctx = Context::new().expect("Error creating context instance.");
        let flist = Builder::new(&ctx)?
            .add_filter(NoopFilterBuilder::new(&ctx)?.build())?
            .add_filter(
                CompressionFilterBuilder::new(
                    &ctx,
                    CompressionType::Dictionary,
                )?
                .build(),
            )?
            .add_filter(
                CompressionFilterBuilder::new(&ctx, CompressionType::Zstd)?
                    .build(),
            )?
            .build();

        let nfilters = flist
            .get_num_filters()
            .expect("Error getting number of filters.");
        assert_eq!(nfilters, 3);

        let filter4 = flist
            .get_filter(1)
            .expect("Error getting filter at index 1");
        let ftype = filter4.get_type().expect("Error getting filter type.");
        assert_eq!(ftype, ffi::FilterType::Dictionary);

        Ok(())
    }
}
