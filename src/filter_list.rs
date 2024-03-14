extern crate tiledb_sys as ffi;

use crate::context::Context;
use crate::filter::Filter;
use crate::Result as TileDBResult;

pub struct FilterList {
    pub(crate) _wrapped: *mut ffi::tiledb_filter_list_t,
}

impl FilterList {
    pub fn new(ctx: &Context) -> TileDBResult<FilterList> {
        let mut flist = FilterList {
            _wrapped: std::ptr::null_mut::<ffi::tiledb_filter_list_t>(),
        };
        let res = unsafe {
            ffi::tiledb_filter_list_alloc(ctx.as_mut_ptr(), &mut flist._wrapped)
        };
        if res == ffi::TILEDB_OK {
            Ok(flist)
        } else {
            Err(ctx.expect_last_error())
        }
    }

    pub fn as_mut_ptr(&self) -> *mut ffi::tiledb_filter_list_t {
        self._wrapped
    }

    pub fn as_mut_ptr_ptr(&mut self) -> *mut *mut ffi::tiledb_filter_list_t {
        &mut self._wrapped
    }

    pub fn add_filter(
        &mut self,
        ctx: &Context,
        filter: &Filter,
    ) -> TileDBResult<()> {
        let res = unsafe {
            ffi::tiledb_filter_list_add_filter(
                ctx.as_mut_ptr(),
                self._wrapped,
                filter.as_mut_ptr(),
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(ctx.expect_last_error())
        }
    }

    pub fn get_num_filters(&self, ctx: &Context) -> TileDBResult<u32> {
        let mut num: u32 = 0;
        let res = unsafe {
            ffi::tiledb_filter_list_get_nfilters(
                ctx.as_mut_ptr(),
                self._wrapped,
                &mut num,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(num)
        } else {
            Err(ctx.expect_last_error())
        }
    }

    pub fn get_filter(
        &self,
        ctx: &Context,
        index: u32,
    ) -> TileDBResult<Filter> {
        let mut filter = Filter::default();
        let res = unsafe {
            ffi::tiledb_filter_list_get_filter_from_index(
                ctx.as_mut_ptr(),
                self._wrapped,
                index,
                filter.as_mut_ptr_ptr(),
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(filter)
        } else {
            Err(ctx.expect_last_error())
        }
    }

    pub fn set_max_chunk_size(
        &self,
        ctx: &Context,
        size: u32,
    ) -> TileDBResult<()> {
        let res = unsafe {
            ffi::tiledb_filter_list_set_max_chunk_size(
                ctx.as_mut_ptr(),
                self._wrapped,
                size,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(ctx.expect_last_error())
        }
    }

    pub fn get_max_chunk_size(&self, ctx: &Context) -> TileDBResult<u32> {
        let mut size: u32 = 0;
        let res = unsafe {
            ffi::tiledb_filter_list_get_max_chunk_size(
                ctx.as_mut_ptr(),
                self._wrapped,
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

impl Default for FilterList {
    fn default() -> Self {
        Self {
            _wrapped: std::ptr::null_mut::<ffi::tiledb_filter_list_t>(),
        }
    }
}

impl Drop for FilterList {
    fn drop(&mut self) {
        if self._wrapped.is_null() {
            return;
        }
        unsafe { ffi::tiledb_filter_list_free(&mut self._wrapped) }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::filter::FilterType;

    #[test]
    fn filter_list_alloc() {
        let ctx = Context::new().expect("Error creating context instance.");
        FilterList::new(&ctx).expect("Error creating filter list instance.");
    }

    #[test]
    fn filter_list_add_filter() {
        let ctx = Context::new().expect("Error creating context instance.");
        let filter = Filter::new(&ctx, FilterType::ZSTD)
            .expect("Error creating filter instance.");
        let mut flist = FilterList::new(&ctx)
            .expect("Error creating filter list instance.");

        let nfilters = flist
            .get_num_filters(&ctx)
            .expect("Error getting number of filters.");
        assert_eq!(nfilters, 0);

        flist
            .add_filter(&ctx, &filter)
            .expect("Error adding filter.");

        let nfilters = flist
            .get_num_filters(&ctx)
            .expect("Error getting number of filters.");
        assert_eq!(nfilters, 1);
    }

    #[test]
    fn filter_list_get_filter() {
        let ctx = Context::new().expect("Error creating context instance.");
        let filter1 = Filter::new(&ctx, FilterType::NONE)
            .expect("Error creating filter instance 1.");
        let filter2 = Filter::new(&ctx, FilterType::DICTIONARY)
            .expect("Error creating filter instance 2.");
        let filter3 = Filter::new(&ctx, FilterType::ZSTD)
            .expect("Error creating filter instance 3.");
        let mut flist = FilterList::new(&ctx)
            .expect("Error creating filter list instance.");

        flist
            .add_filter(&ctx, &filter1)
            .expect("Error adding filter 1.");
        flist
            .add_filter(&ctx, &filter2)
            .expect("Error adding filter 2.");
        flist
            .add_filter(&ctx, &filter3)
            .expect("Error adding filter 3.");

        let nfilters = flist
            .get_num_filters(&ctx)
            .expect("Error getting number of filters.");
        assert_eq!(nfilters, 3);

        let filter4 = flist
            .get_filter(&ctx, 1)
            .expect("Error getting filter at index 1");
        let ftype = filter4.get_type(&ctx).expect("Error getting filter type.");
        assert_eq!(ftype, FilterType::DICTIONARY);
    }
}
