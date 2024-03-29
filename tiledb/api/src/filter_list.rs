use std::borrow::Borrow;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::ops::Deref;

use crate::context::Context;
use crate::filter::{Filter, FilterData, RawFilter};
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

    pub fn to_vec(&self) -> TileDBResult<Vec<Filter<'ctx>>> {
        (0..self.get_num_filters()?)
            .map(|f| self.get_filter(f))
            .collect()
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

impl<'ctx> Debug for FilterList<'ctx> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let nfilters = match self.get_num_filters() {
            Ok(n) => n,
            Err(e) => return write!(f, "<error reading filter list: {}>", e),
        };
        write!(f, "[")?;
        for fi in 0..nfilters {
            match self.get_filter(fi) {
                Ok(fd) => match fd.filter_data() {
                    Ok(fd) => write!(f, "{:?},", fd)?,
                    Err(e) => {
                        write!(f, "<error reading filter {}: {}>", fi, e)?
                    }
                },
                Err(e) => write!(f, "<error reading filter {}: {}>", fi, e)?,
            };
        }
        write!(f, "]")
    }
}

impl<'c1, 'c2> PartialEq<FilterList<'c2>> for FilterList<'c1> {
    fn eq(&self, other: &FilterList<'c2>) -> bool {
        let size_match = match (self.get_num_filters(), other.get_num_filters())
        {
            (Ok(mine), Ok(theirs)) => mine == theirs,
            _ => false,
        };
        if !size_match {
            return false;
        }

        for f in 0..self.get_num_filters().unwrap() {
            let filter_match = match (self.get_filter(f), other.get_filter(f)) {
                (Ok(mine), Ok(theirs)) => mine == theirs,
                _ => false,
            };
            if !filter_match {
                return false;
            }
        }
        true
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

    pub fn add_filter_data<F>(self, filter: F) -> TileDBResult<Self>
    where
        F: Borrow<FilterData>,
    {
        let ctx = self.filter_list.context;
        self.add_filter(Filter::create(ctx, filter)?)
    }

    pub fn build(self) -> FilterList<'ctx> {
        self.filter_list
    }
}

pub type FilterListData = Vec<FilterData>;

impl<'ctx> TryFrom<&FilterList<'ctx>> for FilterListData {
    type Error = crate::error::Error;
    fn try_from(filters: &FilterList) -> TileDBResult<Self> {
        filters
            .to_vec()?
            .into_iter()
            .map(|f| FilterData::try_from(&f))
            .collect::<TileDBResult<Self>>()
    }
}

impl<'ctx> crate::Factory<'ctx> for FilterListData {
    type Item = FilterList<'ctx>;

    fn create(&self, context: &'ctx Context) -> TileDBResult<Self::Item> {
        Ok(self
            .iter()
            .fold(Builder::new(context), |b, filter| {
                b?.add_filter_data(filter)
            })?
            .build())
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
            .add_filter(Filter::create(
                &ctx,
                FilterData::Compression(CompressionData::new(
                    CompressionType::Zstd,
                )),
            )?)?
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
            .add_filter(Filter::create(&ctx, FilterData::None)?)?
            .add_filter(Filter::create(
                &ctx,
                FilterData::Compression(CompressionData::new(
                    CompressionType::Dictionary,
                )),
            )?)?
            .add_filter(Filter::create(
                &ctx,
                FilterData::Compression(CompressionData::new(
                    CompressionType::Zstd,
                )),
            )?)?
            .build();

        let nfilters = flist
            .get_num_filters()
            .expect("Error getting number of filters.");
        assert_eq!(nfilters, 3);

        let filter4 = flist
            .get_filter(1)
            .expect("Error getting filter at index 1");
        let ftype = filter4.filter_data().expect("Error getting filter data");
        assert!(matches!(
            ftype,
            FilterData::Compression(CompressionData {
                kind: CompressionType::Dictionary,
                ..
            })
        ));

        Ok(())
    }
}
