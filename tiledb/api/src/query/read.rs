use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::iter::FusedIterator;
use std::rc::Rc;

use anyhow::anyhow;

use super::buffer::read::ReadBufferGetter;
use super::buffer::{
    ReadBuffer, ReadBufferCollection, ReadBufferCollectionItem,
};
use super::sizeinfo::{SizeEntry, SizeInfo};
use super::status::{QueryStatus, QueryStatusDetails};
use super::traits::{Query, QueryBuilder, QueryInternal};
use super::{QueryType, RawQuery};
use crate::array::{Array, CellValNum, Schema};
use crate::context::{CApiInterface, Context, ContextBound};
use crate::datatype::Datatype;
use crate::error::Error;
use crate::Result as TileDBResult;

pub struct ReadQuery {
    array: Array,
    raw: RawQuery,
    buffers: HashMap<String, (bool, bool)>,
    submitted: bool,
}

impl Query for ReadQuery {
    fn context(&self) -> &Context {
        self.array.context()
    }

    fn array(&self) -> &Array {
        &self.array
    }

    fn capi(&self) -> *mut ffi::tiledb_query_t {
        *self.raw
    }
}

impl QueryInternal for ReadQuery {
    fn context(&self) -> &Context {
        self.array.context()
    }

    fn capi(&self) -> *mut ffi::tiledb_query_t {
        *self.raw
    }

    fn buffer_info(&self, name: &str) -> Option<(bool, bool)> {
        self.buffers.get(name).copied()
    }

    fn submitted(&self) -> bool {
        self.submitted
    }
}

impl ReadQuery {
    fn new(array: Array, raw: RawQuery) -> Self {
        Self {
            array,
            raw,
            buffers: HashMap::new(),
            submitted: false,
        }
    }

    pub fn submit(
        &mut self,
        buffers: &Rc<RefCell<ReadBufferCollection>>,
    ) -> TileDBResult<ReadQueryResult> {
        let mut sizes: HashMap<String, SizeEntry> = HashMap::new();

        let buffers = buffers.clone();
        let bufref = buffers.try_borrow_mut().map_err(|e| {
            Error::InvalidArgument(
                anyhow!("The buffers argument is not borrowable.").context(e),
            )
        })?;

        for buffer in bufref.iter() {
            let entry = self.attach_buffer(buffer)?;
            sizes.insert(buffer.name().to_owned(), entry);
        }

        // Ensure that all buffers were provided if this is a resubmission.
        if self.submitted {
            for (field, _) in self.buffers.iter() {
                if !sizes.contains_key(field) {
                    return Err(Error::InvalidArgument(anyhow!(
                        "Missing buffer for field: {}",
                        field
                    )));
                }
            }
        }

        // Set our buffer info for possible resubmission.
        if !self.submitted {
            for (field, sizes) in sizes.iter() {
                let has_offsets = sizes.offsets_size.is_some();
                let has_validity = sizes.validity_size.is_some();
                self.buffers
                    .insert(field.to_owned(), (has_offsets, has_validity));
            }
        }

        // Mark after the first submission to make sure we're consistently
        // providing the same exact buffers for subsequent submissions.
        self.submitted = true;

        // Finally, do the actual submission
        self.do_submit()?;

        // Swap out of our SizeEntry into raw types.
        let mut new_sizes = HashMap::new();
        for (name, sizes) in sizes.iter() {
            new_sizes.insert(name.clone(), SizeInfo::from(sizes));
        }

        let schema = self.array.schema()?;
        let status = self.capi_status()?;
        let details = self.capi_status_details()?;
        Ok(ReadQueryResult::new(
            schema,
            new_sizes,
            status,
            details,
            buffers.clone(),
        ))
    }

    pub fn finalize(self) -> TileDBResult<Array> {
        self.do_finalize()?;
        Ok(self.array)
    }
}

pub struct ReadQueryBuilder {
    query: ReadQuery,
}

impl ContextBound for ReadQueryBuilder {
    fn context(&self) -> &Context {
        self.query.array.context()
    }
}

impl QueryBuilder for ReadQueryBuilder {
    fn context(&self) -> &Context {
        ContextBound::context(self)
    }

    fn array(&self) -> &Array {
        self.query.array()
    }

    fn capi(&self) -> *mut ffi::tiledb_query_t {
        *self.query.raw
    }
}

impl ReadQueryBuilder {
    pub fn new(array: Array) -> TileDBResult<Self> {
        let c_array = array.capi();
        let c_query_type = QueryType::Read.capi_enum();
        let mut c_query: *mut ffi::tiledb_query_t = out_ptr!();
        array.capi_call(|ctx| unsafe {
            ffi::tiledb_query_alloc(ctx, c_array, c_query_type, &mut c_query)
        })?;

        Ok(Self {
            query: ReadQuery::new(array, RawQuery::Owned(c_query)),
        })
    }

    pub fn build(self) -> ReadQuery {
        self.query
    }
}

pub struct ReadQueryResult {
    schema: Schema,
    sizes: HashMap<String, SizeInfo>,
    status: QueryStatus,
    details: QueryStatusDetails,
    buffers: Rc<RefCell<ReadBufferCollection>>,
}

impl ReadQueryResult {
    pub fn new(
        schema: Schema,
        sizes: HashMap<String, SizeInfo>,
        status: ffi::tiledb_query_status_t,
        details: ffi::tiledb_query_status_details_reason_t,
        buffers: Rc<RefCell<ReadBufferCollection>>,
    ) -> Self {
        Self {
            schema,
            sizes,
            status: QueryStatus::from(status),
            details: QueryStatusDetails::from(details),
            buffers,
        }
    }

    pub fn nresults(&self) -> TileDBResult<u64> {
        if let Some((name, sizes)) = self.sizes.iter().next() {
            let field = self.schema.field(name)?;
            if matches!(field.cell_val_num()?, CellValNum::Var) {
                // Unwrap guaranteed given that the query returned results.
                let nbytes = sizes.offsets_size.unwrap();
                Ok(nbytes / std::mem::size_of::<u64>() as u64)
            } else {
                let nbytes = sizes.data_size;
                let cvn = u32::from(field.cell_val_num()?);
                let bytes_per = field.datatype()?.size() * cvn as u64;
                Ok(nbytes / bytes_per)
            }
        } else {
            Ok(0)
        }
    }

    pub fn details(&self) -> QueryStatusDetails {
        self.details.clone()
    }

    pub fn completed(&self) -> bool {
        matches!(self.status, QueryStatus::Completed)
    }

    pub fn slices(&self) -> TileDBResult<ReadQueryResultSlices> {
        Ok(ReadQueryResultSlices::new(
            &self.schema,
            self.sizes.clone(),
            self.buffers.as_ref().borrow(),
        ))
    }
}

pub struct ReadQueryResultSlices<'result> {
    schema: &'result Schema,
    sizes: HashMap<String, SizeInfo>,
    buffers: Ref<'result, ReadBufferCollection>,
}

impl<'result> ReadQueryResultSlices<'result> {
    pub fn new(
        schema: &'result Schema,
        sizes: HashMap<String, SizeInfo>,
        buffers: Ref<'result, ReadBufferCollection>,
    ) -> Self {
        Self {
            schema,
            sizes,
            buffers,
        }
    }

    pub fn get_field(
        &self,
        name: &str,
    ) -> TileDBResult<(FieldInfo, &ReadBufferCollectionItem)> {
        let field = self.schema.field(name)?;
        let datatype = field.datatype()?;
        let cell_val_num = field.cell_val_num()?;
        let nullable = field.nullability()?;

        let sizes = self.sizes.get(name);
        if sizes.is_none() {
            return Err(Error::InvalidArgument(anyhow!(
                "Unknown field: {}",
                name
            )));
        }
        let sizes = sizes.unwrap().clone();

        let info = (name.to_owned(), datatype, cell_val_num, nullable, sizes);

        for buffer in self.buffers.iter() {
            if buffer.name() == name {
                return Ok((info, buffer));
            }
        }

        Err(Error::InvalidArgument(anyhow!(
            "No buffer found for field: {}",
            name
        )))
    }
}

pub struct ReadQueryField<'result, T> {
    name: String,
    datatype: Datatype,
    cell_val_num: CellValNum,
    nullable: bool,
    sizes: SizeInfo,
    buffer: &'result ReadBuffer<T>,
}

type FieldInfo = (String, Datatype, CellValNum, bool, SizeInfo);

impl<'result, T> ReadQueryField<'result, T> {
    pub fn new(info: FieldInfo, buffer: &'result ReadBuffer<T>) -> Self {
        Self {
            name: info.0,
            datatype: info.1,
            cell_val_num: info.2,
            nullable: info.3,
            sizes: info.4,
            buffer,
        }
    }

    pub fn as_data_slice(&self) -> &[T] {
        let nbytes = self.sizes.data_size as usize;
        let per_item = self.datatype.size() as usize;
        &self.buffer.data[..(nbytes / per_item)]
    }

    pub fn as_offsets_slice(&self) -> Option<&[u64]> {
        let nbytes = self.sizes.offsets_size? as usize;
        let len = nbytes / std::mem::size_of::<u64>();
        self.buffer.offsets.as_ref().map(|o| &o[0..len])
    }

    pub fn as_validity_slice(&self) -> Option<&[u8]> {
        let nbytes = self.sizes.validity_size? as usize;
        self.buffer.validity.as_ref().map(|v| &v[..nbytes])
    }

    pub fn as_slices(&self) -> (&[T], Option<&[u64]>, Option<&[u8]>) {
        (
            self.as_data_slice(),
            self.as_offsets_slice(),
            self.as_validity_slice(),
        )
    }

    fn itype(&self) -> IterType {
        // First we figure out Base/Fixed/Var
        match self.cell_val_num {
            CellValNum::Fixed(cvn) => {
                if cvn.get() == 1 && self.nullable {
                    IterType::Nullable
                } else if cvn.get() == 1 {
                    IterType::Base
                } else if self.nullable {
                    IterType::NullableFixed
                } else {
                    IterType::Fixed
                }
            }
            CellValNum::Var => {
                if self.nullable {
                    IterType::NullableVar
                } else {
                    IterType::Var
                }
            }
        }
    }
}

#[derive(Eq, PartialEq)]
pub enum IterType {
    Base,
    Fixed,
    Var,
    Nullable,
    NullableFixed,
    NullableVar,
}

impl IterType {
    fn to_error_message(&self) -> &str {
        match self {
            Self::Base => "iter()",
            Self::Fixed => "fixed_iter()",
            Self::Var => "var_iter()",
            Self::Nullable => "nullable_iter()",
            Self::NullableFixed => "nullable_fixed_iter()",
            Self::NullableVar => "nullable_var()",
        }
    }
}

pub trait ReadQueryFieldAsIterator<'result, T: 'static> {
    fn iter(&'result self) -> TileDBResult<impl Iterator<Item = &T>>;

    fn fixed_iter(&'result self) -> TileDBResult<impl Iterator<Item = &[T]>>;

    fn var_iter(&'result self) -> TileDBResult<impl Iterator<Item = &[T]>>;

    fn nullable_iter(
        &'result self,
    ) -> TileDBResult<impl Iterator<Item = (&T, &u8)>>;

    fn nullable_fixed_iter(
        &'result self,
    ) -> TileDBResult<impl Iterator<Item = (&[T], &u8)>>;

    fn nullable_var_iter(
        &'result self,
    ) -> TileDBResult<impl Iterator<Item = (&[T], &u8)>>;

    fn check_iter_type(
        &self,
        name: &str,
        expect: IterType,
        found: IterType,
    ) -> TileDBResult<()> {
        if found != expect {
            return Err(Error::InvalidArgument(anyhow!(
                "Incorrect iterator for {}, use {} instead.",
                name,
                found.to_error_message()
            )));
        }

        Ok(())
    }
}

macro_rules! field_iter_impl {
    ($($ty:ty),+) => {
        $(
            impl<'result> ReadQueryFieldAsIterator<'result, $ty>
                for ReadQueryField<'result, $ty>
            {
                fn iter(&'result self) -> TileDBResult<impl Iterator<Item = &$ty>> {
                    self.check_iter_type(&self.name, IterType::Base, self.itype())?;
                    Ok(self.as_data_slice().iter())
                }

                fn fixed_iter(&'result self) -> TileDBResult<impl Iterator<Item = &[$ty]>> {
                    self.check_iter_type(&self.name, IterType::Fixed, self.itype())?;
                    if let CellValNum::Fixed(cvn) = self.cell_val_num {
                        Ok(self.as_data_slice().chunks(cvn.get() as usize))
                    } else {
                        unreachable!()
                    }
                }

                fn var_iter(&'result self) -> TileDBResult<impl Iterator<Item = &[$ty]>> {
                    self.check_iter_type(&self.name, IterType::Var, self.itype())?;
                    if let CellValNum::Var = self.cell_val_num {
                        Ok(VarIterator::new(
                            self.as_data_slice(),
                            self.as_offsets_slice().unwrap(),
                        ))
                    } else {
                        unreachable!()
                    }
                }

                fn nullable_iter(
                    &'result self,
                ) -> TileDBResult<impl Iterator<Item = (&$ty, &u8)>> {
                    self.check_iter_type(&self.name, IterType::Nullable, self.itype())?;
                    let diter = self.as_data_slice().iter();
                    let viter = self.as_validity_slice().unwrap().iter();
                    Ok(std::iter::zip(diter, viter))
                }

                fn nullable_fixed_iter(
                    &'result self,
                ) -> TileDBResult<impl Iterator<Item = (&[$ty], &u8)>> {
                    self.check_iter_type(
                        &self.name,
                        IterType::NullableFixed,
                        self.itype(),
                    )?;
                    let diter = if let CellValNum::Fixed(cvn) = self.cell_val_num {
                        self.as_data_slice().chunks(cvn.get() as usize)
                    } else {
                        unreachable!()
                    };
                    let viter = self.as_validity_slice().unwrap().iter();
                    Ok(std::iter::zip(diter, viter))
                }

                fn nullable_var_iter(
                    &'result self,
                ) -> TileDBResult<impl Iterator<Item = (&[$ty], &u8)>> {
                    self.check_iter_type(&self.name, IterType::NullableVar, self.itype())?;
                    let diter = if let CellValNum::Var = self.cell_val_num {
                        VarIterator::new(
                            self.as_data_slice(),
                            self.as_offsets_slice().unwrap(),
                        )
                    } else {
                        unreachable!()
                    };
                    let viter = self.as_validity_slice().unwrap().iter();
                    Ok(std::iter::zip(diter, viter))
                }
            }
        )+
    }
}

field_iter_impl!(u8, u16, u32, u64);
field_iter_impl!(i8, i16, i32, i64);
field_iter_impl!(f32, f64);

pub struct VarIterator<'result, T> {
    data: &'result [T],
    offsets: &'result [u64],
    curr_idx: usize,
}

impl<'result, T> VarIterator<'result, T> {
    fn new(data: &'result [T], offsets: &'result [u64]) -> Self {
        Self {
            data,
            offsets,
            curr_idx: 0,
        }
    }
}

impl<'result, T> Iterator for VarIterator<'result, T> {
    type Item = &'result [T];

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr_idx >= self.offsets.len() {
            return None;
        }

        let bytes_per = std::mem::size_of::<T>();
        let len = if self.curr_idx < self.offsets.len() - 1 {
            // We have to calculate the number of elements. Offsets are the
            // number of bytes.
            let nbytes =
                self.offsets[self.curr_idx + 1] - self.offsets[self.curr_idx];
            nbytes as usize / bytes_per
        } else {
            // We're returning the rest of the slice
            self.data.len() - self.curr_idx
        };

        let start = self.curr_idx;
        let end = start + len;
        self.curr_idx += 1;
        Some(&self.data[start..end])
    }
}

impl<'result, T> FusedIterator for VarIterator<'result, T> {}

pub trait ReadQueryFieldAsStringIterator<'result> {
    fn lossy_str_iter(&self) -> TileDBResult<impl Iterator<Item = String>>;
}

impl<'result> ReadQueryFieldAsStringIterator<'result>
    for ReadQueryField<'result, u8>
{
    fn lossy_str_iter(&self) -> TileDBResult<impl Iterator<Item = String>> {
        if !matches!(
            self.datatype,
            Datatype::StringAscii | Datatype::StringUtf8
        ) {
            return Err(Error::InvalidArgument(anyhow!(
                "Invalid datatype for lossy string iterator: {:?}",
                self.datatype
            )));
        }

        self.check_iter_type(&self.name, IterType::Var, self.itype())?;

        Ok(LossyStringIterator::new(
            self.as_data_slice(),
            self.as_offsets_slice().unwrap(),
        ))
    }
}

pub struct LossyStringIterator<'result> {
    data: &'result [u8],
    offsets: &'result [u64],
    curr_idx: usize,
}

impl<'result> LossyStringIterator<'result> {
    fn new(data: &'result [u8], offsets: &'result [u64]) -> Self {
        Self {
            data,
            offsets,
            curr_idx: 0,
        }
    }
}

impl<'result> Iterator for LossyStringIterator<'result> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr_idx >= self.offsets.len() {
            return None;
        }

        let len = if self.curr_idx < self.offsets.len() - 1 {
            (self.offsets[self.curr_idx + 1] - self.offsets[self.curr_idx])
                as usize
        } else {
            self.data.len() - self.curr_idx
        };

        let start = self.curr_idx;
        let end = start + len;
        self.curr_idx += 1;
        Some(String::from_utf8_lossy(&self.data[start..end]).to_string())
    }
}

impl<'result> FusedIterator for LossyStringIterator<'result> {}

pub trait ReadQueryFieldAccessor<T> {
    fn field(&self, name: &str) -> TileDBResult<ReadQueryField<T>>;
}

macro_rules! rq_field_accessor_impl {
    ($($ty:ty),+) => {
        $(
            impl<'result> ReadQueryFieldAccessor<$ty> for ReadQueryResultSlices<'result> {
                fn field(&self, name: &str) -> TileDBResult<ReadQueryField<$ty>> {
                    let (field, item) = self.get_field(name)?;
                    Ok(ReadQueryField::new(field, item.get_buffer()?))
                }
            }
        )+
    }
}

rq_field_accessor_impl!(u8, u16, u32, u64);
rq_field_accessor_impl!(i8, i16, i32, i64);
rq_field_accessor_impl!(f32, f64);

#[cfg(test)]
mod tests {
    use super::ReadQueryField as RQField;
    use super::*;
    use itertools::izip;
    use tempfile::TempDir;

    use crate::array::Mode;
    use crate::context::Context;
    use crate::query::QueryLayout;

    #[test]
    fn basic_read() -> TileDBResult<()> {
        let ctx = Context::new()?;

        // Create a temp array uri
        let dir =
            TempDir::new().map_err(|e| Error::InvalidArgument(anyhow!(e)))?;
        let array_dir = dir.path().join("fragment_info_test_dense");
        let array_uri = String::from(array_dir.to_str().unwrap());

        super::super::write::tests::create_sparse_array(&ctx, &array_uri)?;
        super::super::write::tests::write_sparse_data(&ctx, &array_uri)?;

        // Create our query
        let array = Array::open(&ctx, array_uri, Mode::Read)?;
        let mut query = ReadQueryBuilder::new(array)?
            .layout(QueryLayout::Unordered)?
            .build();

        // Create our buffer collection
        let id_data = vec![0i32; 10].into_boxed_slice();
        let attr_data = vec![0u64; 10].into_boxed_slice();

        let buffers = ReadBufferCollection::new();
        buffers
            .borrow_mut()
            .add_buffer("id", id_data)?
            .add_buffer("attr", attr_data)?;

        let result = query.submit(&buffers)?;
        assert!(result.completed());

        let slices = result.slices()?;
        let ids: RQField<i32> = slices.field("id")?;
        let attrs: RQField<u64> = slices.field("attr")?;
        for (id, attr) in izip!(ids.iter()?, attrs.iter()?) {
            println!("Id: {} Attr: {}", id, attr);
        }

        Ok(())
    }
}
