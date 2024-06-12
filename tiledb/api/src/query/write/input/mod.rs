use std::num::NonZeroU32;
use std::rc::Rc;

use crate::array::{CellValNum, Schema};
use crate::datatype::PhysicalType;
use crate::error::{DatatypeErrorKind, Error};
use crate::query::buffer::{
    Buffer, CellStructure, QueryBuffers, QueryBuffersMut, TypedQueryBuffers,
};
use crate::Result as TileDBResult;

#[cfg(feature = "arrow")]
pub mod arrow;

pub trait DataProvider {
    type Unit: PhysicalType;

    fn query_buffers(
        &self,
        cell_val_num: CellValNum,
        is_nullable: bool,
    ) -> TileDBResult<QueryBuffers<Self::Unit>>;
}

pub trait TypedDataProvider {
    fn typed_query_buffers(
        &self,
        cell_val_num: CellValNum,
        is_nullable: bool,
    ) -> TileDBResult<TypedQueryBuffers>;
}

impl<T> TypedDataProvider for T
where
    T: DataProvider,
    for<'data> TypedQueryBuffers<'data>:
        From<QueryBuffers<'data, <T as DataProvider>::Unit>>,
{
    fn typed_query_buffers(
        &self,
        cell_val_num: CellValNum,
        is_nullable: bool,
    ) -> TileDBResult<TypedQueryBuffers> {
        let qb = <T as DataProvider>::query_buffers(
            self,
            cell_val_num,
            is_nullable,
        )?;
        Ok(qb.into())
    }
}

impl<'data, C> DataProvider for QueryBuffers<'data, C>
where
    C: PhysicalType,
{
    type Unit = C;

    fn query_buffers(
        &self,
        _cell_val_num: CellValNum,
        _is_nullable: bool,
    ) -> TileDBResult<QueryBuffers<Self::Unit>> {
        let ptr = self.data.as_ptr();
        let byte_len = std::mem::size_of_val(&self.data);
        let raw_slice = unsafe { std::slice::from_raw_parts(ptr, byte_len) };
        Ok(QueryBuffers {
            data: Buffer::Borrowed(raw_slice),
            cell_structure: self.cell_structure.borrow(),
            validity: Option::map(self.validity.as_ref(), |v| {
                Buffer::Borrowed(v.as_ref())
            }),
        })
    }
}

impl<'data, C> DataProvider for QueryBuffersMut<'data, C>
where
    C: PhysicalType,
{
    type Unit = C;

    fn query_buffers(
        &self,
        _cell_val_num: CellValNum,
        _is_nullable: bool,
    ) -> TileDBResult<QueryBuffers<Self::Unit>> {
        let ptr = self.data.as_ptr();
        let byte_len = std::mem::size_of_val(&self.data);
        let raw_slice = unsafe { std::slice::from_raw_parts(ptr, byte_len) };
        Ok(QueryBuffers {
            data: Buffer::Borrowed(raw_slice),
            cell_structure: self.cell_structure.borrow(),
            validity: Option::map(self.validity.as_ref(), |v| {
                Buffer::Borrowed(v.as_ref())
            }),
        })
    }
}

/// Used to convert data into a slice for use as query input.
// Note that this is a *private* trait, not a public one!
// That's because it's very hard to write a generic impl of
// DataProvider for `[S] where S: AsSlice` and `Vec<S> where S: AsSlice`,
// meaning there's not much use for this beyond consolidating
// some common impl logic inside of private functions.
// Maybe we could make an adapter type in the future.
trait AsSlice {
    type Item: PhysicalType;
    fn values(&self) -> &[Self::Item];
}

impl<T> AsSlice for Vec<T>
where
    T: PhysicalType,
{
    type Item = T;
    fn values(&self) -> &[Self::Item] {
        self.as_slice()
    }
}

impl<T> AsSlice for [T]
where
    T: PhysicalType,
{
    type Item = T;
    fn values(&self) -> &[Self::Item] {
        self
    }
}

impl AsSlice for &str {
    type Item = u8;
    fn values(&self) -> &[Self::Item] {
        self.as_bytes()
    }
}

impl AsSlice for String {
    type Item = u8;
    fn values(&self) -> &[Self::Item] {
        self.as_bytes()
    }
}

/// Helper function to compute the cell structure
/// of objects which resemble a nested slice
fn cell_structure<S>(
    cell_val_num: CellValNum,
    items: &[S],
) -> TileDBResult<CellStructure>
where
    S: AsSlice,
{
    match cell_val_num {
        CellValNum::Fixed(nz) => {
            let expect_len = nz.get() as usize;
            for cell in items.iter() {
                if cell.values().len() != expect_len {
                    return Err(Error::Datatype(
                        DatatypeErrorKind::UnexpectedCellStructure {
                            context: None,
                            expected: CellValNum::Fixed(nz),
                            found: CellValNum::Var,
                        },
                    ));
                }
            }
            Ok(CellStructure::Fixed(nz))
        }
        CellValNum::Var => {
            let mut offset_accumulator = 0;
            let offsets = std::iter::once(0u64)
                .chain(items.iter().map(|s| {
                    offset_accumulator += s.values().len();
                    offset_accumulator as u64
                }))
                .collect::<Vec<u64>>()
                .into_boxed_slice();
            Ok(CellStructure::Var(offsets.into()))
        }
    }
}

/// Helper function to implement `DataProvider::query_buffers`
/// for types which resemble a nested slice.
// (Without negative trait bounds we can't provide separate DataProvider
// impls for `Vec<C> where C: PhysicalType` and `Vec<S> where S: AsSlice`)
fn query_buffers_impl<S>(
    value: &[S],
    cell_val_num: CellValNum,
    is_nullable: bool,
) -> TileDBResult<QueryBuffers<<S as AsSlice>::Item>>
where
    S: AsSlice,
{
    let cell_structure = cell_structure(cell_val_num, value)?;
    let data_capacity = match cell_structure {
        CellStructure::Fixed(nz) => value.len() * nz.get() as usize,
        CellStructure::Var(ref offsets) => *offsets.last().unwrap() as usize,
    };

    let mut data = Vec::with_capacity(data_capacity);
    value.iter().for_each(|s| {
        data.extend(s.values());
    });

    let validity = if is_nullable {
        Some(Buffer::Owned(vec![1u8; value.len()].into_boxed_slice()))
    } else {
        None
    };

    Ok(QueryBuffers {
        data: Buffer::Owned(data.into_boxed_slice()),
        cell_structure,
        validity,
    })
}

impl<C> DataProvider for Vec<C>
where
    C: PhysicalType,
{
    type Unit = C;

    fn query_buffers(
        &self,
        cell_val_num: CellValNum,
        is_nullable: bool,
    ) -> TileDBResult<QueryBuffers<Self::Unit>> {
        self.as_slice().query_buffers(cell_val_num, is_nullable)
    }
}

impl<C> DataProvider for [C]
where
    C: PhysicalType,
{
    type Unit = C;

    fn query_buffers(
        &self,
        _cell_val_num: CellValNum,
        is_nullable: bool,
    ) -> TileDBResult<QueryBuffers<Self::Unit>> {
        let validity = if is_nullable {
            Some(Buffer::Owned(vec![1u8; self.len()].into_boxed_slice()))
        } else {
            None
        };

        Ok(QueryBuffers {
            data: Buffer::Borrowed(self),
            cell_structure: NonZeroU32::new(1).unwrap().into(),
            validity,
        })
    }
}

impl<C> DataProvider for Vec<Vec<C>>
where
    C: PhysicalType,
{
    type Unit = C;

    fn query_buffers(
        &self,
        cell_val_num: CellValNum,
        is_nullable: bool,
    ) -> TileDBResult<QueryBuffers<Self::Unit>> {
        query_buffers_impl(self, cell_val_num, is_nullable)
    }
}

impl DataProvider for Vec<&str> {
    type Unit = u8;

    fn query_buffers(
        &self,
        cell_val_num: CellValNum,
        is_nullable: bool,
    ) -> TileDBResult<QueryBuffers<Self::Unit>> {
        query_buffers_impl(self, cell_val_num, is_nullable)
    }
}

impl DataProvider for Vec<String> {
    type Unit = u8;

    fn query_buffers(
        &self,
        cell_val_num: CellValNum,
        is_nullable: bool,
    ) -> TileDBResult<QueryBuffers<Self::Unit>> {
        query_buffers_impl(self, cell_val_num, is_nullable)
    }
}

pub trait RecordProvider<'data> {
    type Iter: Iterator<Item = TileDBResult<(String, TypedQueryBuffers<'data>)>>;

    fn tiledb_inputs(&'data self, schema: Rc<Schema>) -> Self::Iter;
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::collection::vec;
    use proptest::prelude::*;

    const MIN_RECORDS: usize = 0;
    const MAX_RECORDS: usize = 1024;

    fn do_input_provider_u64(u64vec: Vec<u64>) {
        let input = u64vec.query_buffers(CellValNum::single(), false).unwrap();
        let (u64in, offsets) =
            (input.data.as_ref(), input.cell_structure.offsets_ref());
        assert!(offsets.is_none());

        let u64out = if u64vec.is_empty() {
            assert!(u64in.is_empty());
            vec![]
        } else {
            unsafe {
                std::slice::from_raw_parts(&u64in[0] as *const u64, u64in.len())
            }
            .to_vec()
        };

        assert_eq!(u64vec, u64out);
    }

    fn do_input_provider_as_slice<S>(slicevec: Vec<S>)
    where
        S: AsSlice,
        Vec<S>: DataProvider<Unit = S::Item>,
    {
        let input = slicevec.query_buffers(CellValNum::Var, false).unwrap();
        let values: &[S::Item] = input.data.as_ref();
        let structure = input.cell_structure;

        assert!(input.validity.is_none());

        assert!(structure.is_var());
        let offsets = structure.unwrap().unwrap().to_vec();

        assert_eq!(slicevec.len() + 1, offsets.len());

        let expected_total_values: usize =
            slicevec.iter().map(|s| s.values().len()).sum();
        assert_eq!(expected_total_values, values.len());

        if slicevec.is_empty() {
            assert!(values.is_empty());
            assert_eq!(offsets, vec![0]);
        } else {
            assert_eq!(slicevec.len(), offsets.windows(2).count());

            for (expected, offset) in slicevec.iter().zip(offsets.windows(2)) {
                assert!(offset[1] >= offset[0]);

                let s = &values[offset[0] as usize..offset[1] as usize];
                assert_eq!(expected.values(), s);
            }
        }
    }

    proptest! {
        #[test]
        fn input_provider_u64(u64vec in vec(any::<u64>(), MIN_RECORDS..=MAX_RECORDS)) {
            do_input_provider_u64(u64vec)
        }

        #[test]
        fn input_provider_strings(
            stringvec in crate::query::buffer::strategy::prop_string_vec(
                (MIN_RECORDS..=MAX_RECORDS).into()
            )
        ) {
            do_input_provider_as_slice(stringvec)
        }

        #[test]
        fn input_provider_u64_vec(u64vecvec in vec(vec(any::<u64>(), MIN_RECORDS..=MAX_RECORDS), MIN_RECORDS..=MAX_RECORDS)) {
            do_input_provider_as_slice(u64vecvec)
        }
    }
}
