use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::iter::FusedIterator;

use anyhow::anyhow;

use crate::convert::CAPISameRepr;
use crate::error::{DatatypeErrorKind, Error};
use crate::query::buffer::{BufferMut, QueryBuffers, QueryBuffersMut};
use crate::Result as TileDBResult;

pub struct RawReadOutput<'data, C> {
    pub nvalues: usize,
    pub nbytes: usize,
    pub input: &'data QueryBuffers<'data, C>,
}

pub struct ScratchSpace<C>(
    pub Box<[C]>,
    pub Option<Box<[u64]>>,
    pub Option<Box<[u8]>>,
);

impl<'data, C> TryFrom<QueryBuffersMut<'data, C>> for ScratchSpace<C> {
    type Error = crate::error::Error;

    fn try_from(value: QueryBuffersMut<'data, C>) -> TileDBResult<Self> {
        let data = match value.data {
            BufferMut::Empty => vec![].into_boxed_slice(),
            BufferMut::Borrowed(_) => {
                return Err(Error::InvalidArgument(anyhow!(
                    "Cannot convert borrowed data into owned scratch space"
                )))
            }
            BufferMut::Owned(d) => d,
        };

        let cell_offsets = if let Some(cell_offsets) = value.cell_offsets {
            Some(match cell_offsets {
                BufferMut::Empty => vec![].into_boxed_slice(),
                BufferMut::Borrowed(_) => return Err(Error::InvalidArgument(
                        anyhow!("Cannot convert borrowed offsets buffer into owned scratch space"))),
                BufferMut::Owned(d) => d,
            })
        } else {
            None
        };

        let validity = if let Some(validity) = value.validity {
            Some(match validity {
                BufferMut::Empty => vec![].into_boxed_slice(),
                BufferMut::Borrowed(_) => return Err(Error::InvalidArgument(
                    anyhow!("Cannot convert borrowed validity buffer into owned scratch space"))),
                BufferMut::Owned(d) => d,
            })
        } else {
            None
        };

        Ok(ScratchSpace(data, cell_offsets, validity))
    }
}

impl<'data, C> From<ScratchSpace<C>> for QueryBuffersMut<'data, C> {
    fn from(value: ScratchSpace<C>) -> Self {
        QueryBuffersMut {
            data: BufferMut::Owned(value.0),
            cell_offsets: value.1.map(BufferMut::Owned),
            validity: value.2.map(BufferMut::Owned),
        }
    }
}

pub trait ScratchAllocator<C>: Default {
    fn alloc(&self) -> ScratchSpace<C>;
    fn realloc(&self, old: ScratchSpace<C>) -> ScratchSpace<C>;
}

pub trait HasScratchSpaceStrategy<C> {
    type Strategy: ScratchAllocator<C>;
}

#[derive(Clone, Debug)]
pub struct NonVarSized {
    pub capacity: usize,
}

impl Default for NonVarSized {
    fn default() -> Self {
        NonVarSized {
            capacity: 1024 * 1024,
        }
    }
}

impl<C> ScratchAllocator<C> for NonVarSized
where
    C: CAPISameRepr,
{
    fn alloc(&self) -> ScratchSpace<C> {
        ScratchSpace(
            vec![C::default(); self.capacity].into_boxed_slice(),
            None,
            None,
        )
    }

    fn realloc(&self, old: ScratchSpace<C>) -> ScratchSpace<C> {
        let ScratchSpace(old, _, _) = old;

        let old_capacity = old.len();
        let new_capacity = 2 * (old_capacity + 1);

        let new_data = {
            let mut v = old.to_vec();
            v.resize(new_capacity, Default::default());
            v.into_boxed_slice()
        };

        ScratchSpace(new_data, None, None)
    }
}

#[derive(Clone, Debug)]
pub struct NullableNonVarSized {
    pub data_capacity: usize,
    pub validity_capacity: usize,
}

impl Default for NullableNonVarSized {
    fn default() -> Self {
        NullableNonVarSized {
            data_capacity: 1024 * 1024,
            validity_capacity: 1024 * 1024,
        }
    }
}

impl<C> ScratchAllocator<C> for NullableNonVarSized
where
    C: CAPISameRepr,
{
    fn alloc(&self) -> ScratchSpace<C> {
        ScratchSpace(
            vec![C::default(); self.data_capacity].into_boxed_slice(),
            None,
            Some(vec![0u8; self.validity_capacity].into_boxed_slice()),
        )
    }

    fn realloc(&self, old: ScratchSpace<C>) -> ScratchSpace<C> {
        let ScratchSpace(old_data, _, old_validity) = old;

        let new_data = {
            let mut v = old_data.to_vec();
            v.resize(2 * v.len() + 1, Default::default());
            v.into_boxed_slice()
        };

        let new_validity = {
            let mut v = old_validity.unwrap().to_vec();
            v.resize(2 * v.len() + 1, 0u8);
            v.into_boxed_slice()
        };

        ScratchSpace(new_data, None, Some(new_validity))
    }
}

#[derive(Clone, Debug)]
pub struct VarSized {
    pub byte_capacity: usize,
    pub offset_capacity: usize,
}

impl Default for VarSized {
    fn default() -> Self {
        const DEFAULT_BYTE_CAPACITY: usize = 1024 * 1024;
        const DEFAULT_RECORD_CAPACITY: usize = 256 * 1024;

        VarSized {
            byte_capacity: DEFAULT_BYTE_CAPACITY,
            offset_capacity: DEFAULT_RECORD_CAPACITY,
        }
    }
}

impl<C> ScratchAllocator<C> for VarSized
where
    C: CAPISameRepr,
{
    fn alloc(&self) -> ScratchSpace<C> {
        let data = vec![C::default(); self.byte_capacity].into_boxed_slice();
        let offsets = vec![0u64; self.offset_capacity].into_boxed_slice();
        ScratchSpace(data, Some(offsets), None)
    }

    fn realloc(&self, old: ScratchSpace<C>) -> ScratchSpace<C> {
        let ScratchSpace(old_data, old_offsets, _) = old;

        let new_data = {
            let mut v = old_data.to_vec();
            v.resize(2 * v.len() + 1, Default::default());
            v.into_boxed_slice()
        };

        let new_offsets = {
            let mut v = old_offsets.unwrap().into_vec();
            v.resize(2 * v.len() + 1, Default::default());
            v.into_boxed_slice()
        };

        ScratchSpace(new_data, Some(new_offsets), None)
    }
}

#[derive(Clone, Debug)]
pub struct NullableVarSized {
    pub byte_capacity: usize,
    pub offset_capacity: usize,
    pub validity_capacity: usize,
}

impl Default for NullableVarSized {
    fn default() -> Self {
        const DEFAULT_BYTE_CAPACITY: usize = 1024 * 1024;
        const DEFAULT_RECORD_CAPACITY: usize = 256 * 1024;

        NullableVarSized {
            byte_capacity: DEFAULT_BYTE_CAPACITY,
            offset_capacity: DEFAULT_RECORD_CAPACITY,
            validity_capacity: DEFAULT_RECORD_CAPACITY,
        }
    }
}

impl<C> ScratchAllocator<C> for NullableVarSized
where
    C: CAPISameRepr,
{
    fn alloc(&self) -> ScratchSpace<C> {
        let data = vec![C::default(); self.byte_capacity].into_boxed_slice();
        let offsets = vec![0u64; self.offset_capacity].into_boxed_slice();
        let validity = vec![0u8; self.validity_capacity].into_boxed_slice();
        ScratchSpace(data, Some(offsets), Some(validity))
    }

    fn realloc(&self, old: ScratchSpace<C>) -> ScratchSpace<C> {
        let ScratchSpace(old_data, old_offsets, old_validity) = old;

        let new_data = {
            let mut v = old_data.to_vec();
            v.resize(2 * v.len() + 1, Default::default());
            v.into_boxed_slice()
        };

        let new_offsets = {
            let mut v = old_offsets.unwrap().into_vec();
            v.resize(2 * v.len() + 1, Default::default());
            v.into_boxed_slice()
        };

        let new_validity = {
            let mut v = old_validity.unwrap().into_vec();
            v.resize(2 * v.len() + 1, Default::default());
            v.into_boxed_slice()
        };

        ScratchSpace(new_data, Some(new_offsets), Some(new_validity))
    }
}

impl<C> HasScratchSpaceStrategy<C> for Vec<C>
where
    C: CAPISameRepr,
{
    type Strategy = NonVarSized;
}

impl<C> HasScratchSpaceStrategy<C> for (Vec<C>, Vec<u8>)
where
    C: CAPISameRepr,
{
    type Strategy = NullableNonVarSized;
}

impl HasScratchSpaceStrategy<u8> for Vec<String> {
    type Strategy = VarSized;
}

impl HasScratchSpaceStrategy<u8> for (Vec<String>, Vec<u8>) {
    type Strategy = NullableVarSized;
}

pub struct FixedDataIterator<'data, C> {
    fixed: std::slice::Iter<'data, C>,
}

impl<'data, C> Iterator for FixedDataIterator<'data, C>
where
    C: Copy,
{
    type Item = C;
    fn next(&mut self) -> Option<Self::Item> {
        self.fixed.next().copied()
    }
}

impl<'data, C> FusedIterator for FixedDataIterator<'data, C> where C: Copy {}

impl<'data, C> TryFrom<RawReadOutput<'data, C>>
    for FixedDataIterator<'data, C>
{
    type Error = crate::error::Error;
    fn try_from(value: RawReadOutput<'data, C>) -> TileDBResult<Self> {
        if value.input.cell_offsets.is_some() {
            Err(Error::Datatype(DatatypeErrorKind::ExpectedFixedSize(None)))
        } else {
            Ok(FixedDataIterator {
                fixed: value.input.data.as_ref()[0..value.nvalues].iter(),
            })
        }
    }
}

/// Iterator which yields variable-sized records from a raw read result.
pub struct VarDataIterator<'data, C> {
    nvalues: usize,
    nbytes: usize,
    offset_cursor: usize,
    location: QueryBuffers<'data, C>,
}

impl<'data, C> VarDataIterator<'data, C> {
    pub fn new(
        nvalues: usize,
        nbytes: usize,
        location: &'data QueryBuffers<'data, C>,
    ) -> TileDBResult<Self> {
        let location = location.borrow();

        if location.cell_offsets.is_none() {
            Err(Error::Datatype(DatatypeErrorKind::ExpectedVarSize(
                None, None,
            )))
        } else {
            Ok(VarDataIterator {
                nvalues,
                nbytes,
                offset_cursor: 0,
                location,
            })
        }
    }
}

impl<'data, C> Debug for VarDataIterator<'data, C>
where
    C: Debug,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(
            f,
            "VarDataIterator {{ cursor: {}, offsets: {:?}, bytes: {:?} }}",
            self.offset_cursor,
            &self.location.cell_offsets.as_ref().unwrap().as_ref()
                [0..self.nvalues],
            &self.location.data.as_ref()[0..self.nbytes]
        )
    }
}

impl<'data, C> Iterator for VarDataIterator<'data, C> {
    type Item = &'data [C];

    fn next(&mut self) -> Option<Self::Item> {
        let data_buffer: &'data [C] = unsafe {
            // `self.location.data.borrow()` borrows self, so even though the method
            // nominally returns a 'data lifetime, it is shortened to 'this.
            // And if `self.location.data` were a `Buffer::Owned`, then the returned
            // item actually would be invalid due to dropping self.
            // But the construction of the iterator via `new` removes the possibility
            // of `Buffer::Owned`, so this transmutation to the longer 'data lifetime
            // is safe.
            &*(self.location.data.as_ref() as *const [C]) as &'data [C]
        };
        let offset_buffer =
            self.location.cell_offsets.as_ref().unwrap().as_ref();

        let s = self.offset_cursor;
        self.offset_cursor += 1;

        if s + 1 < self.nvalues {
            let start = offset_buffer[s] as usize;
            let slen = offset_buffer[s + 1] as usize - start;
            Some(&data_buffer[start..start + slen])
        } else if s < self.nvalues {
            let start = offset_buffer[s] as usize;
            let slen = self.nbytes - start;
            Some(&data_buffer[start..start + slen])
        } else {
            None
        }
    }
}

impl<'data, C> FusedIterator for VarDataIterator<'data, C> {}

impl<'data, C> TryFrom<RawReadOutput<'data, C>> for VarDataIterator<'data, C> {
    type Error = crate::error::Error;
    fn try_from(value: RawReadOutput<'data, C>) -> TileDBResult<Self> {
        Self::new(value.nvalues, value.nbytes, value.input)
    }
}

pub struct Utf8LossyIterator<'data> {
    var: VarDataIterator<'data, u8>,
}

impl<'data> TryFrom<RawReadOutput<'data, u8>> for Utf8LossyIterator<'data> {
    type Error = crate::error::Error;
    fn try_from(value: RawReadOutput<'data, u8>) -> TileDBResult<Self> {
        Ok(Utf8LossyIterator {
            var: VarDataIterator::try_from(value)?,
        })
    }
}

impl<'data> Iterator for Utf8LossyIterator<'data> {
    type Item = String;
    fn next(&mut self) -> Option<Self::Item> {
        self.var
            .next()
            .map(|s| String::from_utf8_lossy(s).to_string())
    }
}

impl<'data> FusedIterator for Utf8LossyIterator<'data> {}

pub trait FromQueryOutput: Sized {
    type Unit;
    type Iterator<'data>: Iterator<Item = Self>
        + TryFrom<RawReadOutput<'data, Self::Unit>, Error = crate::error::Error>
    where
        Self::Unit: 'data;
}

impl<C> FromQueryOutput for C
where
    C: CAPISameRepr,
{
    type Unit = C;
    type Iterator<'data> = FixedDataIterator<'data, Self::Unit> where C: 'data;
}

impl FromQueryOutput for String {
    type Unit = u8;
    type Iterator<'data> = Utf8LossyIterator<'data>;
}
