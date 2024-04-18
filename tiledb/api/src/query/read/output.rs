use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::iter::FusedIterator;
use std::ops::{Deref, DerefMut};

use anyhow::anyhow;

use crate::convert::CAPISameRepr;
use crate::error::{DatatypeErrorKind, Error};
use crate::query::write::input::{Buffer, InputData};
use crate::Result as TileDBResult;

pub enum BufferMut<'data, T = u8> {
    Empty,
    Borrowed(&'data mut [T]),
    Owned(Box<[T]>),
}

impl<'data, T> BufferMut<'data, T> {
    pub fn size(&self) -> usize {
        std::mem::size_of_val(self.as_ref())
    }
}

impl<'data, T> AsRef<[T]> for BufferMut<'data, T> {
    fn as_ref(&self) -> &[T] {
        match self {
            BufferMut::Empty => unsafe {
                std::slice::from_raw_parts(
                    std::ptr::NonNull::dangling().as_ptr(),
                    0,
                )
            },
            BufferMut::Borrowed(data) => data,
            BufferMut::Owned(data) => data,
        }
    }
}

impl<'data, T> AsMut<[T]> for BufferMut<'data, T> {
    fn as_mut(&mut self) -> &mut [T] {
        match self {
            BufferMut::Empty => unsafe {
                std::slice::from_raw_parts_mut(
                    std::ptr::NonNull::dangling().as_ptr(),
                    0,
                )
            },
            BufferMut::Borrowed(data) => data,
            BufferMut::Owned(data) => &mut *data,
        }
    }
}

impl<'data, T> Deref for BufferMut<'data, T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<'data, T> DerefMut for BufferMut<'data, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

pub struct OutputLocation<'data, T = u8> {
    pub data: BufferMut<'data, T>,
    pub cell_offsets: Option<BufferMut<'data, u64>>,
}

impl<'data, T> OutputLocation<'data, T> {
    pub fn borrow<'this>(&'this self) -> InputData<'data, T>
    where
        'this: 'data,
    {
        InputData {
            data: Buffer::Borrowed(self.data.as_ref()),
            cell_offsets: Option::map(self.cell_offsets.as_ref(), |c| {
                Buffer::Borrowed(c.as_ref())
            }),
        }
    }

    pub fn borrow_mut<'this>(&'this mut self) -> OutputLocation<'data, T>
    where
        'this: 'data,
    {
        OutputLocation {
            data: BufferMut::Borrowed(self.data.as_mut()),
            cell_offsets: Option::map(self.cell_offsets.as_mut(), |c| {
                BufferMut::Borrowed(c.as_mut())
            }),
        }
    }
}

pub struct RawReadOutput<'data, C> {
    pub nrecords: usize,
    pub nbytes: usize,
    pub input: &'data InputData<'data, C>,
}

pub struct ScratchSpace<C>(pub Box<[C]>, pub Option<Box<[u64]>>);

impl<'data, C> TryFrom<OutputLocation<'data, C>> for ScratchSpace<C> {
    type Error = crate::error::Error;

    fn try_from(value: OutputLocation<'data, C>) -> TileDBResult<Self> {
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

        Ok(ScratchSpace(data, cell_offsets))
    }
}

impl<'data, C> From<ScratchSpace<C>> for OutputLocation<'data, C> {
    fn from(value: ScratchSpace<C>) -> Self {
        OutputLocation {
            data: BufferMut::Owned(value.0),
            cell_offsets: value.1.map(BufferMut::Owned),
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
        ScratchSpace(vec![C::default(); self.capacity].into_boxed_slice(), None)
    }

    fn realloc(&self, old: ScratchSpace<C>) -> ScratchSpace<C> {
        let ScratchSpace(old, _) = old;

        let old_capacity = old.len();
        let new_capacity = 2 * (old_capacity + 1);

        let new_data = {
            let mut v = old.to_vec();
            v.resize(new_capacity, Default::default());
            v.into_boxed_slice()
        };

        ScratchSpace(new_data, None)
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
        ScratchSpace(data, Some(offsets))
    }

    fn realloc(&self, old: ScratchSpace<C>) -> ScratchSpace<C> {
        let ScratchSpace(old_data, old_offsets) = old;

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

        ScratchSpace(new_data, Some(new_offsets))
    }
}

impl<C> HasScratchSpaceStrategy<C> for Vec<C>
where
    C: CAPISameRepr,
{
    type Strategy = NonVarSized;
}

impl HasScratchSpaceStrategy<u8> for Vec<String> {
    type Strategy = VarSized;
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
                fixed: value.input.data.as_ref()[0..value.nrecords].iter(),
            })
        }
    }
}

/// Iterator which yields variable-sized records from a raw read result.
pub struct VarDataIterator<'data, C> {
    nrecords: usize,
    nbytes: usize,
    offset_cursor: usize,
    location: InputData<'data, C>,
}

impl<'data, C> VarDataIterator<'data, C> {
    pub fn new(
        nrecords: usize,
        nbytes: usize,
        location: &'data InputData<'data, C>,
    ) -> TileDBResult<Self> {
        let location = location.borrow();

        if location.cell_offsets.is_none() {
            Err(Error::Datatype(DatatypeErrorKind::ExpectedVarSize(
                None, None,
            )))
        } else {
            Ok(VarDataIterator {
                nrecords,
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
                [0..self.nrecords],
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

        if s + 1 < self.nrecords {
            let start = offset_buffer[s] as usize;
            let slen = offset_buffer[s + 1] as usize - start;
            Some(&data_buffer[start..start + slen])
        } else if s < self.nrecords {
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
        Self::new(value.nrecords, value.nbytes, value.input)
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
