use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::ops::{Deref, DerefMut};

use crate::convert::CAPISameRepr;
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
            BufferMut::Owned(data) => &*data,
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

pub struct ScratchSpace<C>(pub Box<[C]>, pub Option<Box<[u64]>>);

impl<'data, C> TryFrom<OutputLocation<'data, C>> for ScratchSpace<C> {
    type Error = crate::error::Error;

    fn try_from(value: OutputLocation<'data, C>) -> TileDBResult<Self> {
        let data = match value.data {
            BufferMut::Empty => vec![].into_boxed_slice(),
            BufferMut::Borrowed(_) => return Err(unimplemented!()),
            BufferMut::Owned(d) => d,
        };

        let cell_offsets = if let Some(cell_offsets) = value.cell_offsets {
            Some(match cell_offsets {
                BufferMut::Empty => vec![].into_boxed_slice(),
                BufferMut::Borrowed(_) => return Err(unimplemented!()),
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

pub trait DataReceiver: Sized {
    type Unit: CAPISameRepr;

    fn receive<'data>(
        &mut self,
        records: usize,
        bytes: usize,
        input: InputData<'data, Self::Unit>,
    ) -> TileDBResult<()>;
}

pub trait ReadResult: Sized {
    type Receiver: DataReceiver + Into<Self>;

    fn new_receiver() -> Self::Receiver;
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
            v.resize(2 * v.len(), Default::default());
            v.into_boxed_slice()
        };

        let new_offsets = {
            let mut v = old_offsets.unwrap().into_vec();
            v.resize(2 * v.len(), Default::default());
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

impl<C> DataReceiver for Vec<C>
where
    C: CAPISameRepr,
{
    type Unit = C;

    fn receive<'data>(
        &mut self,
        records: usize,
        _bytes: usize,
        input: InputData<'data, C>,
    ) -> TileDBResult<()> {
        Ok(if let Buffer::Owned(data) = input.data {
            if self.is_empty() {
                *self = data.into_vec();
                self.truncate(records)
            } else {
                self.extend_from_slice(&data[0..records])
            }
        } else {
            self.extend_from_slice(&input.data.as_ref()[0..records])
        })
    }
}

impl<C> ReadResult for Vec<C>
where
    C: CAPISameRepr,
{
    type Receiver = Self;

    fn new_receiver() -> Self::Receiver {
        vec![]
    }
}

impl HasScratchSpaceStrategy<u8> for Vec<String> {
    type Strategy = VarSized;
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
            Err(unimplemented!())
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
            &*(self.location.data.borrow() as *const [C]) as &'data [C]
        };
        let offset_buffer =
            self.location.cell_offsets.as_ref().unwrap().borrow();

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

impl DataReceiver for Vec<String> {
    type Unit = u8;

    fn receive<'data>(
        &mut self,
        records: usize,
        bytes: usize,
        input: InputData<'data, Self::Unit>,
    ) -> TileDBResult<()> {
        for s in VarDataIterator::new(records, bytes, &input)? {
            let s = String::from_utf8_lossy(s);
            self.push(s.to_string());
        }

        Ok(())
    }
}

impl ReadResult for Vec<String> {
    type Receiver = Self;

    fn new_receiver() -> Self::Receiver {
        vec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::collection::vec;
    use proptest::prelude::*;

    const MIN_RECORDS: usize = 0;
    const MAX_RECORDS: usize = 1024;

    const MIN_BYTE_CAPACITY: usize = 0;
    const MAX_BYTE_CAPACITY: usize = 1024 * 1024;

    fn do_read_result_repr<C>(dst_unit_capacity: usize, unitsrc: Vec<C>)
    where
        C: CAPISameRepr + std::fmt::Debug + PartialEq,
    {
        let alloc = <NonVarSized as ScratchAllocator<C>>::construct(Some(
            dst_unit_capacity,
        ));

        let mut scratch_space = alloc.scratch_space();

        let mut unitdst = <Vec<C> as ReadResult>::new_receiver();

        while unitdst.len() < unitsrc.len() {
            let ncells = std::cmp::min(
                scratch_space.0.len(),
                unitsrc.len() - unitdst.len(),
            );
            if ncells == 0 {
                scratch_space = alloc.realloc(scratch_space);
                continue;
            }

            unsafe {
                std::ptr::copy_nonoverlapping::<C>(
                    unitsrc[unitdst.len()..unitsrc.len()].as_ptr(),
                    scratch_space.0.as_mut_ptr(),
                    ncells,
                )
            };

            let input_data = InputData {
                data: Buffer::Borrowed(&*scratch_space.0),
                cell_offsets: None,
            };

            let prev_len = unitdst.len();

            <Vec<C> as DataReceiver>::receive(
                &mut unitdst,
                ncells,
                ncells * std::mem::size_of::<u64>(),
                input_data,
            )
            .expect("Error aggregating data into Vec");

            assert_eq!(ncells, unitdst.len() - prev_len);
            assert_eq!(unitsrc[0..unitdst.len()], unitdst);
        }

        assert_eq!(unitsrc, unitdst);
    }

    proptest! {
        #[test]
        fn read_result_u64(dst_unit_capacity in MIN_RECORDS..=MAX_RECORDS, unitsrc in vec(any::<u64>(), MIN_RECORDS..=MAX_RECORDS)) {
            do_read_result_repr::<u64>(dst_unit_capacity, unitsrc)
        }

        #[test]
        fn read_result_u32(dst_unit_capacity in MIN_RECORDS..=MAX_RECORDS, unitsrc in vec(any::<u32>(), MIN_RECORDS..=MAX_RECORDS)) {
            do_read_result_repr::<u32>(dst_unit_capacity, unitsrc)
        }

        #[test]
        fn read_result_u16(dst_unit_capacity in MIN_RECORDS..=MAX_RECORDS, unitsrc in vec(any::<u16>(), MIN_RECORDS..=MAX_RECORDS)) {
            do_read_result_repr::<u16>(dst_unit_capacity, unitsrc)
        }

        #[test]
        fn read_result_u8(dst_unit_capacity in MIN_RECORDS..=MAX_RECORDS, unitsrc in vec(any::<u8>(), MIN_RECORDS..=MAX_RECORDS)) {
            do_read_result_repr::<u8>(dst_unit_capacity, unitsrc)
        }

        #[test]
        fn read_result_f64(dst_unit_capacity in MIN_RECORDS..=MAX_RECORDS, unitsrc in vec(any::<f64>(), MIN_RECORDS..=MAX_RECORDS)) {
            do_read_result_repr::<f64>(dst_unit_capacity, unitsrc)
        }

        #[test]
        fn read_result_f32(dst_unit_capacity in MIN_RECORDS..=MAX_RECORDS, unitsrc in vec(any::<f32>(), MIN_RECORDS..=MAX_RECORDS)) {
            do_read_result_repr::<f32>(dst_unit_capacity, unitsrc)
        }
    }

    fn do_read_result_strings(
        record_capacity: usize,
        byte_capacity: usize,
        stringsrc: Vec<String>,
    ) {
        let alloc = VarSized {
            byte_capacity,
            offset_capacity: record_capacity,
        };

        let mut scratch_space = alloc.scratch_space();

        let mut stringdst: Vec<String> = vec![];

        while stringdst.len() < stringsrc.len() {
            /* copy from stringsrc to scratch data */
            let (nrecords, nbytes) = {
                /* write the offsets first */
                let (nrecords, nbytes) = {
                    let mut scratch_offsets = scratch_space.1.as_mut().unwrap();
                    let mut i = 0;
                    let mut off = 0;
                    let mut src =
                        stringsrc[stringdst.len()..stringsrc.len()].iter();
                    loop {
                        if i >= scratch_offsets.len() {
                            break (i, off);
                        }
                        if let Some(src) = src.next() {
                            if off + src.len() <= scratch_space.0.len() {
                                scratch_offsets[i] = off as u64;
                                off += src.len();
                            } else {
                                break (i, off);
                            }
                        } else {
                            break (i, off);
                        }
                        i += 1;
                    }
                };

                if nrecords == 0 {
                    assert_eq!(0, nbytes);
                    scratch_space = alloc.realloc(scratch_space);
                    continue;
                }

                let scratch_offsets = scratch_space.1.as_ref().unwrap();

                /* then transfer contents */
                for i in 0..nrecords {
                    let s = &stringsrc[stringdst.len() + i];
                    let start = scratch_offsets[i] as usize;
                    let end = if i + 1 < nrecords {
                        scratch_offsets[i + 1] as usize
                    } else {
                        nbytes
                    };
                    scratch_space.0[start..end].copy_from_slice(s.as_bytes())
                }

                (nrecords, nbytes)
            };

            /* then copy from scratch data to stringdst */
            let prev_len = stringdst.len();
            let input = InputData {
                data: Buffer::Borrowed(&scratch_space.0),
                cell_offsets: scratch_space
                    .1
                    .as_ref()
                    .map(|c| Buffer::Borrowed(c)),
            };
            stringdst
                .receive(nrecords, nbytes, input)
                .expect("Error aggregating Vec<String>");

            assert_eq!(nrecords, stringdst.len() - prev_len);
            assert_eq!(stringsrc[0..stringdst.len()], stringdst);
        }
    }

    proptest! {
        #[test]
        fn read_result_strings(record_capacity in MIN_RECORDS..=MAX_RECORDS, byte_capacity in MIN_BYTE_CAPACITY..=MAX_BYTE_CAPACITY, stringsrc in vec(any::<String>(), MIN_RECORDS..=MAX_RECORDS))
        {
            do_read_result_strings(record_capacity, byte_capacity, stringsrc)
        }
    }
}
