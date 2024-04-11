use std::ops::{Deref, DerefMut};

use crate::Result as TileDBResult;

pub trait CAPISameRepr: Copy + Default {}

impl CAPISameRepr for u8 {}
impl CAPISameRepr for u16 {}
impl CAPISameRepr for u32 {}
impl CAPISameRepr for u64 {}
impl CAPISameRepr for i8 {}
impl CAPISameRepr for i16 {}
impl CAPISameRepr for i32 {}
impl CAPISameRepr for i64 {}
impl CAPISameRepr for f32 {}
impl CAPISameRepr for f64 {}

pub trait CAPIConverter {
    type CAPIType: Default + Copy;

    fn to_capi(&self) -> Self::CAPIType;
    fn to_rust(value: &Self::CAPIType) -> Self;
}

impl<T: CAPISameRepr> CAPIConverter for T {
    type CAPIType = Self;

    fn to_capi(&self) -> Self::CAPIType {
        *self
    }

    fn to_rust(value: &Self::CAPIType) -> T {
        *value
    }
}

pub enum Buffer<'data, T = u8> {
    Empty,
    Borrowed(&'data [T]),
    Owned(Box<[T]>),
}

impl<'data, T> Buffer<'data, T> {
    pub fn size(&self) -> usize {
        std::mem::size_of_val(self.as_ref())
    }
}

impl<'data, T> AsRef<[T]> for Buffer<'data, T> {
    fn as_ref(&self) -> &[T] {
        match self {
            Buffer::Empty => unsafe {
                std::slice::from_raw_parts(
                    std::ptr::NonNull::dangling().as_ptr(),
                    0,
                )
            },
            Buffer::Borrowed(data) => data,
            Buffer::Owned(data) => &*data,
        }
    }
}

impl<'data, T> Deref for Buffer<'data, T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

pub struct InputData<'data, T = u8> {
    pub data: Buffer<'data, T>,
    pub cell_offsets: Option<Buffer<'data, u64>>,
}

pub trait DataProvider {
    fn as_tiledb_input(&self) -> InputData;
}

impl<C> DataProvider for Vec<C>
where
    C: CAPISameRepr,
{
    fn as_tiledb_input(&self) -> InputData {
        self.as_slice().as_tiledb_input()
    }
}

impl<C> DataProvider for [C]
where
    C: CAPISameRepr,
{
    fn as_tiledb_input(&self) -> InputData {
        let ptr = self.as_ptr();
        let byte_len = std::mem::size_of_val(self);
        let raw_slice =
            unsafe { std::slice::from_raw_parts(ptr as *const u8, byte_len) };
        InputData {
            data: Buffer::Borrowed(raw_slice),
            cell_offsets: None,
        }
    }
}

impl DataProvider for Vec<&str> {
    fn as_tiledb_input(&self) -> InputData {
        let mut offset_accumulator = 0;
        let offsets = self
            .iter()
            .map(|s| {
                let my_offset = offset_accumulator;
                offset_accumulator += s.len();
                my_offset as u64
            })
            .collect::<Vec<u64>>()
            .into_boxed_slice();

        let mut data = Vec::with_capacity(offset_accumulator);
        self.iter().for_each(|s| {
            data.extend(s.as_bytes());
        });

        InputData {
            data: Buffer::Owned(data.into_boxed_slice()),
            cell_offsets: Some(Buffer::Owned(offsets)),
        }
    }
}

impl DataProvider for Vec<String> {
    fn as_tiledb_input(&self) -> InputData {
        let mut offset_accumulator = 0;
        let offsets = self
            .iter()
            .map(|s| {
                let my_offset = offset_accumulator;
                offset_accumulator += s.len();
                my_offset as u64
            })
            .collect::<Vec<u64>>()
            .into_boxed_slice();

        let mut data = Vec::with_capacity(offset_accumulator);
        self.iter().for_each(|s| {
            data.extend(s.as_bytes());
        });

        InputData {
            data: Buffer::Owned(data.into_boxed_slice()),
            cell_offsets: Some(Buffer::Owned(offsets)),
        }
    }
}

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

pub trait ScratchAllocator<C> {
    type Parameters: Default + Sized;

    fn construct(params: Self::Parameters) -> Self;
    fn scratch_space(&self) -> ScratchSpace<C>;
    fn realloc(&self, old: ScratchSpace<C>) -> ScratchSpace<C>;
}

pub trait HasScratchSpaceStrategy<C> {
    type Strategy: ScratchAllocator<C>;
}

pub trait DataReceiver: Sized {
    type ScratchAllocator: Default + ScratchAllocator<Self::Unit>;
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

pub struct NonVarSized {
    capacity: usize,
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
    type Parameters = Option<usize>;

    fn construct(params: Self::Parameters) -> Self {
        if let Some(capacity) = params {
            NonVarSized { capacity }
        } else {
            Default::default()
        }
    }

    fn scratch_space(&self) -> ScratchSpace<C> {
        ScratchSpace(vec![C::default(); self.capacity].into_boxed_slice(), None)
    }

    fn realloc(&self, old: ScratchSpace<C>) -> ScratchSpace<C> {
        let old_capacity = old.0.len();
        let new_capacity = 2 * (old_capacity + 1);
        ScratchSpace(vec![C::default(); new_capacity].into_boxed_slice(), None)
    }
}

pub struct VarSized {
    byte_capacity: usize,
    offset_capacity: usize,
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
    type Parameters = Option<VarSized>;

    fn construct(params: Self::Parameters) -> Self {
        params.unwrap_or(Default::default())
    }

    fn scratch_space(&self) -> ScratchSpace<C> {
        let data = vec![C::default(); self.byte_capacity].into_boxed_slice();
        let offsets = vec![0u64; self.offset_capacity].into_boxed_slice();
        ScratchSpace(data, Some(offsets))
    }

    fn realloc(&self, old: ScratchSpace<C>) -> ScratchSpace<C> {
        let data = vec![C::default(); 2 * (old.0.len() + 1)].into_boxed_slice();
        let offsets = old
            .1
            .map(|c| vec![0u64; 2 * (c.len() + 1)].into_boxed_slice());
        ScratchSpace(data, offsets)
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
    type ScratchAllocator = NonVarSized;
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

impl DataReceiver for Vec<String> {
    type ScratchAllocator = VarSized;
    type Unit = u8;

    fn receive<'data>(
        &mut self,
        records: usize,
        bytes: usize,
        input: InputData<'data, Self::Unit>,
    ) -> TileDBResult<()> {
        let data_buffer = input.data.as_ref();
        let offset_buffer = input.cell_offsets.as_ref().unwrap();

        for s in 0..records {
            let start = offset_buffer[s] as usize;
            let slen = if s + 1 < records {
                offset_buffer[s + 1] as usize - start
            } else {
                bytes - start
            };

            let s = String::from_utf8_lossy(&data_buffer[start..start + slen]);
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

/// Trait for comparisons based on value bits.
/// This exists to work around float NaN which is not equal to itself,
/// but we want it to be for generic operations with TileDB structures.
/*
 * Fun fact:
 * `impl<T> BitsEq for T where T: Eq` is forbidden in concert with
 * `impl BitsEq for f32` because the compiler says that `std` may
 * `impl Eq for f32` someday. Seems unlikely.
 */
pub trait BitsEq: PartialEq {
    fn bits_eq(&self, other: &Self) -> bool;
}

macro_rules! derive_reflexive_eq {
    ($typename:ty) => {
        impl BitsEq for $typename {
            fn bits_eq(&self, other: &Self) -> bool {
                <Self as PartialEq>::eq(self, other)
            }
        }
    };
}

derive_reflexive_eq!(bool);
derive_reflexive_eq!(u8);
derive_reflexive_eq!(u16);
derive_reflexive_eq!(u32);
derive_reflexive_eq!(u64);
derive_reflexive_eq!(i8);
derive_reflexive_eq!(i16);
derive_reflexive_eq!(i32);
derive_reflexive_eq!(i64);

impl BitsEq for f32 {
    fn bits_eq(&self, other: &Self) -> bool {
        self.to_bits() == other.to_bits()
    }
}

impl BitsEq for f64 {
    fn bits_eq(&self, other: &Self) -> bool {
        self.to_bits() == other.to_bits()
    }
}

impl<T1, T2> BitsEq for (T1, T2)
where
    T1: BitsEq,
    T2: BitsEq,
{
    fn bits_eq(&self, other: &Self) -> bool {
        self.0.bits_eq(&other.0) && self.1.bits_eq(&other.1)
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

    proptest! {
        #[test]
        fn input_provider_u64(u64vec in vec(any::<u64>(), MIN_RECORDS..=MAX_RECORDS)) {
            let input = u64vec.as_tiledb_input();
            let (bytes, offsets) = (input.data.as_ref(), input.cell_offsets);
            assert!(offsets.is_none());

            let u64out = if u64vec.is_empty() {
                assert!(bytes.is_empty());
                vec![]
            } else {
                unsafe {
                    std::slice::from_raw_parts(&bytes[0] as *const u8 as *const u64, bytes.len() / std::mem::size_of::<u64>())
                }.to_vec()
            };

            assert_eq!(u64vec, u64out);
        }
    }

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

    proptest! {
        #[test]
        fn input_provider_strings(stringvec in vec(any::<String>(), MIN_RECORDS..=MAX_RECORDS)) {
            let input = stringvec.as_tiledb_input();
            let (bytes, offsets) = (input.data.as_ref(), input.cell_offsets);
            assert!(offsets.is_some());
            let mut offsets = offsets.unwrap().to_vec();

            assert_eq!(stringvec.len(), offsets.len());

            let expected_total_bytes : usize = stringvec.iter().map(|s| s.len()).sum();
            assert_eq!(expected_total_bytes, bytes.len());

            if stringvec.is_empty() {
                assert!(bytes.is_empty());
            } else {
                assert_eq!(stringvec.len(), offsets.windows(2).count() + 1);

                offsets.push(bytes.len() as u64);

                for (expected, offset) in stringvec.iter().zip(offsets.windows(2)) {
                    assert!(offset[1] >= offset[0]);

                    let slen = (offset[1] - offset[0]) as usize;
                    let s = if slen == 0 {
                        String::from("")
                    } else {
                        let slice = unsafe {
                            std::slice::from_raw_parts(&bytes[offset[0] as usize] as *const u8, slen)
                        };
                        std::str::from_utf8(slice).unwrap().to_string()
                    };
                    assert_eq!(*expected, s);
                }
            }
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
