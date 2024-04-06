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

pub struct InputData<'data> {
    pub data: Buffer<'data>,
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
        let ptr = self.as_ptr();
        let byte_len = self.len() * std::mem::size_of::<C>();
        let raw_slice =
            unsafe { std::slice::from_raw_parts(ptr as *const u8, byte_len) };
        InputData {
            data: Buffer::Borrowed(raw_slice),
            cell_offsets: None,
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

pub struct ReadResult<'this, 'data, T = u8> {
    buffers: &'this mut OutputLocation<'data, T>,
    records: usize,
    bytes: usize,
}

pub trait DataCollector<'data, T = u8>: Sized
where
    T: CAPISameRepr,
{
    type Parameters: Default;

    /// Allocate memory for raw TileDB-format daata
    fn prepare(parameters: Self::Parameters) -> OutputLocation<'data, T>;

    /// Create an instance of this type from the
    fn construct<'this>(
        receiver: ReadResult<'this, 'data, T>,
    ) -> TileDBResult<Self>;
}

impl<'data, C> DataCollector<'data, C> for Vec<C>
where
    C: 'data + CAPISameRepr,
{
    type Parameters = Option<usize>;

    fn prepare(parameters: Self::Parameters) -> OutputLocation<'data, C> {
        const DEFAULT_CAPACITY: usize = 1024;

        let capacity = if let Some(capacity) = parameters {
            capacity
        } else {
            DEFAULT_CAPACITY
        };

        let data = vec![C::default(); capacity].into_boxed_slice();

        OutputLocation {
            data: BufferMut::Owned(data),
            cell_offsets: None,
        }
    }

    fn construct<'this>(
        receiver: ReadResult<'this, 'data, C>,
    ) -> TileDBResult<Self> {
        let mut v = match std::mem::replace(
            &mut receiver.buffers.data,
            BufferMut::Empty,
        ) {
            BufferMut::Empty => unreachable!(),
            BufferMut::Borrowed(_) => unreachable!(),
            BufferMut::Owned(slice) => slice.into_vec(),
        };
        v.truncate(receiver.records);
        Ok(v)
    }
}

impl<'data> DataCollector<'data, u8> for Vec<String> {
    type Parameters = (Option<usize>, Option<usize>);

    fn prepare(parameters: Self::Parameters) -> OutputLocation<'data, u8> {
        const DEFAULT_RECORD_CAPACITY: usize = 256 * 1024;
        const DEFAULT_BYTE_CAPACITY: usize = 1024 * 1024;

        let record_capacity = parameters.0.unwrap_or(DEFAULT_RECORD_CAPACITY);
        let byte_capacity = parameters.1.unwrap_or(DEFAULT_BYTE_CAPACITY);

        let data_buffer: Box<[u8]> = vec![0; byte_capacity].into_boxed_slice();
        let offset_buffer: Box<[u64]> =
            vec![0; record_capacity].into_boxed_slice();

        OutputLocation {
            data: BufferMut::Owned(data_buffer),
            cell_offsets: Some(BufferMut::Owned(offset_buffer)),
        }
    }

    fn construct<'this>(
        receiver: ReadResult<'this, 'data, u8>,
    ) -> TileDBResult<Self> {
        let mut results: Vec<String> = vec![];

        let offset_buffer = receiver.buffers.cell_offsets.as_mut().unwrap();

        for s in 0..receiver.records {
            let start = offset_buffer[s] as usize;
            let slen = if s + 1 < receiver.records {
                offset_buffer[s + 1] as usize - start
            } else {
                receiver.bytes - start
            };

            let s = String::from_utf8_lossy(
                &receiver.buffers.data[start..start + slen],
            );
            results.push(s.to_string());
        }

        Ok(results)
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

    fn do_output_collector_repr<C>(dst_unit_capacity: usize, unitsrc: Vec<C>)
    where
        C: CAPISameRepr + std::fmt::Debug + PartialEq,
    {
        let ncells = std::cmp::min(dst_unit_capacity, unitsrc.len());

        let unitdst = {
            let mut output =
                <Vec<C> as DataCollector<C>>::prepare(Some(dst_unit_capacity));
            let (unitdst, offsets) =
                (output.data.as_mut(), output.cell_offsets.as_ref());
            assert!(offsets.is_none());

            unsafe {
                std::ptr::copy_nonoverlapping::<C>(
                    unitsrc.as_ptr(),
                    unitdst.as_mut_ptr(),
                    ncells,
                )
            }
            <Vec<C> as DataCollector<C>>::construct(ReadResult {
                buffers: &mut output,
                records: ncells,
                bytes: ncells * std::mem::size_of::<u64>(),
            })
        }
        .unwrap();

        assert_eq!(ncells, unitdst.len());
        assert_eq!(dst_unit_capacity, unitdst.capacity());
        assert_eq!(unitdst[0..ncells], unitsrc[0..ncells]);
    }

    proptest! {
        #[test]
        fn output_collector_u64(dst_unit_capacity in MIN_RECORDS..=MAX_RECORDS, unitsrc in vec(any::<u64>(), MIN_RECORDS..=MAX_RECORDS)) {
            do_output_collector_repr::<u64>(dst_unit_capacity, unitsrc)
        }

        #[test]
        fn output_collector_u32(dst_unit_capacity in MIN_RECORDS..=MAX_RECORDS, unitsrc in vec(any::<u32>(), MIN_RECORDS..=MAX_RECORDS)) {
            do_output_collector_repr::<u32>(dst_unit_capacity, unitsrc)
        }

        #[test]
        fn output_collector_u16(dst_unit_capacity in MIN_RECORDS..=MAX_RECORDS, unitsrc in vec(any::<u16>(), MIN_RECORDS..=MAX_RECORDS)) {
            do_output_collector_repr::<u16>(dst_unit_capacity, unitsrc)
        }

        #[test]
        fn output_collector_u8(dst_unit_capacity in MIN_RECORDS..=MAX_RECORDS, unitsrc in vec(any::<u8>(), MIN_RECORDS..=MAX_RECORDS)) {
            do_output_collector_repr::<u8>(dst_unit_capacity, unitsrc)
        }

        #[test]
        fn output_collector_f64(dst_unit_capacity in MIN_RECORDS..=MAX_RECORDS, unitsrc in vec(any::<f64>(), MIN_RECORDS..=MAX_RECORDS)) {
            do_output_collector_repr::<f64>(dst_unit_capacity, unitsrc)
        }

        #[test]
        fn output_collector_f32(dst_unit_capacity in MIN_RECORDS..=MAX_RECORDS, unitsrc in vec(any::<f32>(), MIN_RECORDS..=MAX_RECORDS)) {
            do_output_collector_repr::<f32>(dst_unit_capacity, unitsrc)
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

    fn do_output_collector_strings(
        record_capacity: usize,
        byte_capacity: usize,
        stringsrc: Vec<String>,
    ) {
        let (stringdst, nrecords, nbytes) = {
            let mut output = <Vec<String> as DataCollector>::prepare((
                Some(record_capacity),
                Some(byte_capacity),
            ));

            let (u8dst, offsets) =
                (output.data.as_mut(), output.cell_offsets.as_mut());
            assert!(offsets.is_some());
            let offsets = offsets.unwrap();

            /* write the offsets first */
            let (nrecords, nbytes) = {
                let mut i = 0;
                let mut off = 0;
                let mut src = stringsrc.iter();
                loop {
                    if i >= offsets.len() {
                        break (i, off);
                    }
                    if let Some(src) = src.next() {
                        if off + src.len() <= u8dst.len() {
                            offsets[i] = off as u64;
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

            /* then transfer contents */
            for i in 0..nrecords {
                let s = &stringsrc[i];
                let start = offsets[i] as usize;
                let end = if i + 1 < nrecords {
                    offsets[i + 1] as usize
                } else {
                    nbytes
                };
                u8dst[start..end].copy_from_slice(s.as_bytes())
            }

            (
                <Vec<String> as DataCollector>::construct(ReadResult {
                    buffers: &mut output,
                    records: nrecords,
                    bytes: nbytes,
                })
                .unwrap(),
                nrecords,
                nbytes,
            )
        };

        let dstbytes: usize = stringdst.iter().map(|s| s.len()).sum();
        let srcbytes: usize = stringsrc.iter().map(|s| s.len()).sum();

        let srccopyable: usize = {
            let mut acc = 0;
            stringsrc
                .iter()
                .take_while(|s| {
                    acc += s.len();
                    acc <= byte_capacity
                })
                .map(|s| s.len())
                .sum()
        };

        assert!(dstbytes <= srcbytes);
        assert_eq!(nbytes, dstbytes);

        assert!(stringdst.len() <= stringsrc.len());
        assert_eq!(nrecords, stringdst.len());

        if srcbytes < byte_capacity {
            assert_eq!(
                std::cmp::min(record_capacity, stringsrc.len()),
                stringdst.len()
            );
        }
        if stringsrc.len() < record_capacity {
            assert_eq!(srccopyable, dstbytes);
        }

        for (src, dst) in stringsrc.iter().zip(stringdst.iter()) {
            assert_eq!(src, dst);
        }
    }

    proptest! {
        #[test]
        fn output_collector_strings(record_capacity in MIN_RECORDS..=MAX_RECORDS, byte_capacity in MIN_BYTE_CAPACITY..=MAX_BYTE_CAPACITY, stringsrc in vec(any::<String>(), MIN_RECORDS..=MAX_RECORDS))
        {
            do_output_collector_strings(record_capacity, byte_capacity, stringsrc)
        }
    }
}
