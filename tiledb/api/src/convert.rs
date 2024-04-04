use std::ops::Deref;

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
    Borrowed(&'data [T]),
    Owned(Box<[T]>),
}

impl<'data, T> AsRef<[T]> for Buffer<'data, T> {
    fn as_ref(&self) -> &[T] {
        match self {
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

pub trait InputConverter {
    fn for_tiledb(&self) -> InputData;
}

impl<C> InputConverter for Vec<C>
where
    C: CAPISameRepr,
{
    fn for_tiledb(&self) -> InputData {
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

impl InputConverter for Vec<String> {
    fn for_tiledb(&self) -> InputData {
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
    use proptest::prelude::*;

    const MIN_CELLS: usize = 0;
    const MAX_CELLS: usize = 1024;

    proptest! {
        #[test]
        fn input_converter_u64(u64vec in proptest::collection::vec(any::<u64>(), MIN_CELLS..=MAX_CELLS)) {
            let input = u64vec.for_tiledb();
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

    proptest! {
        #[test]
        fn input_converter_strings(stringvec in proptest::collection::vec(any::<String>(), MIN_CELLS..=MAX_CELLS)) {
            let input = stringvec.for_tiledb();
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

                /* TODO probably have a trait for converting back at some point */
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
}
