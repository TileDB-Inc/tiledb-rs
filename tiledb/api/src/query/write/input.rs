use std::ops::Deref;

use crate::convert::CAPISameRepr;

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
            Buffer::Owned(data) => data,
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

impl<'data, T> InputData<'data, T> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::collection::vec;
    use proptest::prelude::*;

    const MIN_RECORDS: usize = 0;
    const MAX_RECORDS: usize = 1024;

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
}
