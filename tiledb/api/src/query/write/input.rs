use crate::array::CellValNum;
use crate::convert::CAPISameRepr;
use crate::query::buffer::{Buffer, QueryBuffers, QueryBuffersMut};

pub trait DataProvider {
    type Unit: CAPISameRepr;
    fn as_tiledb_input(
        &self,
        cell_val_num: CellValNum,
        is_nullable: bool,
    ) -> QueryBuffers<Self::Unit>;
}

impl<'data, C> DataProvider for QueryBuffers<'data, C>
where
    C: CAPISameRepr,
{
    type Unit = C;

    fn as_tiledb_input(
        &self,
        _cell_val_num: CellValNum,
        _is_nullable: bool,
    ) -> QueryBuffers<Self::Unit> {
        let ptr = self.data.as_ptr();
        let byte_len = std::mem::size_of_val(&self.data);
        let raw_slice =
            unsafe { std::slice::from_raw_parts(ptr as *const C, byte_len) };
        QueryBuffers {
            data: Buffer::Borrowed(raw_slice),
            cell_offsets: Option::map(self.cell_offsets.as_ref(), |c| {
                Buffer::Borrowed(c.as_ref())
            }),
            validity: Option::map(self.validity.as_ref(), |v| {
                Buffer::Borrowed(v.as_ref())
            }),
        }
    }
}

impl<'data, C> DataProvider for QueryBuffersMut<'data, C>
where
    C: CAPISameRepr,
{
    type Unit = C;

    fn as_tiledb_input(
        &self,
        _cell_val_num: CellValNum,
        _is_nullable: bool,
    ) -> QueryBuffers<Self::Unit> {
        let ptr = self.data.as_ptr();
        let byte_len = std::mem::size_of_val(&self.data);
        let raw_slice =
            unsafe { std::slice::from_raw_parts(ptr as *const C, byte_len) };
        QueryBuffers {
            data: Buffer::Borrowed(raw_slice),
            cell_offsets: Option::map(self.cell_offsets.as_ref(), |c| {
                Buffer::Borrowed(c.as_ref())
            }),
            validity: Option::map(self.validity.as_ref(), |v| {
                Buffer::Borrowed(v.as_ref())
            }),
        }
    }
}

impl<C> DataProvider for Vec<C>
where
    C: CAPISameRepr,
{
    type Unit = C;

    fn as_tiledb_input(
        &self,
        cell_val_num: CellValNum,
        is_nullable: bool,
    ) -> QueryBuffers<Self::Unit> {
        self.as_slice().as_tiledb_input(cell_val_num, is_nullable)
    }
}

impl<C> DataProvider for [C]
where
    C: CAPISameRepr,
{
    type Unit = C;

    fn as_tiledb_input(
        &self,
        _cell_val_num: CellValNum,
        is_nullable: bool,
    ) -> QueryBuffers<Self::Unit> {
        let validity = if is_nullable {
            Some(Buffer::Owned(vec![1u8; self.len()].into_boxed_slice()))
        } else {
            None
        };

        QueryBuffers {
            data: Buffer::Borrowed(self.as_ref()),
            cell_offsets: None,
            validity,
        }
    }
}

impl<C> DataProvider for Vec<Vec<C>>
where
    C: CAPISameRepr,
{
    type Unit = C;

    fn as_tiledb_input(
        &self,
        _cell_val_num: CellValNum,
        is_nullable: bool,
    ) -> QueryBuffers<Self::Unit> {
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
            data.extend(s);
        });

        let validity = if is_nullable {
            Some(Buffer::Owned(vec![1u8; self.len()].into_boxed_slice()))
        } else {
            None
        };

        QueryBuffers {
            data: Buffer::Owned(data.into_boxed_slice()),
            cell_offsets: Some(Buffer::Owned(offsets)),
            validity,
        }
    }
}

impl DataProvider for Vec<&str> {
    type Unit = u8;

    fn as_tiledb_input(
        &self,
        _cell_val_num: CellValNum,
        is_nullable: bool,
    ) -> QueryBuffers<Self::Unit> {
        let mut offset_accumulator = 0;
        let offsets = self
            .iter()
            .map(|s| {
                let my_offset = offset_accumulator;
                offset_accumulator += s.len();
                my_offset as u64
            })
            .collect::<Vec<u64>>();

        let mut data = Vec::with_capacity(offset_accumulator);
        self.iter().for_each(|s| {
            data.extend(s.as_bytes());
        });

        let validity = if is_nullable {
            Some(Buffer::Owned(vec![1u8; offsets.len()].into_boxed_slice()))
        } else {
            None
        };

        QueryBuffers {
            data: Buffer::Owned(data.into_boxed_slice()),
            cell_offsets: Some(Buffer::Owned(offsets.into_boxed_slice())),
            validity,
        }
    }
}

impl DataProvider for Vec<String> {
    type Unit = u8;

    fn as_tiledb_input(
        &self,
        _cell_val_num: CellValNum,
        is_nullable: bool,
    ) -> QueryBuffers<Self::Unit> {
        let mut offset_accumulator = 0;
        let offsets = self
            .iter()
            .map(|s| {
                let my_offset = offset_accumulator;
                offset_accumulator += s.len();
                my_offset as u64
            })
            .collect::<Vec<u64>>();

        let mut data = Vec::with_capacity(offset_accumulator);
        self.iter().for_each(|s| {
            data.extend(s.as_bytes());
        });

        let validity = if is_nullable {
            Some(Buffer::Owned(vec![1u8; offsets.len()].into_boxed_slice()))
        } else {
            None
        };

        QueryBuffers {
            data: Buffer::Owned(data.into_boxed_slice()),
            cell_offsets: Some(Buffer::Owned(offsets.into_boxed_slice())),
            validity,
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
            let input = u64vec.as_tiledb_input(CellValNum::try_from(1).unwrap(), false);
            let (u64in, offsets) = (input.data.as_ref(), input.cell_offsets);
            assert!(offsets.is_none());

            let u64out = if u64vec.is_empty() {
                assert!(u64in.is_empty());
                vec![]
            } else {
                unsafe {
                    std::slice::from_raw_parts(&u64in[0] as *const u64, u64in.len())
                }.to_vec()
            };

            assert_eq!(u64vec, u64out);
        }

        #[test]
        fn input_provider_strings(
            stringvec in crate::query::buffer::strategy::prop_string_vec(
                (MIN_RECORDS..=MAX_RECORDS).into()
            )
        ) {
            let input = stringvec.as_tiledb_input(CellValNum::Var, false);
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
