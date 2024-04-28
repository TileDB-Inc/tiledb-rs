use std::collections::HashSet;

use anyhow::anyhow;

use crate::convert::CAPISameRepr;
use crate::error::Error;
use crate::Result as TileDBResult;

/// A write buffer is used by WriterQuery to pass data to TileDB. As such
/// it's name is a bit of an oxymoron as we only ever read from a WriteBuffer
/// which references data stored somewhere provided by the user.
pub struct WriteBuffer<'data, T: CAPISameRepr> {
    data: &'data [T],
    offsets: Option<&'data [u64]>,
    validity: Option<&'data [u8]>,
}

impl<'data, T: CAPISameRepr> From<&'data [T]> for WriteBuffer<'data, T> {
    fn from(value: &'data [T]) -> WriteBuffer<'data, T> {
        WriteBuffer {
            data: value,
            offsets: None,
            validity: None,
        }
    }
}

impl<'data, T: CAPISameRepr> From<(&'data [T], &'data [u64])>
    for WriteBuffer<'data, T>
{
    fn from(value: (&'data [T], &'data [u64])) -> WriteBuffer<'data, T> {
        WriteBuffer {
            data: value.0,
            offsets: Some(value.1),
            validity: None,
        }
    }
}

impl<'data, T: CAPISameRepr> From<(&'data [T], &'data [u8])>
    for WriteBuffer<'data, T>
{
    fn from(value: (&'data [T], &'data [u8])) -> WriteBuffer<'data, T> {
        WriteBuffer {
            data: value.0,
            offsets: None,
            validity: Some(value.1),
        }
    }
}

impl<'data, T: CAPISameRepr> From<(&'data [T], &'data [u64], &'data [u8])>
    for WriteBuffer<'data, T>
{
    fn from(
        value: (&'data [T], &'data [u64], &'data [u8]),
    ) -> WriteBuffer<'data, T> {
        WriteBuffer {
            data: value.0,
            offsets: Some(value.1),
            validity: Some(value.2),
        }
    }
}

impl<'data> From<&'data AllocatedWriteBuffer> for WriteBuffer<'data, u8> {
    fn from(wbuf: &'data AllocatedWriteBuffer) -> WriteBuffer<'data, u8> {
        WriteBuffer {
            data: wbuf.data.as_ref(),
            offsets: wbuf.offsets.as_ref().map(|o| o.as_ref()),
            validity: wbuf.validity.as_ref().map(|v| v.as_ref()),
        }
    }
}

/// An AllocatedWriteBuffer is used to create a valid WriteBuffer from data
/// sources that don't provide a native interface. This is mainly used as
/// a syntax helper for creating WriteBuffer instances from vectors of strings
/// or other variable length sources that are not already in a single
/// contiguous buffer required by TileDB.
pub struct AllocatedWriteBuffer {
    data: Box<[u8]>,
    offsets: Option<Box<[u64]>>,
    validity: Option<Box<[u8]>>,
}

impl From<&Vec<&str>> for AllocatedWriteBuffer {
    fn from(value: &Vec<&str>) -> AllocatedWriteBuffer {
        // Create and calculate our offsets
        let mut offsets: Vec<u64> = Vec::with_capacity(value.len());
        let mut curr_offset = 0u64;
        for val in value {
            offsets.push(curr_offset);
            curr_offset += val.len() as u64;
        }

        // Create our linearized data vector
        let mut data: Vec<u8> = Vec::with_capacity(curr_offset as usize);
        for (idx, val) in value.iter().enumerate() {
            let start = offsets[idx] as usize;
            let len = if idx < value.len() - 1 {
                offsets[idx + 1] - offsets[idx]
            } else {
                curr_offset - offsets[idx]
            } as usize;
            data[start..(start + len)].copy_from_slice(val.as_bytes())
        }

        AllocatedWriteBuffer {
            data: data.into_boxed_slice(),
            offsets: Some(offsets.into_boxed_slice()),
            validity: None,
        }
    }
}

impl From<&Vec<String>> for AllocatedWriteBuffer {
    fn from(value: &Vec<String>) -> AllocatedWriteBuffer {
        let refs = value.iter().map(|v| v.as_ref()).collect::<Vec<_>>();
        AllocatedWriteBuffer::from(&refs)
    }
}

pub enum WriteBufferCollectionEntry<'data> {
    UInt8(WriteBuffer<'data, u8>),
    UInt16(WriteBuffer<'data, u16>),
    UInt32(WriteBuffer<'data, u32>),
    UInt64(WriteBuffer<'data, u64>),
    Int8(WriteBuffer<'data, i8>),
    Int16(WriteBuffer<'data, i16>),
    Int32(WriteBuffer<'data, i32>),
    Int64(WriteBuffer<'data, i64>),
    Float32(WriteBuffer<'data, f32>),
    Float64(WriteBuffer<'data, f64>),
}

macro_rules! wb_entry_create_impl {
    ($($variant:ident : $ty:ty),+) => {
        $(
            impl<'data> From<WriteBuffer<'data, $ty>>
                for WriteBufferCollectionEntry<'data>
            {
                fn from(
                    value: WriteBuffer<'data, $ty>,
                ) -> WriteBufferCollectionEntry<'data> {
                    WriteBufferCollectionEntry::$variant(value)
                }
            }
        )+
    }
}

wb_entry_create_impl!(UInt8: u8, UInt16: u16, UInt32: u32, UInt64: u64);
wb_entry_create_impl!(Int8: i8, Int16: i16, Int32: i32, Int64: i64);
wb_entry_create_impl!(Float32: f32, Float64: f64);

pub struct WriteBufferCollectionItem<'data> {
    field: String,
    entry: WriteBufferCollectionEntry<'data>,
    next: Option<Box<WriteBufferCollectionItem<'data>>>,
}

/// A WriteBufferCollection is passed to the WriteQuery::submit method to send
/// data to TileDB.
pub struct WriteBufferCollection<'data> {
    buffers: Option<Box<WriteBufferCollectionItem<'data>>>,
    fields: HashSet<String>,
}

impl<'data> WriteBufferCollection<'data> {
    pub fn new() -> Self {
        Self {
            buffers: None,
            fields: HashSet::new(),
        }
    }

    pub fn with_buffer<T: CAPISameRepr>(
        mut self,
        field: &str,
        buffer: T,
    ) -> TileDBResult<Self>
    where
        T: Into<WriteBufferCollectionEntry<'data>>,
    {
        if self.fields.contains(field) {
            return Err(Error::InvalidArgument(anyhow!(
                "Duplicate values for field: {}",
                field
            )));
        }

        let old_buffers = self.buffers.take();

        self.fields.insert(field.to_owned());
        self.buffers = Some(Box::new(WriteBufferCollectionItem {
            field: field.to_owned(),
            entry: buffer.into(),
            next: old_buffers,
        }));

        Ok(self)
    }
}

impl<'data> Default for WriteBufferCollection<'data> {
    fn default() -> WriteBufferCollection<'data> {
        WriteBufferCollection::new()
    }
}
