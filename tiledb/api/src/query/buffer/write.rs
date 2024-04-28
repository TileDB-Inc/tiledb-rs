use std::collections::HashSet;
use std::iter::FusedIterator;

use anyhow::anyhow;

use super::BufferCollectionItem;
use crate::error::Error;
use crate::Result as TileDBResult;

/// A write buffer is used by WriterQuery to pass data to TileDB. As such
/// it's name is a bit of an oxymoron as we only ever read from a WriteBuffer
/// which references data stored somewhere provided by the user.
pub struct WriteBuffer<'data, T> {
    data: &'data [T],
    offsets: Option<&'data [u64]>,
    validity: Option<&'data [u8]>,
}

impl<'data, T> WriteBuffer<'data, T> {
    pub fn data_ptr(&self) -> *mut std::ffi::c_void {
        self.data.as_ptr() as *mut std::ffi::c_void
    }

    pub fn data_size(&self) -> u64 {
        std::mem::size_of_val(self.data) as u64
    }

    pub fn offsets_ptr(&self) -> Option<*mut u64> {
        self.offsets.map(|o| o.as_ptr() as *mut u64)
    }

    pub fn offsets_size(&self) -> Option<u64> {
        self.offsets.map(|o| std::mem::size_of_val(o) as u64)
    }

    pub fn validity_ptr(&self) -> Option<*mut u8> {
        self.validity.map(|v| v.as_ptr() as *mut u8)
    }

    pub fn validity_size(&self) -> Option<u64> {
        self.validity.map(|v| std::mem::size_of_val(v) as u64)
    }
}

macro_rules! wb_entry_create_impl {
    ($($ty:ty),+) => {
        $(
            impl<'data> From<&'data [$ty]> for WriteBuffer<'data, $ty> {
                fn from(value: &'data [$ty]) -> WriteBuffer<'data, $ty> {
                    WriteBuffer {
                        data: value,
                        offsets: None,
                        validity: None,
                    }
                }
            }

            impl<'data> From<(&'data [$ty], &'data [u64])> for WriteBuffer<'data, $ty> {
                fn from(value: (&'data [$ty], &'data [u64])) -> WriteBuffer<'data, $ty> {
                    WriteBuffer {
                        data: value.0,
                        offsets: Some(value.1),
                        validity: None,
                    }
                }
            }

            impl<'data> From<(&'data [$ty], &'data [u8])> for WriteBuffer<'data, $ty> {
                fn from(value: (&'data [$ty], &'data [u8])) -> WriteBuffer<'data, $ty> {
                    WriteBuffer {
                        data: value.0,
                        offsets: None,
                        validity: Some(value.1),
                    }
                }
            }

            impl<'data> From<(&'data [$ty], &'data [u64], &'data [u8])>
                for WriteBuffer<'data, $ty>
            {
                fn from(
                    value: (&'data [$ty], &'data [u64], &'data [u8]),
                ) -> WriteBuffer<'data, $ty> {
                    WriteBuffer {
                        data: value.0,
                        offsets: Some(value.1),
                        validity: Some(value.2),
                    }
                }
            }
        )+
    }
}

wb_entry_create_impl!(u8, u16, u32, u64);
wb_entry_create_impl!(i8, i16, i32, i64);
wb_entry_create_impl!(f32, f64);

impl<'data, T> From<&'data AllocatedWriteBuffer<T>> for WriteBuffer<'data, T> {
    fn from(wbuf: &'data AllocatedWriteBuffer<T>) -> WriteBuffer<'data, T> {
        WriteBuffer {
            data: wbuf.data.as_ref(),
            offsets: wbuf.offsets.as_ref().map(|o| o.as_ref()),
            validity: None,
        }
    }
}

impl<'data, T> From<(&'data AllocatedWriteBuffer<T>, &'data [u8])>
    for WriteBuffer<'data, T>
{
    fn from(
        value: (&'data AllocatedWriteBuffer<T>, &'data [u8]),
    ) -> WriteBuffer<'data, T> {
        WriteBuffer {
            data: value.0.data.as_ref(),
            offsets: value.0.offsets.as_ref().map(|o| o.as_ref()),
            validity: Some(value.1),
        }
    }
}

/// An AllocatedWriteBuffer is used to create a valid WriteBuffer from data
/// sources that don't provide a native interface. This is mainly used as
/// a syntax helper for creating WriteBuffer instances from vectors of strings
/// or other variable length sources that are not already in a single
/// contiguous buffer required by TileDB.
pub struct AllocatedWriteBuffer<T> {
    data: Box<[T]>,
    offsets: Option<Box<[u64]>>,
}

impl From<&Vec<&str>> for AllocatedWriteBuffer<u8> {
    fn from(value: &Vec<&str>) -> AllocatedWriteBuffer<u8> {
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
            let len = val.len();
            data[start..(start + len)].copy_from_slice(val.as_bytes())
        }

        AllocatedWriteBuffer {
            data: data.into_boxed_slice(),
            offsets: Some(offsets.into_boxed_slice()),
        }
    }
}

impl From<&Vec<String>> for AllocatedWriteBuffer<u8> {
    fn from(value: &Vec<String>) -> AllocatedWriteBuffer<u8> {
        let refs: Vec<&str> =
            value.iter().map(|v| v.as_ref()).collect::<Vec<_>>();
        AllocatedWriteBuffer::from(&refs)
    }
}

impl<T: Copy> From<&Vec<&[T]>> for AllocatedWriteBuffer<T> {
    fn from(value: &Vec<&[T]>) -> AllocatedWriteBuffer<T> {
        // Calculate our offsets and required capacity. Its important to note
        // that offsets are the byte offsets which is different than the
        // array offsets for anything except u8/i8.
        let mut offsets: Vec<u64> = Vec::with_capacity(value.len());
        let mut curr_offset = 0u64;
        let mut capacity = 0usize;
        for val in value {
            offsets.push(curr_offset);
            curr_offset += std::mem::size_of_val(*val) as u64;
            capacity += val.len();
        }

        // Create an fill the linearized buffer
        let mut data: Vec<T> = Vec::with_capacity(capacity);
        let mut curr_offset = 0usize;
        for val in value {
            let start = curr_offset;
            let len = val.len();
            data[start..(start + len)].copy_from_slice(val);
            curr_offset += val.len();
        }

        AllocatedWriteBuffer {
            data: data.into_boxed_slice(),
            offsets: Some(offsets.into_boxed_slice()),
        }
    }
}

impl<T: Copy> From<&Vec<&Vec<T>>> for AllocatedWriteBuffer<T> {
    fn from(value: &Vec<&Vec<T>>) -> AllocatedWriteBuffer<T> {
        let refs: Vec<&[T]> = value.iter().map(|v| v.as_slice()).collect();
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

            impl<'data> From<&'data [$ty]> for WriteBufferCollectionEntry<'data> {
                fn from(value: &'data [$ty]) -> WriteBufferCollectionEntry<'data> {
                    let buffer = WriteBuffer::from(value);
                    WriteBufferCollectionEntry::from(buffer)
                }
            }
        )+
    }
}

wb_entry_create_impl!(UInt8: u8, UInt16: u16, UInt32: u32, UInt64: u64);
wb_entry_create_impl!(Int8: i8, Int16: i16, Int32: i32, Int64: i64);
wb_entry_create_impl!(Float32: f32, Float64: f64);

macro_rules! write_collection_entry_go {
    ($expr:expr, $DT:ident, $inner:pat, $then:expr) => {
        match $expr {
            WriteBufferCollectionEntry::UInt8($inner) => {
                type $DT = u8;
                $then
            }
            WriteBufferCollectionEntry::UInt16($inner) => {
                type $DT = u16;
                $then
            }
            WriteBufferCollectionEntry::UInt32($inner) => {
                type $DT = u32;
                $then
            }
            WriteBufferCollectionEntry::UInt64($inner) => {
                type $DT = u64;
                $then
            }
            WriteBufferCollectionEntry::Int8($inner) => {
                type $DT = i8;
                $then
            }
            WriteBufferCollectionEntry::Int16($inner) => {
                type $DT = i16;
                $then
            }
            WriteBufferCollectionEntry::Int32($inner) => {
                type $DT = i32;
                $then
            }
            WriteBufferCollectionEntry::Int64($inner) => {
                type $DT = i64;
                $then
            }
            WriteBufferCollectionEntry::Float32($inner) => {
                type $DT = f32;
                $then
            }
            WriteBufferCollectionEntry::Float64($inner) => {
                type $DT = f64;
                $then
            }
        }
    };
}

pub struct WriteBufferCollectionItem<'data> {
    name: String,
    entry: WriteBufferCollectionEntry<'data>,
    next: Option<Box<WriteBufferCollectionItem<'data>>>,
}

impl<'data> WriteBufferCollectionItem<'data> {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn entry(&self) -> &WriteBufferCollectionEntry<'data> {
        &self.entry
    }

    pub fn data_ptr(&self) -> *mut std::ffi::c_void {
        write_collection_entry_go!(&self.entry, _DT, buf, buf.data_ptr())
    }

    pub fn data_size(&self) -> u64 {
        write_collection_entry_go!(&self.entry, _DT, buf, buf.data_size())
    }

    pub fn offsets_ptr(&self) -> Option<*mut u64> {
        write_collection_entry_go!(&self.entry, _DT, buf, buf.offsets_ptr())
    }

    pub fn offsets_size(&self) -> Option<u64> {
        write_collection_entry_go!(&self.entry, _DT, buf, buf.offsets_size())
    }

    pub fn validity_ptr(&self) -> Option<*mut u8> {
        write_collection_entry_go!(&self.entry, _DT, buf, buf.validity_ptr())
    }

    pub fn validity_size(&self) -> Option<u64> {
        write_collection_entry_go!(&self.entry, _DT, buf, buf.validity_size())
    }
}

impl<'data> BufferCollectionItem for WriteBufferCollectionItem<'data> {
    fn name(&self) -> &str {
        self.name()
    }

    fn data_ptr(&self) -> *mut std::ffi::c_void {
        self.data_ptr()
    }

    fn data_size(&self) -> u64 {
        self.data_size()
    }

    fn offsets_ptr(&self) -> Option<*mut u64> {
        self.offsets_ptr()
    }

    fn offsets_size(&self) -> Option<u64> {
        self.offsets_size()
    }

    fn validity_ptr(&self) -> Option<*mut u8> {
        self.validity_ptr()
    }

    fn validity_size(&self) -> Option<u64> {
        self.validity_size()
    }
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

    pub fn add_buffer<T>(mut self, name: &str, buffer: T) -> TileDBResult<Self>
    where
        T: Into<WriteBufferCollectionEntry<'data>>,
    {
        if self.fields.contains(name) {
            return Err(Error::InvalidArgument(anyhow!(
                "Duplicate values for field: {}",
                name
            )));
        }

        let old_buffers = self.buffers.take();

        self.fields.insert(name.to_owned());
        self.buffers = Some(Box::new(WriteBufferCollectionItem {
            name: name.to_owned(),
            entry: buffer.into(),
            next: old_buffers,
        }));

        Ok(self)
    }

    pub fn iter<'this: 'data>(
        &'this self,
    ) -> WriteBufferCollectionIterator<'this, 'data> {
        WriteBufferCollectionIterator {
            curr_item: self.buffers.as_ref().map(|b| b.as_ref()),
        }
    }
}

impl<'data> Default for WriteBufferCollection<'data> {
    fn default() -> WriteBufferCollection<'data> {
        WriteBufferCollection::new()
    }
}

pub struct WriteBufferCollectionIterator<'this, 'data> {
    curr_item: Option<&'this WriteBufferCollectionItem<'data>>,
}

impl<'data, 'this: 'data> Iterator
    for WriteBufferCollectionIterator<'this, 'data>
{
    type Item = &'this WriteBufferCollectionItem<'data>;

    // This is a bit gnarly to get all of the Option<&T> things lined up
    // correctly, but otherwise this is pretty straightforward. First, if
    // curr_item is None, return None. Otherwise, extract an Option<&T> to
    // curr_item.next, assign that to self.curr_item and return the previous
    // contents of self.curr_item.
    //
    // This is standard walk over a singly linked list, popping nodes off
    // the stack.
    fn next(&mut self) -> Option<Self::Item> {
        // Return None if we're out of buffers.
        self.curr_item?;
        let curr_item = self.curr_item;
        self.curr_item =
            curr_item.map(|item| item.next.as_ref().map(|b| b.as_ref()))?;
        curr_item
    }
}

impl<'data, 'this: 'data> FusedIterator
    for WriteBufferCollectionIterator<'this, 'data>
{
}
