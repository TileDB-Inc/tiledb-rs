use std::collections::HashSet;
use std::iter::FusedIterator;

use anyhow::anyhow;

use super::BufferCollectionItem;
use crate::error::Error;
use crate::Result as TileDBResult;

/// A write buffer is used by WriterQuery to pass data to TileDB. As such
/// it's name is a bit of an oxymoron as we only ever read from a WriteBuffer
/// which references data stored somewhere provided by the user.
pub struct ReadBuffer<T> {
    data: Box<[T]>,
    offsets: Option<Box<[u64]>>,
    validity: Option<Box<[u8]>>,
}

impl<T> ReadBuffer<T> {
    pub fn data_ptr(&self) -> *mut std::ffi::c_void {
        self.data.as_ptr() as *mut std::ffi::c_void
    }

    pub fn data_size(&self) -> u64 {
        (std::mem::size_of::<T>() * self.data.len()) as u64
    }

    pub fn offsets_ptr(&self) -> Option<*mut u64> {
        self.offsets.as_ref().map(|o| o.as_ptr() as *mut u64)
    }

    pub fn offsets_size(&self) -> Option<u64> {
        self.offsets
            .as_ref()
            .map(|o| (std::mem::size_of::<u64>() * o.len()) as u64)
    }

    pub fn validity_ptr(&self) -> Option<*mut u8> {
        self.validity.as_ref().map(|v| v.as_ptr() as *mut u8)
    }

    pub fn validity_size(&self) -> Option<u64> {
        self.validity
            .as_ref()
            .map(|v| (std::mem::size_of::<u8>() * v.len()) as u64)
    }
}

macro_rules! wb_entry_create_impl {
    ($($ty:ty),+) => {
        $(
            impl From<Box<[$ty]>> for ReadBuffer<$ty> {
                fn from(value: Box<[$ty]>) -> ReadBuffer<$ty> {
                    ReadBuffer {
                        data: value,
                        offsets: None,
                        validity: None,
                    }
                }
            }

            impl From<(Box<[$ty]>, Box<[u64]>)> for ReadBuffer<$ty> {
                fn from(value: (Box<[$ty]>, Box<[u64]>)) -> ReadBuffer<$ty> {
                    ReadBuffer {
                        data: value.0,
                        offsets: Some(value.1),
                        validity: None,
                    }
                }
            }

            impl From<(Box<[$ty]>, Box<[u8]>)> for ReadBuffer<$ty> {
                fn from(value: (Box<[$ty]>, Box<[u8]>)) -> ReadBuffer<$ty> {
                    ReadBuffer {
                        data: value.0,
                        offsets: None,
                        validity: Some(value.1),
                    }
                }
            }

            impl From<(Box<[$ty]>, Box<[u64]>, Box<[u8]>)> for ReadBuffer<$ty> {
                fn from(
                    value:(Box<[$ty]>, Box<[u64]>, Box<[u8]>),
                ) -> ReadBuffer<$ty> {
                    ReadBuffer {
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

impl From<&Vec<&str>> for ReadBuffer<u8> {
    fn from(value: &Vec<&str>) -> ReadBuffer<u8> {
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

        ReadBuffer {
            data: data.into_boxed_slice(),
            offsets: Some(offsets.into_boxed_slice()),
            validity: None,
        }
    }
}

impl From<&Vec<String>> for ReadBuffer<u8> {
    fn from(value: &Vec<String>) -> ReadBuffer<u8> {
        let refs: Vec<&str> =
            value.iter().map(|v| v.as_ref()).collect::<Vec<_>>();
        ReadBuffer::from(&refs)
    }
}

impl<T: Copy> From<&Vec<&[T]>> for ReadBuffer<T> {
    fn from(value: &Vec<&[T]>) -> ReadBuffer<T> {
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

        ReadBuffer {
            data: data.into_boxed_slice(),
            offsets: Some(offsets.into_boxed_slice()),
            validity: None,
        }
    }
}

impl<T: Copy> From<&Vec<&Vec<T>>> for ReadBuffer<T> {
    fn from(value: &Vec<&Vec<T>>) -> ReadBuffer<T> {
        let refs: Vec<&[T]> = value.iter().map(|v| v.as_slice()).collect();
        ReadBuffer::from(&refs)
    }
}

pub enum ReadBufferCollectionEntry {
    UInt8(ReadBuffer<u8>),
    UInt16(ReadBuffer<u16>),
    UInt32(ReadBuffer<u32>),
    UInt64(ReadBuffer<u64>),
    Int8(ReadBuffer<i8>),
    Int16(ReadBuffer<i16>),
    Int32(ReadBuffer<i32>),
    Int64(ReadBuffer<i64>),
    Float32(ReadBuffer<f32>),
    Float64(ReadBuffer<f64>),
}

macro_rules! wb_entry_create_impl {
    ($($variant:ident : $ty:ty),+) => {
        $(
            impl From<ReadBuffer< $ty>>
                for ReadBufferCollectionEntry
            {
                fn from(
                    value: ReadBuffer<$ty>,
                ) -> ReadBufferCollectionEntry {
                    ReadBufferCollectionEntry::$variant(value)
                }
            }

            impl<'data> From<Box<[$ty]>> for ReadBufferCollectionEntry {
                fn from(value: Box<[$ty]>) -> ReadBufferCollectionEntry {
                    let buffer = ReadBuffer::from(value);
                    ReadBufferCollectionEntry::from(buffer)
                }
            }
        )+
    }
}

wb_entry_create_impl!(UInt8: u8, UInt16: u16, UInt32: u32, UInt64: u64);
wb_entry_create_impl!(Int8: i8, Int16: i16, Int32: i32, Int64: i64);
wb_entry_create_impl!(Float32: f32, Float64: f64);

macro_rules! read_collection_entry_go {
    ($expr:expr, $DT:ident, $inner:pat, $then:expr) => {
        match $expr {
            ReadBufferCollectionEntry::UInt8($inner) => {
                type $DT = u8;
                $then
            }
            ReadBufferCollectionEntry::UInt16($inner) => {
                type $DT = u16;
                $then
            }
            ReadBufferCollectionEntry::UInt32($inner) => {
                type $DT = u32;
                $then
            }
            ReadBufferCollectionEntry::UInt64($inner) => {
                type $DT = u64;
                $then
            }
            ReadBufferCollectionEntry::Int8($inner) => {
                type $DT = i8;
                $then
            }
            ReadBufferCollectionEntry::Int16($inner) => {
                type $DT = i16;
                $then
            }
            ReadBufferCollectionEntry::Int32($inner) => {
                type $DT = i32;
                $then
            }
            ReadBufferCollectionEntry::Int64($inner) => {
                type $DT = i64;
                $then
            }
            ReadBufferCollectionEntry::Float32($inner) => {
                type $DT = f32;
                $then
            }
            ReadBufferCollectionEntry::Float64($inner) => {
                type $DT = f64;
                $then
            }
        }
    };
}

pub struct ReadBufferCollectionItem {
    name: String,
    entry: ReadBufferCollectionEntry,
    next: Option<Box<ReadBufferCollectionItem>>,
}

impl ReadBufferCollectionItem {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_ptr(&self) -> *mut std::ffi::c_void {
        read_collection_entry_go!(&self.entry, _DT, buf, buf.data_ptr())
    }

    pub fn data_size(&self) -> u64 {
        read_collection_entry_go!(&self.entry, _DT, buf, buf.data_size())
    }

    pub fn offsets_ptr(&self) -> Option<*mut u64> {
        read_collection_entry_go!(&self.entry, _DT, buf, buf.offsets_ptr())
    }

    pub fn offsets_size(&self) -> Option<u64> {
        read_collection_entry_go!(&self.entry, _DT, buf, buf.offsets_size())
    }

    pub fn validity_ptr(&self) -> Option<*mut u8> {
        read_collection_entry_go!(&self.entry, _DT, buf, buf.validity_ptr())
    }

    pub fn validity_size(&self) -> Option<u64> {
        read_collection_entry_go!(&self.entry, _DT, buf, buf.validity_size())
    }
}

impl BufferCollectionItem for ReadBufferCollectionItem {
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
pub struct ReadBufferCollection {
    buffers: Option<Box<ReadBufferCollectionItem>>,
    fields: HashSet<String>,
}

impl ReadBufferCollection {
    pub fn new() -> Self {
        Self {
            buffers: None,
            fields: HashSet::new(),
        }
    }

    pub fn add_buffer<T>(mut self, name: &str, buffer: T) -> TileDBResult<Self>
    where
        T: Into<ReadBufferCollectionEntry>,
    {
        if self.fields.contains(name) {
            return Err(Error::InvalidArgument(anyhow!(
                "Duplicate values for field: {}",
                name
            )));
        }

        let old_buffers = self.buffers.take();

        self.fields.insert(name.to_owned());
        self.buffers = Some(Box::new(ReadBufferCollectionItem {
            name: name.to_owned(),
            entry: buffer.into(),
            next: old_buffers,
        }));

        Ok(self)
    }

    pub fn iter(&self) -> ReadBufferCollectionIterator {
        ReadBufferCollectionIterator {
            curr_item: self.buffers.as_ref().map(|b| b.as_ref()),
        }
    }
}

impl Default for ReadBufferCollection {
    fn default() -> ReadBufferCollection {
        ReadBufferCollection::new()
    }
}

pub struct ReadBufferCollectionIterator<'this> {
    curr_item: Option<&'this ReadBufferCollectionItem>,
}

impl<'this> Iterator for ReadBufferCollectionIterator<'this> {
    type Item = &'this ReadBufferCollectionItem;

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
    for ReadBufferCollectionIterator<'this>
{
}
