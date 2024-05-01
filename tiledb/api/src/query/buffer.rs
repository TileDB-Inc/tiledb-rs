use std::cell::Ref;
use std::num::NonZeroU32;
use std::ops::{Deref, DerefMut};

use crate::array::CellValNum;

pub enum Buffer<'data, T = u8> {
    Empty,
    Borrowed(&'data [T]),
    Owned(Box<[T]>),
}

impl<'data, T> Buffer<'data, T> {
    pub fn size(&self) -> usize {
        std::mem::size_of_val(self.as_ref())
    }

    pub fn borrow<'this>(&'this self) -> Buffer<'data, T>
    where
        'this: 'data,
    {
        Buffer::Borrowed(self.as_ref())
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

impl<'data, T> From<Vec<T>> for Buffer<'data, T> {
    fn from(value: Vec<T>) -> Self {
        Buffer::Owned(value.into_boxed_slice())
    }
}

pub enum CellStructure<'data> {
    Fixed(NonZeroU32),
    Var(Buffer<'data, u64>),
}

impl<'data> CellStructure<'data> {
    pub fn single() -> Self {
        CellStructure::Fixed(NonZeroU32::new(1).unwrap())
    }

    pub fn is_fixed(&self) -> bool {
        matches!(self, Self::Fixed(_))
    }

    pub fn is_var(&self) -> bool {
        matches!(self, Self::Var(_))
    }

    pub fn as_cell_val_num(&self) -> CellValNum {
        match self {
            Self::Fixed(nz) => CellValNum::Fixed(*nz),
            Self::Var(_) => CellValNum::Var,
        }
    }

    pub fn unwrap(self) -> Option<Buffer<'data, u64>> {
        if let Self::Var(offsets) = self {
            Some(offsets)
        } else {
            None
        }
    }

    pub fn offsets_ref(&self) -> Option<&Buffer<'data, u64>> {
        if let Self::Var(ref offsets) = self {
            Some(offsets)
        } else {
            None
        }
    }

    pub fn map_offsets<U, F>(&self, func: F) -> Option<U>
    where
        for<'a> F: FnOnce(&'a Buffer<'data, u64>) -> U,
    {
        if let Self::Var(ref offsets) = self {
            Some(func(offsets))
        } else {
            None
        }
    }

    pub fn borrow<'this>(&'this self) -> CellStructure<'data>
    where
        'this: 'data,
    {
        match self {
            Self::Fixed(ref nz) => Self::Fixed(*nz),
            Self::Var(ref offsets) => Self::Var(offsets.borrow()),
        }
    }
}

impl Default for CellStructure<'_> {
    fn default() -> Self {
        Self::single()
    }
}

impl<'data> From<NonZeroU32> for CellStructure<'data> {
    fn from(value: NonZeroU32) -> Self {
        Self::Fixed(value)
    }
}

pub struct QueryBuffers<'data, C> {
    pub data: Buffer<'data, C>,
    pub cell_structure: CellStructure<'data>,
    pub validity: Option<Buffer<'data, u8>>,
}

impl<'data, C> QueryBuffers<'data, C> {
    pub fn borrow<'this>(&'this self) -> QueryBuffers<'data, C>
    where
        'this: 'data,
    {
        QueryBuffers {
            data: Buffer::Borrowed(self.data.as_ref()),
            cell_structure: self.cell_structure.borrow(),
            validity: Option::map(self.validity.as_ref(), |v| {
                Buffer::Borrowed(v.as_ref())
            }),
        }
    }
}

pub enum BufferMut<'data, C> {
    Empty,
    Borrowed(&'data mut [C]),
    Owned(Box<[C]>),
}

impl<'data, T> BufferMut<'data, T> {
    pub fn size(&self) -> usize {
        std::mem::size_of_val(self.as_ref())
    }

    pub fn borrow<'this>(&'this self) -> Buffer<'data, T>
    where
        'this: 'data,
    {
        Buffer::Borrowed(self.as_ref())
    }

    pub fn borrow_mut<'this>(&'this mut self) -> BufferMut<'data, T>
    where
        'this: 'data,
    {
        BufferMut::Borrowed(self.as_mut())
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

impl<'data, T> From<Vec<T>> for BufferMut<'data, T> {
    fn from(value: Vec<T>) -> Self {
        BufferMut::Owned(value.into_boxed_slice())
    }
}

pub enum CellStructureMut<'data> {
    Fixed(NonZeroU32),
    Var(BufferMut<'data, u64>),
}

impl<'data> CellStructureMut<'data> {
    pub fn single() -> Self {
        CellStructureMut::Fixed(NonZeroU32::new(1).unwrap())
    }

    pub fn is_fixed(&self) -> bool {
        matches!(self, Self::Fixed(_))
    }

    pub fn is_var(&self) -> bool {
        matches!(self, Self::Var(_))
    }

    pub fn as_cell_val_num(&self) -> CellValNum {
        match self {
            Self::Fixed(nz) => CellValNum::Fixed(*nz),
            Self::Var(_) => CellValNum::Var,
        }
    }

    pub fn unwrap(self) -> Option<BufferMut<'data, u64>> {
        if let Self::Var(offsets) = self {
            Some(offsets)
        } else {
            None
        }
    }

    pub fn offsets_ref(&self) -> Option<&BufferMut<'data, u64>> {
        if let Self::Var(ref offsets) = self {
            Some(offsets)
        } else {
            None
        }
    }

    pub fn offsets_mut(&mut self) -> Option<&mut BufferMut<'data, u64>> {
        if let Self::Var(ref mut offsets) = self {
            Some(offsets)
        } else {
            None
        }
    }

    pub fn map_offsets<U, F>(&self, func: F) -> Option<U>
    where
        for<'a> F: FnOnce(&'a BufferMut<'data, u64>) -> U,
    {
        if let Self::Var(ref offsets) = self {
            Some(func(offsets))
        } else {
            None
        }
    }

    pub fn borrow<'this>(&'this self) -> CellStructure<'data>
    where
        'this: 'data,
    {
        match self {
            Self::Fixed(ref nz) => CellStructure::Fixed(*nz),
            Self::Var(ref offsets) => CellStructure::Var(offsets.borrow()),
        }
    }

    pub fn borrow_mut<'this>(&'this mut self) -> CellStructureMut<'data>
    where
        'this: 'data,
    {
        match self {
            Self::Fixed(ref nz) => Self::Fixed(*nz),
            Self::Var(ref mut offsets) => Self::Var(offsets.borrow_mut()),
        }
    }
}

impl Default for CellStructureMut<'_> {
    fn default() -> Self {
        Self::single()
    }
}

impl<'data> From<NonZeroU32> for CellStructureMut<'data> {
    fn from(value: NonZeroU32) -> Self {
        Self::Fixed(value)
    }
}

pub struct QueryBuffersMut<'data, T = u8> {
    pub data: BufferMut<'data, T>,
    pub cell_structure: CellStructureMut<'data>,
    pub validity: Option<BufferMut<'data, u8>>,
}

impl<'data, T> QueryBuffersMut<'data, T> {
    /// Borrows this QueryBuffersMut to use as input data.
    pub fn as_shared<'this>(&'this self) -> QueryBuffers<'data, T>
    where
        'this: 'data,
    {
        QueryBuffers {
            data: Buffer::Borrowed(self.data.as_ref()),
            cell_structure: self.cell_structure.borrow(),
            validity: Option::map(self.validity.as_ref(), |v| {
                Buffer::Borrowed(v.as_ref())
            }),
        }
    }
}

pub enum TypedQueryBuffers<'data> {
    UInt8(QueryBuffers<'data, u8>),
    UInt16(QueryBuffers<'data, u16>),
    UInt32(QueryBuffers<'data, u32>),
    UInt64(QueryBuffers<'data, u64>),
    Int8(QueryBuffers<'data, i8>),
    Int16(QueryBuffers<'data, i16>),
    Int32(QueryBuffers<'data, i32>),
    Int64(QueryBuffers<'data, i64>),
    Float32(QueryBuffers<'data, f32>),
    Float64(QueryBuffers<'data, f64>),
}

pub enum RefTypedQueryBuffersMut<'cell, 'data> {
    UInt8(Ref<'cell, QueryBuffersMut<'data, u8>>),
    UInt16(Ref<'cell, QueryBuffersMut<'data, u16>>),
    UInt32(Ref<'cell, QueryBuffersMut<'data, u32>>),
    UInt64(Ref<'cell, QueryBuffersMut<'data, u64>>),
    Int8(Ref<'cell, QueryBuffersMut<'data, i8>>),
    Int16(Ref<'cell, QueryBuffersMut<'data, i16>>),
    Int32(Ref<'cell, QueryBuffersMut<'data, i32>>),
    Int64(Ref<'cell, QueryBuffersMut<'data, i64>>),
    Float32(Ref<'cell, QueryBuffersMut<'data, f32>>),
    Float64(Ref<'cell, QueryBuffersMut<'data, f64>>),
}

macro_rules! typed_query_buffers {
    ($($V:ident : $U:ty),+) => {
        $(
            impl<'data> From<QueryBuffers<'data, $U>> for TypedQueryBuffers<'data> {
                fn from(value: QueryBuffers<'data, $U>) -> Self {
                    TypedQueryBuffers::$V(value)
                }
            }

            impl<'data> TryFrom<TypedQueryBuffers<'data>> for QueryBuffers<'data, $U> {
                type Error = ();
                fn try_from(value: TypedQueryBuffers<'data>) -> std::result::Result<Self, Self::Error> {
                    if let TypedQueryBuffers::$V(value) = value {
                        Ok(value)
                    } else {
                        Err(())
                    }
                }
            }

            impl<'cell, 'data> From<Ref<'cell, QueryBuffersMut<'data, $U>>> for RefTypedQueryBuffersMut<'cell, 'data> {
                fn from(value: Ref<'cell, QueryBuffersMut<'data, $U>>) -> Self {
                    RefTypedQueryBuffersMut::$V(value)
                }
            }
        )+
    }
}

typed_query_buffers!(UInt8: u8, UInt16: u16, UInt32: u32, UInt64: u64);
typed_query_buffers!(Int8: i8, Int16: i16, Int32: i32, Int64: i64);
typed_query_buffers!(Float32: f32, Float64: f64);

#[macro_export]
macro_rules! typed_query_buffers_go {
    ($expr:expr, $DT:ident, $inner:pat, $then:expr) => {
        match $expr {
            TypedQueryBuffers::UInt8($inner) => {
                type $DT = u8;
                $then
            }
            TypedQueryBuffers::UInt16($inner) => {
                type $DT = u16;
                $then
            }
            TypedQueryBuffers::UInt32($inner) => {
                type $DT = u32;
                $then
            }
            TypedQueryBuffers::UInt64($inner) => {
                type $DT = u64;
                $then
            }
            TypedQueryBuffers::Int8($inner) => {
                type $DT = i8;
                $then
            }
            TypedQueryBuffers::Int16($inner) => {
                type $DT = i16;
                $then
            }
            TypedQueryBuffers::Int32($inner) => {
                type $DT = i32;
                $then
            }
            TypedQueryBuffers::Int64($inner) => {
                type $DT = i64;
                $then
            }
            TypedQueryBuffers::Float32($inner) => {
                type $DT = f32;
                $then
            }
            TypedQueryBuffers::Float64($inner) => {
                type $DT = f64;
                $then
            }
        }
    };
}

macro_rules! ref_typed_query_buffers_go {
    ($expr:expr, $DT:ident, $inner:pat, $then:expr) => {
        match $expr {
            RefTypedQueryBuffersMut::UInt8($inner) => {
                type $DT = u8;
                $then
            }
            RefTypedQueryBuffersMut::UInt16($inner) => {
                type $DT = u16;
                $then
            }
            RefTypedQueryBuffersMut::UInt32($inner) => {
                type $DT = u32;
                $then
            }
            RefTypedQueryBuffersMut::UInt64($inner) => {
                type $DT = u64;
                $then
            }
            RefTypedQueryBuffersMut::Int8($inner) => {
                type $DT = i8;
                $then
            }
            RefTypedQueryBuffersMut::Int16($inner) => {
                type $DT = i16;
                $then
            }
            RefTypedQueryBuffersMut::Int32($inner) => {
                type $DT = i32;
                $then
            }
            RefTypedQueryBuffersMut::Int64($inner) => {
                type $DT = i64;
                $then
            }
            RefTypedQueryBuffersMut::Float32($inner) => {
                type $DT = f32;
                $then
            }
            RefTypedQueryBuffersMut::Float64($inner) => {
                type $DT = f64;
                $then
            }
        }
    };
}

impl<'cell, 'data> RefTypedQueryBuffersMut<'cell, 'data> {
    pub fn as_shared(&'cell self) -> TypedQueryBuffers<'cell> {
        ref_typed_query_buffers_go!(self, _DT, qb, {
            TypedQueryBuffers::from(qb.as_shared())
        })
    }
}

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy {
    use proptest::collection::vec;
    use proptest::prelude::*;

    pub fn prop_string_vec(
        range: proptest::collection::SizeRange,
    ) -> impl Strategy<Value = Vec<String>> {
        vec(vec(1u8..127, 0..64), range)
            .prop_map(move |mut v| {
                v.iter_mut()
                    .map(|s| String::from_utf8(s.clone()).unwrap())
                    .collect::<Vec<_>>()
            })
            .boxed()
    }
}
