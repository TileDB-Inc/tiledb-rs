use std::cell::Ref;
use std::num::NonZeroU32;
use std::ops::{Deref, DerefMut};

use crate::array::CellValNum;

#[cfg(feature = "arrow")]
pub mod arrow;

#[derive(Debug)]
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
        Self::from(value.into_boxed_slice())
    }
}

impl<'data, T> From<Box<[T]>> for Buffer<'data, T> {
    fn from(value: Box<[T]>) -> Self {
        Buffer::Owned(value)
    }
}

/// Contains the structural information needed to interpret the values of a
/// query buffer into distinct cells of an attribute or dimension.
#[derive(Debug)]
pub enum CellStructure<'data> {
    /// The number of values per cell is a specific fixed number.
    Fixed(NonZeroU32),
    /// The number of values per cell varies.
    /// The contained buffer indicates the offset of each cell into an accompanying
    /// values buffer. The values contained within cell `x` are those within the
    /// range `offsets[x].. offsets[x + 1]`, except for the last value which
    /// begins at `offset[x]` and ends at the end of the values buffer.
    Var(Buffer<'data, u64>),
}

impl<'data> CellStructure<'data> {
    /// Returns `CellStructure::Fixed(1)`, where each value is its own cell.
    pub fn single() -> Self {
        CellStructure::Fixed(NonZeroU32::new(1).unwrap())
    }

    /// Returns whether the cells contain exactly one value.
    pub fn is_single(&self) -> bool {
        matches!(self, Self::Fixed(nz) if nz.get() == 1)
    }

    /// Returns whether the cells contain a fixed number of values.
    pub fn is_fixed(&self) -> bool {
        matches!(self, Self::Fixed(_))
    }

    /// Returns whether the cells contain a variable number of values.
    pub fn is_var(&self) -> bool {
        matches!(self, Self::Var(_))
    }

    /// Returns a corresponding `CellValNum` for this structure.
    pub fn as_cell_val_num(&self) -> CellValNum {
        match self {
            Self::Fixed(nz) => CellValNum::Fixed(*nz),
            Self::Var(_) => CellValNum::Var,
        }
    }

    /// Consumes the `CellStructure` and returns the offsets if present.
    pub fn unwrap(self) -> Option<Buffer<'data, u64>> {
        if let Self::Var(offsets) = self {
            Some(offsets)
        } else {
            None
        }
    }

    /// Return the fixed number of values per cell, if not variable.
    pub fn fixed(&self) -> Option<NonZeroU32> {
        if let Self::Fixed(nz) = self {
            Some(*nz)
        } else {
            None
        }
    }

    /// Returns a reference to the offsets buffer, if any.
    pub fn offsets_ref(&self) -> Option<&[u64]> {
        if let Self::Var(ref offsets) = self {
            Some(offsets.as_ref())
        } else {
            None
        }
    }

    /// Applies a function to the offsets buffer, if any, and returns the result.
    pub fn map_offsets<U, F>(&self, func: F) -> Option<U>
    where
        F: FnOnce(&[u64]) -> U,
    {
        if let Self::Var(ref offsets) = self {
            Some(func(offsets.as_ref()))
        } else {
            None
        }
    }

    /// Returns a `CellStructure` with the same contents as this one, sharing the underlying
    /// offsets buffer if any.
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
    /// Returns `CellStructure::single()`.
    fn default() -> Self {
        Self::single()
    }
}

impl<'data> From<NonZeroU32> for CellStructure<'data> {
    fn from(value: NonZeroU32) -> Self {
        Self::Fixed(value)
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
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

/// Contains the structural information needed to interpret the values of a
/// query buffer into distinct cells of an attribute or dimension.
#[derive(Debug)]
pub enum CellStructureMut<'data> {
    /// The number of values per cell is a specific fixed number.
    Fixed(NonZeroU32),
    /// The number of values per cell varies.
    /// The contained buffer provides space to write offsets which
    /// define the contents of each cell.  See `CellStructure::Var` for the
    /// expected offset format.
    Var(BufferMut<'data, u64>),
}

impl<'data> CellStructureMut<'data> {
    /// Returns `CellStructure::Fixed(1)`, where each value is its own cell.
    pub fn single() -> Self {
        CellStructureMut::Fixed(NonZeroU32::new(1).unwrap())
    }

    /// Returns whether the cells contain exactly one value.
    pub fn is_single(&self) -> bool {
        matches!(self, Self::Fixed(nz) if nz.get() == 1)
    }

    /// Returns whether the cells contain a fixed number of values.
    pub fn is_fixed(&self) -> bool {
        matches!(self, Self::Fixed(_))
    }

    /// Returns whether the cells contain a variable number of values.
    pub fn is_var(&self) -> bool {
        matches!(self, Self::Var(_))
    }

    /// Returns a corresponding `CellValNum` for this structure.
    pub fn as_cell_val_num(&self) -> CellValNum {
        match self {
            Self::Fixed(nz) => CellValNum::Fixed(*nz),
            Self::Var(_) => CellValNum::Var,
        }
    }

    /// Consumes the `CellStructure` and returns the offsets if present.
    pub fn unwrap(self) -> Option<BufferMut<'data, u64>> {
        if let Self::Var(offsets) = self {
            Some(offsets)
        } else {
            None
        }
    }

    /// Return the fixed number of values per cell, if not variable.
    pub fn fixed(&self) -> Option<NonZeroU32> {
        if let Self::Fixed(nz) = self {
            Some(*nz)
        } else {
            None
        }
    }

    /// Returns a reference to the offsets buffer, if any.
    pub fn offsets_ref(&self) -> Option<&[u64]> {
        if let Self::Var(ref offsets) = self {
            Some(offsets.as_ref())
        } else {
            None
        }
    }

    /// Returns a mutable reference to the offsets buffer, if any.
    pub fn offsets_mut(&mut self) -> Option<&mut BufferMut<'data, u64>> {
        if let Self::Var(ref mut offsets) = self {
            Some(offsets)
        } else {
            None
        }
    }

    /// Applies a function to the offsets buffer, if any, and returns the result.
    pub fn map_offsets<U, F>(&self, func: F) -> Option<U>
    where
        F: FnOnce(&BufferMut<'data, u64>) -> U,
    {
        if let Self::Var(ref offsets) = self {
            Some(func(offsets))
        } else {
            None
        }
    }

    /// Returns a `CellStructure` with the same contents as this one, sharing the underlying
    /// offsets buffer if any.
    pub fn borrow<'this>(&'this self) -> CellStructure<'data>
    where
        'this: 'data,
    {
        match self {
            Self::Fixed(ref nz) => CellStructure::Fixed(*nz),
            Self::Var(ref offsets) => CellStructure::Var(offsets.borrow()),
        }
    }

    /// Returns a `CellStructure` with the same contents as this one, with a mutable reference
    /// to the same underlying offsets buffer if any.
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
    /// Returns `CellStructureMut::single()`.
    fn default() -> Self {
        Self::single()
    }
}

impl<'data> From<NonZeroU32> for CellStructureMut<'data> {
    fn from(value: NonZeroU32) -> Self {
        Self::Fixed(value)
    }
}

#[derive(Debug)]
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

/// Generates a set of `impl`s for a "query buffer proof", which we
/// use to mean a type which wraps a `QueryBuffers` if the wrapped buffers
/// satisfies some property. Code which accesses the wrapped buffers may
/// safely assume the property is satisfied, which is useful for things
/// like `unwrap` and `unsafe`.
///
/// Usage of this macro for a type requires a method `fn accept` which
/// returns whether a `QueryBuffers` satisfies the property desired by the type.
macro_rules! query_buffers_proof_impls {
    ($($Q:ident),+) => {
        $(
            impl<'data, C> $Q<'data, C> {
                pub fn into_inner(self) -> QueryBuffers<'data, C> {
                    self.0
                }
            }

            impl<'data, C> AsRef<QueryBuffers<'data, C>> for $Q<'data, C>
            {
                fn as_ref(&self) -> &QueryBuffers<'data, C> {
                    &self.0
                }
            }

            impl<'data, C> TryFrom<QueryBuffers<'data, C>> for $Q<'data, C>
            {
                type Error = QueryBuffers<'data, C>;

                fn try_from(value: QueryBuffers<'data, C>) -> Result<Self, Self::Error> {
                    if Self::accept(&value) {
                        Ok(Self(value))
                    } else {
                        Err(value)
                    }
                }
            }
        )+
    }
}
pub(crate) use query_buffers_proof_impls;

/// A set of `QueryBuffers` which is known to have `cell_structure: CellStructure::Fixed(1)`.
pub struct QueryBuffersCellStructureSingle<'data, C>(QueryBuffers<'data, C>);

impl<'data, C> QueryBuffersCellStructureSingle<'data, C> {
    pub fn accept(value: &QueryBuffers<'data, C>) -> bool {
        value.cell_structure.is_single()
    }
}

/// A set of `QueryBuffers` which is known to have `cell_structure: CellStructure::Fixed(nz)`
/// for some `1 < nz < u32::MAX`.
pub struct QueryBuffersCellStructureFixed<'data, C>(QueryBuffers<'data, C>);

impl<'data, C> QueryBuffersCellStructureFixed<'data, C> {
    pub fn accept(value: &QueryBuffers<'data, C>) -> bool {
        matches!(&value.cell_structure, CellStructure::Fixed(ref nz) if nz.get() != 1)
    }
}

/// A set of `QueryBuffers` which is known to have `cell_structure: CellStructure::Var(_)`.
pub struct QueryBuffersCellStructureVar<'data, C>(QueryBuffers<'data, C>);

impl<'data, C> QueryBuffersCellStructureVar<'data, C> {
    pub fn accept(value: &QueryBuffers<'data, C>) -> bool {
        value.cell_structure.is_var()
    }
}

query_buffers_proof_impls!(
    QueryBuffersCellStructureSingle,
    QueryBuffersCellStructureFixed,
    QueryBuffersCellStructureVar
);

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

impl<'data> TypedQueryBuffers<'data> {
    pub fn values_capacity(&self) -> usize {
        crate::typed_query_buffers_go!(self, _DT, ref qb, qb.data.len())
    }

    pub fn cell_structure(&self) -> &CellStructure<'data> {
        crate::typed_query_buffers_go!(self, _DT, ref qb, &qb.cell_structure)
    }

    pub fn validity(&self) -> Option<&Buffer<'data, u8>> {
        crate::typed_query_buffers_go!(self, _DT, ref qb, qb.validity.as_ref())
    }

    pub fn borrow<'this>(&'this self) -> TypedQueryBuffers<'data>
    where
        'this: 'data,
    {
        crate::typed_query_buffers_go!(self, _DT, ref qb, qb.borrow().into())
    }
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
    ($expr:expr, $DT:ident, $inner:pat, $then:expr) => {{
        use $crate::query::buffer::TypedQueryBuffers;
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
    }};
    ($left:expr, $right:expr, $DT:ident, $lbind:pat, $rbind:pat, $then:expr) => {{
        use $crate::query::buffer::TypedQueryBuffers;
        match ($left, $right) {
            (
                TypedQueryBuffers::UInt8($lbind),
                TypedQueryBuffers::UInt8($rbind),
            ) => {
                type $DT = u8;
                $then
            }
            (
                TypedQueryBuffers::UInt16($lbind),
                TypedQueryBuffers::UInt16($rbind),
            ) => {
                type $DT = u16;
                $then
            }
            (
                TypedQueryBuffers::UInt32($lbind),
                TypedQueryBuffers::UInt32($rbind),
            ) => {
                type $DT = u32;
                $then
            }
            (
                TypedQueryBuffers::UInt64($lbind),
                TypedQueryBuffers::UInt64($rbind),
            ) => {
                type $DT = u64;
                $then
            }
            (
                TypedQueryBuffers::Int8($lbind),
                TypedQueryBuffers::Int8($rbind),
            ) => {
                type $DT = i8;
                $then
            }
            (
                TypedQueryBuffers::Int16($lbind),
                TypedQueryBuffers::Int16($rbind),
            ) => {
                type $DT = i16;
                $then
            }
            (
                TypedQueryBuffers::Int32($lbind),
                TypedQueryBuffers::Int32($rbind),
            ) => {
                type $DT = i32;
                $then
            }
            (
                TypedQueryBuffers::Int64($lbind),
                TypedQueryBuffers::Int64($rbind),
            ) => {
                type $DT = i64;
                $then
            }
            (
                TypedQueryBuffers::Float32($lbind),
                TypedQueryBuffers::Float32($rbind),
            ) => {
                type $DT = f32;
                $then
            }
            (
                TypedQueryBuffers::Float64($lbind),
                TypedQueryBuffers::Float64($rbind),
            ) => {
                type $DT = f64;
                $then
            }
            _ => unreachable!(),
        }
    }};
}

macro_rules! ref_typed_query_buffers_go {
    ($expr:expr, $DT:ident, $inner:pat, $then:expr) => {{
        use $crate::query::buffer::RefTypedQueryBuffersMut;
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
    }};
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
