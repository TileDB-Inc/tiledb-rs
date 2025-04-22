use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::iter::FusedIterator;
use std::num::{NonZeroU32, NonZeroUsize};

use anyhow::anyhow;

use crate::Result as TileDBResult;
use crate::array::CellValNum;
use crate::datatype::PhysicalType;
use crate::error::Error;
use crate::query::CellValue;
use crate::query::buffer::*;
use crate::{Datatype, typed_query_buffers_go};

#[cfg(feature = "arrow")]
pub mod arrow;

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;

#[derive(Debug)]
pub struct RawReadOutput<'data, C> {
    pub ncells: usize,
    pub input: QueryBuffers<'data, C>,
}

impl<C> RawReadOutput<'_, C> {
    pub fn nvalues(&self) -> usize {
        match self.input.cell_structure {
            CellStructure::Fixed(nz) => self.ncells * nz.get() as usize,
            CellStructure::Var(ref offsets) => {
                if offsets.is_empty() {
                    0
                } else {
                    assert!(self.ncells < offsets.len());
                    offsets.as_ref()[self.ncells] as usize
                }
            }
        }
    }

    pub fn nbytes(&self) -> usize {
        self.nvalues() * std::mem::size_of::<C>()
    }

    #[cfg(feature = "serde")]
    pub fn to_json(&self) -> serde_json::value::Value
    where
        C: Debug,
    {
        use serde_json::json;

        let cell_json = match self.input.cell_structure {
            CellStructure::Fixed(nz) => json!({"cell_val_num": nz}),
            CellStructure::Var(ref offsets) => json!({
                "capacity": offsets.len(),
                "defined": self.ncells + 1,
                "values": format!("{:?}", &offsets.as_ref()[0.. std::cmp::min(offsets.len(), self.ncells + 1)])
            }),
        };

        let validity_json = self.input.validity.as_ref().map(|validity| {
            json!({
                "capacity": validity.len(),
                "defined": self.ncells,
                "values": format!("{:?}", &validity.as_ref()[0.. self.ncells])
            })
        });

        json!({
            "ncells": self.ncells,
            "nbytes": self.nbytes(),
            "data": {
                "capacity": self.input.data.len(),
                "defined": self.nvalues(),
                "values": format!("{:?}", &self.input.data.as_ref()[0.. self.nvalues()])
            },
            "cell_structure": cell_json,
            "validity": validity_json,
        })
    }
}

#[derive(Debug)]
pub struct TypedRawReadOutput<'data> {
    pub datatype: Datatype,
    pub ncells: usize,
    pub buffers: TypedQueryBuffers<'data>,
}

impl<'data> TypedRawReadOutput<'data> {
    pub fn new<C>(datatype: Datatype, rr: RawReadOutput<'data, C>) -> Self
    where
        TypedQueryBuffers<'data>: From<QueryBuffers<'data, C>>,
    {
        TypedRawReadOutput {
            datatype,
            ncells: rr.ncells,
            buffers: rr.input.into(),
        }
    }

    pub fn nvalues(&self) -> usize {
        typed_query_buffers_go!(self.buffers, _DT, ref qb, {
            RawReadOutput {
                ncells: self.ncells,
                input: qb.borrow(),
            }
            .nvalues()
        })
    }

    pub fn is_nullable(&self) -> bool {
        typed_query_buffers_go!(
            self.buffers,
            _DT,
            ref qb,
            qb.validity.is_some()
        )
    }

    pub fn cell_structure(&self) -> &CellStructure<'data> {
        self.buffers.cell_structure()
    }
}

/// Represents either a fixed number of values per cell,
/// or the scratch space needed to write the offsets needed to determine
/// the variable number of values per cell.
pub enum ScratchCellStructure {
    Fixed(NonZeroU32),
    Var(Box<[u64]>),
}

impl From<NonZeroU32> for ScratchCellStructure {
    fn from(value: NonZeroU32) -> Self {
        Self::Fixed(value)
    }
}

impl ScratchCellStructure {
    /// Returns `ScratchCellStructure::Fixed(1)`, where each value is its own cell.
    pub fn single() -> Self {
        ScratchCellStructure::Fixed(NonZeroU32::new(1).unwrap())
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
        if let Self::Var(offsets) = self {
            Some(offsets.as_ref())
        } else {
            None
        }
    }

    /// Returns a mutable reference to the offsets buffer, if any.
    pub fn offsets_mut(&mut self) -> Option<&mut [u64]> {
        if let Self::Var(offsets) = self {
            Some(offsets.as_mut())
        } else {
            None
        }
    }
}

impl Default for ScratchCellStructure {
    /// Returns `ScratchCellStructure::single()`.
    fn default() -> Self {
        Self::single()
    }
}

impl TryFrom<CellStructure<'_>> for ScratchCellStructure {
    type Error = crate::error::Error;
    fn try_from(value: CellStructure) -> TileDBResult<Self> {
        match value {
            CellStructure::Fixed(nz) => Ok(Self::Fixed(nz)),
            CellStructure::Var(Buffer::Owned(offsets)) => {
                Ok(Self::Var(offsets))
            }
            CellStructure::Var(_) => Err(Error::InvalidArgument(anyhow!(
                "Cannot convert borrowed offsets buffer into owned scratch space"
            ))),
        }
    }
}

impl TryFrom<CellStructureMut<'_>> for ScratchCellStructure {
    type Error = crate::error::Error;
    fn try_from(value: CellStructureMut) -> TileDBResult<Self> {
        match value {
            CellStructureMut::Fixed(nz) => Ok(Self::Fixed(nz)),
            CellStructureMut::Var(BufferMut::Owned(offsets)) => {
                Ok(Self::Var(offsets))
            }
            CellStructureMut::Var(_) => Err(Error::InvalidArgument(anyhow!(
                "Cannot convert borrowed offsets buffer into owned scratch space"
            ))),
        }
    }
}

impl From<ScratchCellStructure> for CellStructure<'_> {
    fn from(value: ScratchCellStructure) -> Self {
        match value {
            ScratchCellStructure::Fixed(nz) => Self::Fixed(nz),
            ScratchCellStructure::Var(offsets) => {
                Self::Var(Buffer::Owned(offsets))
            }
        }
    }
}

impl From<ScratchCellStructure> for CellStructureMut<'_> {
    fn from(value: ScratchCellStructure) -> Self {
        match value {
            ScratchCellStructure::Fixed(nz) => Self::Fixed(nz),
            ScratchCellStructure::Var(offsets) => {
                Self::Var(BufferMut::Owned(offsets))
            }
        }
    }
}

pub struct ScratchSpace<C>(
    pub Box<[C]>,
    pub ScratchCellStructure,
    pub Option<Box<[u8]>>,
);

impl<'data, C> TryFrom<QueryBuffersMut<'data, C>> for ScratchSpace<C> {
    type Error = crate::error::Error;

    fn try_from(value: QueryBuffersMut<'data, C>) -> TileDBResult<Self> {
        let data = match value.data {
            BufferMut::Empty => vec![].into_boxed_slice(),
            BufferMut::Borrowed(_) => {
                return Err(Error::InvalidArgument(anyhow!(
                    "Cannot convert borrowed data into owned scratch space"
                )));
            }
            BufferMut::Owned(d) => d,
        };

        let cell_structure = value.cell_structure.try_into()?;

        let validity = if let Some(validity) = value.validity {
            Some(match validity {
                BufferMut::Empty => vec![].into_boxed_slice(),
                BufferMut::Borrowed(_) => {
                    return Err(Error::InvalidArgument(anyhow!(
                        "Cannot convert borrowed validity buffer into owned scratch space"
                    )));
                }
                BufferMut::Owned(d) => d,
            })
        } else {
            None
        };

        Ok(ScratchSpace(data, cell_structure, validity))
    }
}

impl<C> From<ScratchSpace<C>> for QueryBuffers<'_, C> {
    fn from(value: ScratchSpace<C>) -> Self {
        QueryBuffers {
            data: Buffer::Owned(value.0),
            cell_structure: CellStructure::from(value.1),
            validity: value.2.map(Buffer::Owned),
        }
    }
}

impl<C> From<ScratchSpace<C>> for QueryBuffersMut<'_, C> {
    fn from(value: ScratchSpace<C>) -> Self {
        QueryBuffersMut {
            data: BufferMut::Owned(value.0),
            cell_structure: CellStructureMut::from(value.1),
            validity: value.2.map(BufferMut::Owned),
        }
    }
}

pub trait ScratchAllocator<C> {
    fn alloc(&self) -> ScratchSpace<C>;
    fn realloc(&self, old: ScratchSpace<C>) -> ScratchSpace<C>;
}

#[derive(Clone, Debug)]
pub struct NonVarSized {
    pub cell_val_num: NonZeroU32,
    pub capacity: usize,
}

impl Default for NonVarSized {
    fn default() -> Self {
        NonVarSized {
            cell_val_num: NonZeroU32::new(1).unwrap(),
            capacity: 1024 * 1024,
        }
    }
}

impl<C> ScratchAllocator<C> for NonVarSized
where
    C: PhysicalType,
{
    fn alloc(&self) -> ScratchSpace<C> {
        eprintln!("ALLOC 1: {}", std::mem::size_of::<C>() * self.capacity);
        ScratchSpace(
            vec![C::default(); self.capacity].into_boxed_slice(),
            self.cell_val_num.into(),
            None,
        )
    }

    fn realloc(&self, old: ScratchSpace<C>) -> ScratchSpace<C> {
        let ScratchSpace(old, _, _) = old;

        let old_capacity = old.len();
        let new_capacity = 2 * (old_capacity + 1);

        let new_data = {
            let mut v = old.to_vec();
            v.resize(new_capacity, Default::default());
            v.into_boxed_slice()
        };

        eprintln!("REALLOC 1: {}", std::mem::size_of_val(&(*new_data)));

        ScratchSpace(new_data, self.cell_val_num.into(), None)
    }
}

#[derive(Clone, Debug)]
pub struct NullableNonVarSized {
    pub cell_val_num: NonZeroU32,
    pub data_capacity: usize,
    pub validity_capacity: usize,
}

impl Default for NullableNonVarSized {
    fn default() -> Self {
        NullableNonVarSized {
            cell_val_num: NonZeroU32::new(1).unwrap(),
            data_capacity: 1024 * 1024,
            validity_capacity: 1024 * 1024,
        }
    }
}

impl<C> ScratchAllocator<C> for NullableNonVarSized
where
    C: PhysicalType,
{
    fn alloc(&self) -> ScratchSpace<C> {
        eprintln!("ALLOC 2: {}", std::mem::size_of::<C>() * self.data_capacity);
        eprintln!("VALIDITY ALLOC: {}", self.validity_capacity);
        ScratchSpace(
            vec![C::default(); self.data_capacity].into_boxed_slice(),
            ScratchCellStructure::Fixed(self.cell_val_num),
            Some(vec![0u8; self.validity_capacity].into_boxed_slice()),
        )
    }

    fn realloc(&self, old: ScratchSpace<C>) -> ScratchSpace<C> {
        let ScratchSpace(old_data, _, old_validity) = old;

        let new_data = {
            let mut v = old_data.to_vec();
            v.resize(2 * v.len() + 1, Default::default());
            v.into_boxed_slice()
        };

        eprintln!("REALLOC 2: {}", std::mem::size_of_val(&(*new_data)));

        let new_validity = {
            let mut v = old_validity.unwrap().to_vec();
            v.resize(2 * v.len() + 1, 0u8);
            v.into_boxed_slice()
        };

        ScratchSpace(new_data, self.cell_val_num.into(), Some(new_validity))
    }
}

#[derive(Clone, Debug)]
pub struct VarSized {
    pub byte_capacity: usize,
    pub offset_capacity: usize,
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
    C: PhysicalType,
{
    fn alloc(&self) -> ScratchSpace<C> {
        eprintln!("ALLOC 3: {}", std::mem::size_of::<C>() * self.byte_capacity);
        eprintln!("OFFSETS ALLOC: {}", 8 * self.offset_capacity);
        let data = vec![C::default(); self.byte_capacity].into_boxed_slice();
        let offsets = vec![0u64; self.offset_capacity].into_boxed_slice();
        ScratchSpace(data, ScratchCellStructure::Var(offsets), None)
    }

    fn realloc(&self, old: ScratchSpace<C>) -> ScratchSpace<C> {
        let ScratchSpace(old_data, old_structure, _) = old;

        let new_data = {
            let mut v = old_data.to_vec();
            v.resize(2 * v.len() + 1, Default::default());
            v.into_boxed_slice()
        };

        eprintln!("REALLOC 3: {}", std::mem::size_of_val(&(*new_data)));

        let new_structure = match old_structure {
            ScratchCellStructure::Fixed(nz) => ScratchCellStructure::Fixed(nz),
            ScratchCellStructure::Var(old_offsets) => {
                let mut v = old_offsets.into_vec();
                v.resize(2 * v.len() + 1, Default::default());
                ScratchCellStructure::Var(v.into_boxed_slice())
            }
        };

        ScratchSpace(new_data, new_structure, None)
    }
}

#[derive(Clone, Debug)]
pub struct NullableVarSized {
    pub byte_capacity: usize,
    pub offset_capacity: usize,
    pub validity_capacity: usize,
}

impl Default for NullableVarSized {
    fn default() -> Self {
        const DEFAULT_BYTE_CAPACITY: usize = 1024 * 1024;
        const DEFAULT_RECORD_CAPACITY: usize = 256 * 1024;

        NullableVarSized {
            byte_capacity: DEFAULT_BYTE_CAPACITY,
            offset_capacity: DEFAULT_RECORD_CAPACITY,
            validity_capacity: DEFAULT_RECORD_CAPACITY,
        }
    }
}

impl<C> ScratchAllocator<C> for NullableVarSized
where
    C: PhysicalType,
{
    fn alloc(&self) -> ScratchSpace<C> {
        eprintln!("ALLOC 4: {}", std::mem::size_of::<C>() * self.byte_capacity);
        eprintln!("OFFSETS ALLOC: {}", 8 * self.offset_capacity);
        eprintln!("VALIDITY ALLOC: {}", self.validity_capacity);
        let data = vec![C::default(); self.byte_capacity].into_boxed_slice();
        let offsets = vec![0u64; self.offset_capacity].into_boxed_slice();
        let validity = vec![0u8; self.validity_capacity].into_boxed_slice();
        ScratchSpace(data, ScratchCellStructure::Var(offsets), Some(validity))
    }

    fn realloc(&self, old: ScratchSpace<C>) -> ScratchSpace<C> {
        let ScratchSpace(old_data, old_structure, old_validity) = old;

        let new_data = {
            let mut v = old_data.to_vec();
            v.resize(2 * v.len() + 1, Default::default());
            v.into_boxed_slice()
        };

        eprintln!("REALLOC 4: {}", std::mem::size_of_val(&(*new_data)));

        let new_structure = match old_structure {
            ScratchCellStructure::Fixed(nz) => ScratchCellStructure::Fixed(nz),
            ScratchCellStructure::Var(old_offsets) => {
                let mut v = old_offsets.into_vec();
                v.resize(2 * v.len() + 1, Default::default());
                ScratchCellStructure::Var(v.into_boxed_slice())
            }
        };

        let new_validity = {
            let mut v = old_validity.unwrap().into_vec();
            v.resize(2 * v.len() + 1, Default::default());
            v.into_boxed_slice()
        };

        ScratchSpace(new_data, new_structure, Some(new_validity))
    }
}

/// Allocator for a schema field of any shape.
// Note that we don't need bytes per value because the user
// will be registering a buffer of appropriate primitive type.
pub struct FieldScratchAllocator {
    pub cell_val_num: CellValNum,
    pub record_capacity: NonZeroUsize,
    pub is_nullable: bool,
}

impl FieldScratchAllocator {
    pub const DEFAULT_MEMORY_LIMIT: usize = 64 * 1024 * 1024;
}

impl<C> ScratchAllocator<C> for FieldScratchAllocator
where
    C: PhysicalType,
{
    fn alloc(&self) -> ScratchSpace<C> {
        let (byte_capacity, cell_structure) = match self.cell_val_num {
            CellValNum::Fixed(values_per_record) => {
                let byte_capacity = self.record_capacity.get()
                    * values_per_record.get() as usize;
                (
                    byte_capacity,
                    ScratchCellStructure::Fixed(values_per_record),
                )
            }
            CellValNum::Var => {
                let values_per_record = 64; /* TODO: get some kind of hint from the schema */
                let byte_capacity =
                    self.record_capacity.get() * values_per_record;
                (
                    byte_capacity,
                    ScratchCellStructure::Var(
                        vec![0u64; self.record_capacity.get()]
                            .into_boxed_slice(),
                    ),
                )
            }
        };

        eprintln!("ALLOC 5: {}", std::mem::size_of::<C>() * byte_capacity);
        eprintln!("VALIDITY ALLOC: {}", self.record_capacity.get());

        let data = vec![C::default(); byte_capacity].into_boxed_slice();
        let validity = if self.is_nullable {
            Some(vec![0u8; self.record_capacity.get()].into_boxed_slice())
        } else {
            None
        };

        ScratchSpace(data, cell_structure, validity)
    }

    fn realloc(&self, old: ScratchSpace<C>) -> ScratchSpace<C> {
        let ScratchSpace(old_data, old_structure, old_validity) = old;

        let new_data = {
            let mut v = old_data.to_vec();
            v.resize(2 * v.len(), Default::default());
            v.into_boxed_slice()
        };

        eprintln!("REALLOC 5: {}", std::mem::size_of_val(&(*new_data)));

        let new_structure = match old_structure {
            ScratchCellStructure::Fixed(nz) => ScratchCellStructure::Fixed(nz),
            ScratchCellStructure::Var(old_offsets) => {
                let mut v = old_offsets.to_vec();
                v.resize(2 * v.len(), Default::default());
                ScratchCellStructure::Var(v.into_boxed_slice())
            }
        };

        let new_validity = old_validity.map(|old_validity| {
            let mut v = old_validity.to_vec();
            v.resize(2 * v.len(), Default::default());
            v.into_boxed_slice()
        });

        ScratchSpace(new_data, new_structure, new_validity)
    }
}

pub struct CellStructureSingleIterator<'data, C> {
    ncells: usize,
    index: usize,
    location: QueryBuffers<'data, C>,
}

impl<'data, C> CellStructureSingleIterator<'data, C> {
    pub fn new(
        ncells: usize,
        input: QueryBuffersCellStructureSingle<'data, C>,
    ) -> Self {
        CellStructureSingleIterator {
            ncells,
            index: 0,
            location: input.into_inner(),
        }
    }

    pub fn try_new(
        ncells: usize,
        input: QueryBuffers<'data, C>,
    ) -> TileDBResult<Self> {
        match QueryBuffersCellStructureSingle::try_from(input) {
            Ok(qb) => Ok(Self::new(ncells, qb)),
            Err(qb) => Err(Error::UnexpectedCellStructure {
                expected: CellValNum::single(),
                found: qb.cell_structure.as_cell_val_num(),
            }),
        }
    }
}

impl<C> Iterator for CellStructureSingleIterator<'_, C>
where
    C: Copy,
{
    type Item = C;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.ncells {
            self.index += 1;
            Some(self.location.data[self.index - 1])
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.ncells - self.index;
        (exact, Some(exact))
    }
}

impl<C> FusedIterator for CellStructureSingleIterator<'_, C> where C: Copy {}

impl<'data, C> TryFrom<RawReadOutput<'data, C>>
    for CellStructureSingleIterator<'data, C>
{
    type Error = crate::error::Error;
    fn try_from(value: RawReadOutput<'data, C>) -> TileDBResult<Self> {
        Self::try_new(value.ncells, value.input)
    }
}

/// A set of `QueryBuffers` which can be correctly used by `FixedDataIterator`.
///
/// A `QueryBuffers` instance can be wrapped this way if it has
/// `cell_structure: CellStructure::Fixed(nz)` for some `1 < nz < u32::MAX`,
/// and also does not own the `data` buffer.
pub struct QueryBuffersFixedDataIterable<'data, C>(QueryBuffers<'data, C>);

impl<'data, C> QueryBuffersFixedDataIterable<'data, C> {
    pub fn accept(value: &QueryBuffers<'data, C>) -> bool {
        QueryBuffersCellStructureFixed::accept(value)
            && !matches!(value.data, Buffer::Owned(_))
    }
}

/// A set of `QueryBuffers` which can be correctly used by `VarDataIterator`.
///
/// A `QueryBuffers` instance can be wrapped this way if it has
/// `cell_structure: CellStructure::Var(_)`
/// and also does not own the `data` buffer.
pub struct QueryBuffersVarDataIterable<'data, C>(QueryBuffers<'data, C>);

impl<'data, C> QueryBuffersVarDataIterable<'data, C> {
    fn accept(value: &QueryBuffers<'data, C>) -> bool {
        QueryBuffersCellStructureVar::accept(value)
            && !matches!(value.data, Buffer::Owned(_))
    }
}

query_buffers_proof_impls!(
    QueryBuffersFixedDataIterable,
    QueryBuffersVarDataIterable
);

pub struct FixedDataIterator<'data, C> {
    ncells: usize,
    index: usize,
    location: QueryBuffers<'data, C>,
}

impl<'data, C> FixedDataIterator<'data, C> {
    pub fn new(
        ncells: usize,
        input: QueryBuffersFixedDataIterable<'data, C>,
    ) -> Self {
        FixedDataIterator {
            ncells,
            index: 0,
            location: input.into_inner(),
        }
    }

    pub fn try_new(
        ncells: usize,
        input: QueryBuffers<'data, C>,
    ) -> TileDBResult<Self> {
        match QueryBuffersFixedDataIterable::try_from(input) {
            Ok(qb) => Ok(Self::new(ncells, qb)),
            Err(input) => {
                if matches!(input.data, Buffer::Owned(_)) {
                    Err(Error::InvalidArgument(anyhow!(
                        "FixedDataIterator cannot take ownership of data inside QueryBuffers"
                    )))
                } else {
                    assert!(!QueryBuffersCellStructureFixed::accept(&input));
                    Err(Error::UnexpectedCellStructure {
                        expected: CellValNum::single(), /* TODO: this is not really accurate, any Fixed */
                        found: input.cell_structure.as_cell_val_num(),
                    })
                }
            }
        }
    }
}

impl<'data, C> Iterator for FixedDataIterator<'data, C> {
    type Item = &'data [C];
    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.ncells {
            self.index += 1;
            match self.location.cell_structure {
                CellStructure::Fixed(nz) => {
                    let len = nz.get() as usize;
                    let lb = (self.index - 1) * len;
                    let ub = self.index * len;

                    /* this is not sound for the same reason as VarDataIterator::next() */
                    let data: &'data [C] = unsafe {
                        &*(self.location.data.as_ref() as *const [C])
                            as &'data [C]
                    };
                    Some(&data[lb..ub])
                }
                _ => unreachable!(),
            }
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.ncells - self.index;
        (exact, Some(exact))
    }
}

impl<C> FusedIterator for FixedDataIterator<'_, C> where C: Copy {}

impl<'data, C> TryFrom<RawReadOutput<'data, C>>
    for FixedDataIterator<'data, C>
{
    type Error = crate::error::Error;
    fn try_from(value: RawReadOutput<'data, C>) -> TileDBResult<Self> {
        Self::try_new(value.ncells, value.input)
    }
}

/// Iterator which yields variable-sized records from a raw read result.
pub struct VarDataIterator<'data, C> {
    ncells: usize,
    offset_cursor: usize,
    location: QueryBuffers<'data, C>,
}

impl<'data, C> VarDataIterator<'data, C> {
    pub fn new(
        ncells: usize,
        location: QueryBuffersVarDataIterable<'data, C>,
    ) -> Self {
        VarDataIterator {
            ncells,
            offset_cursor: 0,
            location: location.into_inner(),
        }
    }

    pub fn try_new(
        ncells: usize,
        location: QueryBuffers<'data, C>,
    ) -> TileDBResult<Self> {
        match QueryBuffersVarDataIterable::try_from(location) {
            Ok(qb) => Ok(Self::new(ncells, qb)),
            Err(input) => {
                if matches!(input.data, Buffer::Owned(_)) {
                    Err(Error::InvalidArgument(anyhow!(
                        "VarDataIterator cannot take ownership of data inside QueryBuffers"
                    )))
                } else {
                    assert!(!QueryBuffersCellStructureVar::accept(&input));
                    Err(Error::UnexpectedCellStructure {
                        expected: CellValNum::Var,
                        found: input.cell_structure.as_cell_val_num(),
                    })
                }
            }
        }
    }
}

impl<C> Debug for VarDataIterator<'_, C>
where
    C: Debug,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let cell_offsets = self.location.cell_structure.offsets_ref().unwrap();
        let nbytes = cell_offsets[self.ncells] as usize;
        write!(
            f,
            "VarDataIterator {{ cursor: {}, offsets: {:?}, bytes: {:?} }}",
            self.offset_cursor,
            &cell_offsets[0..=self.ncells],
            &self.location.data.as_ref()[0..nbytes]
        )
    }
}

impl<'data, C> Iterator for VarDataIterator<'data, C> {
    type Item = &'data [C];

    fn next(&mut self) -> Option<Self::Item> {
        let data_buffer: &'data [C] = unsafe {
            /*
             * If `self.location.data` is `Buffer::Owned`, then the underlying
             * data will be dropped when `self` is. The 'data item could live
             * longer, but would have been dropped. This is undefined behavior.
             *
             * If `self.location.data` is `Buffer::Borrowed`, then the underlying
             * data will be dropped when 'data expires, are returned items
             * are guaranteed to live in the 'data lifetime. This is safe.
             *
             * The constructor requires `QueryBuffersVarDataIterable` which
             * requires `Buffer::Borrowed` for each buffer.
             * We will never see `Buffer::Owned` here, hence this is safe.
             */
            &*(self.location.data.as_ref() as *const [C]) as &'data [C]
        };
        let offset_buffer = self.location.cell_structure.offsets_ref().unwrap();

        let s = self.offset_cursor;
        self.offset_cursor += 1;

        if s < self.ncells {
            let start = offset_buffer[s] as usize;
            let end = offset_buffer[s + 1] as usize;
            Some(&data_buffer[start..end])
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.ncells - self.offset_cursor;
        (exact, Some(exact))
    }
}

impl<C> FusedIterator for VarDataIterator<'_, C> {}

impl<'data, C> TryFrom<RawReadOutput<'data, C>> for VarDataIterator<'data, C> {
    type Error = crate::error::Error;
    fn try_from(value: RawReadOutput<'data, C>) -> TileDBResult<Self> {
        Self::try_new(value.ncells, value.input)
    }
}

pub struct Utf8LossyIterator<'data> {
    var: VarDataIterator<'data, u8>,
}

impl<'data> TryFrom<RawReadOutput<'data, u8>> for Utf8LossyIterator<'data> {
    type Error = crate::error::Error;
    fn try_from(value: RawReadOutput<'data, u8>) -> TileDBResult<Self> {
        Ok(Utf8LossyIterator {
            var: VarDataIterator::try_from(value)?,
        })
    }
}

impl Iterator for Utf8LossyIterator<'_> {
    type Item = String;
    fn next(&mut self) -> Option<Self::Item> {
        self.var
            .next()
            .map(|s| String::from_utf8_lossy(s).to_string())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.var.size_hint()
    }
}

impl FusedIterator for Utf8LossyIterator<'_> {}

pub trait FromQueryOutput: Sized {
    type Unit;
    type Iterator<'data>: Iterator<Item = Self>
        + TryFrom<RawReadOutput<'data, Self::Unit>, Error = crate::error::Error>
    where
        Self::Unit: 'data;
}

impl<C> FromQueryOutput for C
where
    C: CellValue,
{
    type Unit = C;
    type Iterator<'data>
        = CellStructureSingleIterator<'data, Self::Unit>
    where
        C: 'data;
}

impl FromQueryOutput for String {
    type Unit = u8;
    type Iterator<'data> = Utf8LossyIterator<'data>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::buffer::Buffer;

    #[test]
    fn cell_val_num_single_iterator() {
        let bufs: QueryBuffers<u64> = QueryBuffers {
            data: std::iter::repeat(())
                .enumerate()
                .map(|(i, _)| i as u64)
                .take(1024)
                .collect::<Vec<u64>>()
                .into(),
            cell_structure: CellStructure::single(),
            validity: None,
        };

        {
            let vals = CellStructureSingleIterator::try_new(1, bufs.borrow())
                .unwrap()
                .collect::<Vec<u64>>();
            assert_eq!(vals, vec![0]);
        }
        {
            let vals = CellStructureSingleIterator::try_new(10, bufs.borrow())
                .unwrap()
                .collect::<Vec<u64>>();
            assert_eq!(vals, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        }
    }

    #[test]
    fn cell_val_num_fixed_iterator() {
        let bufs: QueryBuffers<u64> = QueryBuffers {
            data: std::iter::repeat(())
                .enumerate()
                .map(|(i, _)| i as u64)
                .take(1024)
                .collect::<Vec<u64>>()
                .into(),
            cell_structure: CellStructure::from(NonZeroU32::new(4).unwrap()),
            validity: None,
        };

        {
            let vals = FixedDataIterator::try_new(1, bufs.borrow())
                .unwrap()
                .collect::<Vec<&[u64]>>();
            assert_eq!(vals, vec![&[0, 1, 2, 3]]);
        }
        {
            let vals = FixedDataIterator::try_new(3, bufs.borrow())
                .unwrap()
                .collect::<Vec<&[u64]>>();
            assert_eq!(
                vals,
                vec![&[0, 1, 2, 3], &[4, 5, 6, 7], &[8, 9, 10, 11]]
            );
        }
    }

    #[test]
    fn test_fixed_data_iterator_lifetime() {
        let data = vec![0u8, 1, 2, 3, 4, 5, 6, 7];

        let mut databuf = Buffer::Borrowed(&data);

        let _ = std::mem::replace(
            &mut databuf,
            Buffer::Owned(vec![1u8; 16].into_boxed_slice()),
        );

        let try_new = FixedDataIterator::try_new(
            2,
            QueryBuffers {
                data: databuf,
                cell_structure: CellStructure::from(
                    NonZeroU32::new(4).unwrap(),
                ),
                validity: None,
            },
        );
        assert!(matches!(try_new, Err(Error::InvalidArgument(_))));
    }

    #[test]
    fn test_var_data_iterator_lifetime() {
        let data = vec![0u8; 16]; // not important
        let offsets = vec![0u64, 4, 8, 12, data.len() as u64];

        let mut databuf = Buffer::Borrowed(&data);

        let _ = std::mem::replace(
            &mut databuf,
            Buffer::Owned(vec![1u8; 16].into_boxed_slice()),
        );

        let try_new = VarDataIterator::try_new(
            offsets.len(),
            QueryBuffers {
                data: databuf,
                cell_structure: CellStructure::Var(offsets.into()),
                validity: None,
            },
        );
        assert!(matches!(try_new, Err(Error::InvalidArgument(_))));
    }
}
