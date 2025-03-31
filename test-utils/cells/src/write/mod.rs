#[cfg(feature = "proptest-strategies")]
pub mod strategy;

use std::ops::Deref;

use tiledb_common::array::{CellOrder, CellValNum};
use tiledb_common::datatype::physical::BitsOrd;
use tiledb_common::range::{NonEmptyDomain, Range, SingleValueRange};
use tiledb_pod::array::schema::SchemaData;

use crate::{Cells, typed_field_data_go};

#[derive(Clone, Debug)]
pub struct DenseWriteInput {
    pub layout: CellOrder,
    pub data: Cells,
    pub subarray: Vec<SingleValueRange>,
}

#[derive(Clone, Debug)]
pub struct SparseWriteInput {
    pub dimensions: Vec<(String, CellValNum)>,
    pub data: Cells,
}

impl SparseWriteInput {
    pub fn from_schema_and_data(schema: &SchemaData, data: Cells) -> Self {
        let dimensions = schema
            .domain
            .dimension
            .iter()
            .map(|d| (d.name.clone(), d.cell_val_num()))
            .collect::<Vec<_>>();
        SparseWriteInput { dimensions, data }
    }

    /// Returns the minimum bounding rectangle containing all
    /// the coordinates of this write operation.
    pub fn domain(&self) -> Option<NonEmptyDomain> {
        self.dimensions
            .iter()
            .map(|(dim, cell_val_num)| {
                let dim_cells = self.data.fields().get(dim).unwrap();
                Some(typed_field_data_go!(
                    dim_cells,
                    _DT,
                    dim_cells,
                    {
                        let min =
                            *dim_cells.iter().min_by(|l, r| l.bits_cmp(r))?;
                        let max =
                            *dim_cells.iter().max_by(|l, r| l.bits_cmp(r))?;
                        Range::from(&[min, max])
                    },
                    {
                        let min = dim_cells
                            .iter()
                            .min_by(|l, r| l.bits_cmp(r))?
                            .clone()
                            .into_boxed_slice();
                        let max = dim_cells
                            .iter()
                            .max_by(|l, r| l.bits_cmp(r))?
                            .clone()
                            .into_boxed_slice();
                        match cell_val_num {
                            CellValNum::Fixed(_) => {
                                Range::try_from((*cell_val_num, min, max))
                                    .unwrap()
                            }
                            CellValNum::Var => Range::from((min, max)),
                        }
                    }
                ))
            })
            .collect::<Option<NonEmptyDomain>>()
    }

    /// Sort the data cells using the dimensions as sort keys, in order.
    pub fn sort_cells(&mut self) {
        let keys = self
            .dimensions
            .iter()
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>();
        self.data.sort(&keys)
    }
}

#[derive(Debug)]
pub struct DenseWriteSequence {
    pub writes: Vec<DenseWriteInput>,
}

impl DenseWriteSequence {
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut DenseWriteInput> {
        self.writes.iter_mut()
    }
}

impl Deref for DenseWriteSequence {
    type Target = Vec<DenseWriteInput>;
    fn deref(&self) -> &Self::Target {
        &self.writes
    }
}

impl<T> From<T> for DenseWriteSequence
where
    T: Into<Vec<DenseWriteInput>>,
{
    fn from(value: T) -> Self {
        DenseWriteSequence {
            writes: value.into(),
        }
    }
}

impl IntoIterator for DenseWriteSequence {
    type Item = DenseWriteInput;
    type IntoIter = <Vec<Self::Item> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.writes.into_iter()
    }
}

impl FromIterator<DenseWriteInput> for DenseWriteSequence {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = DenseWriteInput>,
    {
        DenseWriteSequence {
            writes: iter.into_iter().collect::<Vec<_>>(),
        }
    }
}

#[derive(Debug)]
pub struct SparseWriteSequence {
    pub writes: Vec<SparseWriteInput>,
}

impl SparseWriteSequence {
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut SparseWriteInput> {
        self.writes.iter_mut()
    }
}

impl Deref for SparseWriteSequence {
    type Target = Vec<SparseWriteInput>;
    fn deref(&self) -> &Self::Target {
        &self.writes
    }
}

impl<T> From<T> for SparseWriteSequence
where
    T: Into<Vec<SparseWriteInput>>,
{
    fn from(value: T) -> Self {
        SparseWriteSequence {
            writes: value.into(),
        }
    }
}

impl IntoIterator for SparseWriteSequence {
    type Item = SparseWriteInput;
    type IntoIter = <Vec<Self::Item> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.writes.into_iter()
    }
}

impl FromIterator<SparseWriteInput> for SparseWriteSequence {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = SparseWriteInput>,
    {
        SparseWriteSequence {
            writes: iter.into_iter().collect::<Vec<_>>(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum WriteInput {
    Dense(DenseWriteInput),
    Sparse(SparseWriteInput),
}

impl WriteInput {
    /// Returns a reference to the cells of input of this write operation.
    pub fn cells(&self) -> &Cells {
        match self {
            Self::Dense(dense) => &dense.data,
            Self::Sparse(sparse) => &sparse.data,
        }
    }

    /// Returns a mutable reference to the cells of input of this write operation.
    pub fn cells_mut(&mut self) -> &mut Cells {
        match self {
            Self::Dense(dense) => &mut dense.data,
            Self::Sparse(sparse) => &mut sparse.data,
        }
    }

    /// Returns the minimum bounding rectangle containing
    /// the coordinates of this write operation.
    pub fn domain(&self) -> Option<NonEmptyDomain> {
        match self {
            Self::Dense(dense) => Some(
                dense
                    .subarray
                    .clone()
                    .into_iter()
                    .map(Range::from)
                    .collect::<NonEmptyDomain>(),
            ),
            Self::Sparse(sparse) => sparse.domain(),
        }
    }

    /// Returns the subarray for this write operation,
    /// if it is a dense write. Returns `None` otherwise.
    pub fn subarray(&self) -> Option<NonEmptyDomain> {
        if let Self::Dense(_) = self {
            self.domain()
        } else {
            None
        }
    }

    /// Consumes `self` and returns the underlying test data.
    pub fn unwrap_cells(self) -> Cells {
        match self {
            Self::Dense(dense) => dense.data,
            Self::Sparse(sparse) => sparse.data,
        }
    }
}

pub enum WriteInputRef<'a> {
    Dense(&'a DenseWriteInput),
    Sparse(&'a SparseWriteInput),
}

impl WriteInputRef<'_> {
    /// Returns a reference to the cells of input of this write operation.
    pub fn cells(&self) -> &Cells {
        match self {
            Self::Dense(dense) => &dense.data,
            Self::Sparse(sparse) => &sparse.data,
        }
    }

    pub fn cloned(&self) -> WriteInput {
        match self {
            Self::Dense(dense) => WriteInput::Dense((*dense).clone()),
            Self::Sparse(sparse) => WriteInput::Sparse((*sparse).clone()),
        }
    }

    /// Returns the minimum bounding rectangle containing
    /// the coordinates of this write operation.
    pub fn domain(&self) -> Option<NonEmptyDomain> {
        match self {
            Self::Dense(dense) => Some(
                dense
                    .subarray
                    .clone()
                    .into_iter()
                    .map(Range::from)
                    .collect::<NonEmptyDomain>(),
            ),
            Self::Sparse(sparse) => sparse.domain(),
        }
    }

    /// Returns the subarray for this write operation,
    /// if it is a dense write. Returns `None` otherwise.
    pub fn subarray(&self) -> Option<NonEmptyDomain> {
        if let Self::Dense(_) = self {
            self.domain()
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub enum WriteSequence {
    Dense(DenseWriteSequence),
    Sparse(SparseWriteSequence),
}

impl WriteSequence {
    pub fn iter(&self) -> WriteSequenceRefIter {
        self.into_iter()
    }
}

impl From<WriteInput> for WriteSequence {
    fn from(value: WriteInput) -> Self {
        match value {
            WriteInput::Dense(dense) => Self::Dense(DenseWriteSequence {
                writes: vec![dense],
            }),
            WriteInput::Sparse(sparse) => Self::Sparse(SparseWriteSequence {
                writes: vec![sparse],
            }),
        }
    }
}

impl IntoIterator for WriteSequence {
    type Item = WriteInput;
    type IntoIter = WriteSequenceIter;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::Dense(dense) => WriteSequenceIter::Dense(dense.into_iter()),
            Self::Sparse(sparse) => {
                WriteSequenceIter::Sparse(sparse.into_iter())
            }
        }
    }
}

impl<'a> IntoIterator for &'a WriteSequence {
    type Item = WriteInputRef<'a>;
    type IntoIter = WriteSequenceRefIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match *self {
            WriteSequence::Dense(ref dense) => {
                WriteSequenceRefIter::Dense(dense.iter())
            }
            WriteSequence::Sparse(ref sparse) => {
                WriteSequenceRefIter::Sparse(sparse.iter())
            }
        }
    }
}

pub enum WriteSequenceIter {
    Dense(<DenseWriteSequence as IntoIterator>::IntoIter),
    Sparse(<SparseWriteSequence as IntoIterator>::IntoIter),
}

impl Iterator for WriteSequenceIter {
    type Item = WriteInput;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Dense(dense) => dense.next().map(WriteInput::Dense),
            Self::Sparse(sparse) => sparse.next().map(WriteInput::Sparse),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Dense(d) => d.size_hint(),
            Self::Sparse(s) => s.size_hint(),
        }
    }
}

pub enum WriteSequenceRefIter<'a> {
    Dense(<&'a Vec<DenseWriteInput> as IntoIterator>::IntoIter),
    Sparse(<&'a Vec<SparseWriteInput> as IntoIterator>::IntoIter),
}

impl<'a> Iterator for WriteSequenceRefIter<'a> {
    type Item = WriteInputRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Dense(dense) => dense.next().map(WriteInputRef::Dense),
            Self::Sparse(sparse) => sparse.next().map(WriteInputRef::Sparse),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Dense(d) => d.size_hint(),
            Self::Sparse(s) => s.size_hint(),
        }
    }
}
