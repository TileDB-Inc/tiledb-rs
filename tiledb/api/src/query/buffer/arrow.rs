use std::num::TryFromIntError;
use std::sync::Arc;

use arrow::array::{
    Array as ArrowArray, FixedSizeListArray, GenericListArray,
    LargeBinaryArray, PrimitiveArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{ArrowNativeType, Field};

use crate::array::CellValNum;
use crate::datatype::arrow::ArrowPrimitiveTypeNative;
use crate::query::buffer::{
    Buffer, BufferMut, QueryBuffers, QueryBuffersCellStructureFixed,
    QueryBuffersCellStructureSingle, QueryBuffersCellStructureVar,
};

pub type PrimitiveArrayAlias<C> =
    PrimitiveArray<<C as ArrowPrimitiveTypeNative>::ArrowPrimitiveType>;

pub struct Celled<B>(pub usize, pub B);

impl<B> Celled<B> {
    pub fn ncells(&self) -> usize {
        self.0
    }
}

impl<B> From<(usize, B)> for Celled<B> {
    fn from(value: (usize, B)) -> Self {
        Self(value.0, value.1)
    }
}

impl<C> From<Celled<Buffer<'_, C>>> for ScalarBuffer<C>
where
    C: ArrowNativeType,
{
    fn from(value: Celled<Buffer<C>>) -> Self {
        let Celled(ncells, value) = value;
        let mut v: Vec<C> = match value {
            Buffer::Empty => vec![],
            Buffer::Owned(b) => b.into_vec(),
            Buffer::Borrowed(b) => b.to_vec(),
        };
        v.truncate(ncells);
        v.into()
    }
}

impl<C> From<Celled<BufferMut<'_, C>>> for ScalarBuffer<C>
where
    C: ArrowNativeType,
{
    fn from(value: Celled<BufferMut<C>>) -> Self {
        let Celled(ncells, value) = value;
        match value {
            BufferMut::Owned(data) => {
                ScalarBuffer::from(Celled(ncells, Buffer::Owned(data)))
            }
            value => ScalarBuffer::from(Celled(ncells, value.borrow())),
        }
    }
}

impl From<Celled<Buffer<'_, u8>>> for NullBuffer {
    fn from(value: Celled<Buffer<'_, u8>>) -> Self {
        let Celled(ncells, validity) = value;

        let validity = match validity {
            Buffer::Empty => vec![],
            Buffer::Owned(v) => {
                let mut v = v.into_vec();
                v.truncate(ncells);
                v
            }
            Buffer::Borrowed(v) => v[0..ncells].to_vec(),
        };
        validity
            .into_iter()
            .map(|v| v != 0)
            .collect::<arrow::buffer::NullBuffer>()
    }
}

impl<C> From<Celled<QueryBuffersCellStructureSingle<'_, C>>>
    for PrimitiveArrayAlias<C>
where
    C: ArrowPrimitiveTypeNative,
{
    fn from(value: Celled<QueryBuffersCellStructureSingle<C>>) -> Self {
        let Celled(ncells, buffers) = value;

        let values = ScalarBuffer::<C>::from(Celled(ncells, buffers.0.data));

        let validity = buffers
            .0
            .validity
            .map(|v| NullBuffer::from(Celled(ncells, v)));

        PrimitiveArrayAlias::<C>::new(values, validity)
    }
}

pub struct QueryBuffersCellStructureFixedArrowArray<C>(
    FixedSizeListArray,
    Arc<PrimitiveArray<<C as ArrowPrimitiveTypeNative>::ArrowPrimitiveType>>,
)
where
    C: ArrowPrimitiveTypeNative;

impl<C> QueryBuffersCellStructureFixedArrowArray<C>
where
    C: ArrowPrimitiveTypeNative,
{
    /// Returns the parent `FixedSizeListArray`.
    pub fn list(&self) -> &FixedSizeListArray {
        &self.0
    }

    /// Returns the child values array.
    pub fn values(&self) -> &Arc<PrimitiveArrayAlias<C>> {
        &self.1
    }

    pub fn boxed(self) -> Box<dyn ArrowArray> {
        Box::new(self.0)
    }

    pub fn unwrap(self) -> (FixedSizeListArray, Arc<PrimitiveArrayAlias<C>>) {
        (self.0, self.1)
    }
}

impl<C> TryFrom<Celled<QueryBuffersCellStructureFixed<'_, C>>>
    for QueryBuffersCellStructureFixedArrowArray<C>
where
    C: ArrowPrimitiveTypeNative,
{
    /// If the cell structure fixed list size is greater than `i32::MAX`,
    /// then the conversion fails. Otherwise it succeeds.
    type Error = std::num::TryFromIntError;

    fn try_from(
        value: Celled<QueryBuffersCellStructureFixed<C>>,
    ) -> Result<Self, Self::Error> {
        let Celled(ncells, buffers) = value;

        let values = ScalarBuffer::<C>::from(Celled(ncells, buffers.0.data));
        let values = PrimitiveArrayAlias::<C>::new(values, None);

        let validity = buffers
            .0
            .validity
            .map(|v| NullBuffer::from(Celled(ncells, v)));

        let field = Field::new_list_field(values.data_type().clone(), false);

        let nz = buffers.0.cell_structure.fixed().unwrap();
        let fixed_len = i32::try_from(nz.get())?;

        let values = Arc::new(values);
        let fl = FixedSizeListArray::new(
            Arc::new(field),
            fixed_len,
            values.clone(),
            validity,
        );
        Ok(QueryBuffersCellStructureFixedArrowArray(fl, values))
    }
}

pub struct QueryBuffersCellStructureVarArrowArray<C>(
    GenericListArray<i64>,
    Arc<PrimitiveArrayAlias<C>>,
)
where
    C: ArrowPrimitiveTypeNative;

impl<C> QueryBuffersCellStructureVarArrowArray<C>
where
    C: ArrowPrimitiveTypeNative,
{
    /// Returns the parent `GenericListArray`.
    pub fn list(&self) -> &GenericListArray<i64> {
        &self.0
    }

    /// Returns the child values array.
    pub fn values(&self) -> &Arc<PrimitiveArrayAlias<C>> {
        &self.1
    }

    pub fn boxed(self) -> Box<dyn ArrowArray> {
        Box::new(self.0)
    }

    pub fn unwrap(
        self,
    ) -> (GenericListArray<i64>, Arc<PrimitiveArrayAlias<C>>) {
        (self.0, self.1)
    }
}

impl<C> TryFrom<Celled<QueryBuffersCellStructureVar<'_, C>>>
    for QueryBuffersCellStructureVarArrowArray<C>
where
    C: ArrowPrimitiveTypeNative,
{
    /// If an offset exceeds `i64::MAX`, then the conversion fails.
    /// Otherwise, it succeeds.
    // And that should realistically never happen... right?
    type Error = std::num::TryFromIntError;

    fn try_from(
        value: Celled<QueryBuffersCellStructureVar<C>>,
    ) -> Result<Self, Self::Error> {
        let Celled(ncells, buffers) = value;

        // convert u64 byte offsets to i64 element offsets
        let offsets = {
            let offsets = if ncells == 0 {
                vec![0u64]
            } else {
                let noffsets = ncells + 1;
                match buffers.0.cell_structure.unwrap().unwrap() {
                    Buffer::Empty => vec![0u64],
                    Buffer::Borrowed(offsets) => offsets[0..noffsets].to_vec(),
                    Buffer::Owned(offsets) => {
                        let mut offsets = offsets.into_vec();
                        offsets.truncate(noffsets);
                        offsets
                    }
                }
            };

            // convert u64 byte offsets to i64 element offsets
            let offsets = offsets
                .into_iter()
                .map(|o| i64::try_from(o))
                .collect::<Result<Vec<i64>, TryFromIntError>>()?;
            OffsetBuffer::<i64>::new(ScalarBuffer::<i64>::from(offsets))
        };

        let values = ScalarBuffer::<C>::from(Celled(
            *offsets.last().unwrap() as usize,
            buffers.0.data,
        ));
        let values = PrimitiveArrayAlias::<C>::new(values, None);
        let values = Arc::new(values);

        let field = Field::new_list_field(values.data_type().clone(), false);

        let validity = buffers
            .0
            .validity
            .map(|v| NullBuffer::from(Celled(ncells, v)));

        let field = Arc::new(field);
        let gl = GenericListArray::<i64>::new(
            field,
            offsets,
            values.clone(),
            validity,
        );
        Ok(QueryBuffersCellStructureVarArrowArray(gl, values))
    }
}

impl<C> TryFrom<Celled<QueryBuffersCellStructureVar<'_, C>>>
    for GenericListArray<i64>
where
    C: ArrowPrimitiveTypeNative,
{
    type Error = std::num::TryFromIntError;
    fn try_from(
        value: Celled<QueryBuffersCellStructureVar<'_, C>>,
    ) -> Result<Self, Self::Error> {
        Ok(
            QueryBuffersCellStructureVarArrowArray::<C>::try_from(value)?
                .unwrap()
                .0,
        )
    }
}

impl TryFrom<Celled<QueryBuffersCellStructureVar<'_, u8>>>
    for LargeBinaryArray
{
    /// If an offset exceeds `i64::MAX`, then the conversion fails.
    /// Otherwise, it succeeds.
    // And that should realistically never happen... right?
    type Error = std::num::TryFromIntError;

    fn try_from(
        value: Celled<QueryBuffersCellStructureVar<u8>>,
    ) -> Result<Self, Self::Error> {
        Ok(GenericListArray::<i64>::try_from(value)?.into())
    }
}

pub enum QueryBufferArrowArray<C>
where
    C: ArrowPrimitiveTypeNative,
{
    Primitive(
        PrimitiveArray<<C as ArrowPrimitiveTypeNative>::ArrowPrimitiveType>,
    ),
    FixedSizeList(QueryBuffersCellStructureFixedArrowArray<C>),
    VarSizeList(QueryBuffersCellStructureVarArrowArray<C>),
}

impl<C> QueryBufferArrowArray<C>
where
    C: ArrowPrimitiveTypeNative,
{
    pub fn boxed(self) -> Box<dyn ArrowArray> {
        match self {
            Self::Primitive(p) => Box::new(p),
            Self::FixedSizeList(fl) => fl.boxed(),
            Self::VarSizeList(vl) => vl.boxed(),
        }
    }
}

impl<C> TryFrom<Celled<QueryBuffers<'_, C>>> for QueryBufferArrowArray<C>
where
    C: ArrowPrimitiveTypeNative,
{
    type Error = std::num::TryFromIntError;

    fn try_from(value: Celled<QueryBuffers<C>>) -> Result<Self, Self::Error> {
        let Celled(ncells, buffers) = value;

        match buffers.cell_structure.as_cell_val_num() {
            CellValNum::Fixed(nz) if nz.get() == 1 => {
                let celled = Celled(
                    ncells,
                    QueryBuffersCellStructureSingle::try_from(buffers).unwrap(),
                );
                let flat = PrimitiveArrayAlias::<C>::from(celled);
                Ok(Self::Primitive(flat))
            }
            CellValNum::Fixed(_) => {
                let celled = Celled(
                    ncells,
                    QueryBuffersCellStructureFixed::try_from(buffers).unwrap(),
                );
                let array =
                    QueryBuffersCellStructureFixedArrowArray::<C>::try_from(
                        celled,
                    )?;
                Ok(Self::FixedSizeList(array))
            }
            CellValNum::Var => {
                let celled = Celled(
                    ncells,
                    QueryBuffersCellStructureVar::try_from(buffers).unwrap(),
                );
                let array =
                    QueryBuffersCellStructureVarArrowArray::<C>::try_from(
                        celled,
                    )?;
                Ok(Self::VarSizeList(array))
            }
        }
    }
}
