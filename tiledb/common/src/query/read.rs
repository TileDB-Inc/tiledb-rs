/*
use crate::datatype::PhysicalType;

pub trait FromQueryOutput: Sized {
    type Unit;
    type Iterator<'data>: Iterator<Item = Self>
        + TryFrom<RawReadOutput<'data, Self::Unit>, Error = crate::error::Error>
    where
        Self::Unit: 'data;
}

impl<C> FromQueryOutput for C
where
    C: PhysicalType,
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
*/
