use arrow::array::{
    FixedSizeListArray, GenericListArray, OffsetSizeTrait, PrimitiveArray,
};
use arrow::datatypes::ArrowPrimitiveType;
use tiledb_common::range::{Range, SingleValueRange};

pub trait ArrayExt {
    fn domain(&self) -> Option<Range>;
}

impl<T> ArrayExt for PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    SingleValueRange: From<[<T as ArrowPrimitiveType>::Native; 2]>,
{
    fn domain(&self) -> Option<Range> {
        let min = arrow::compute::min(self)?;
        let max = arrow::compute::max(self)?;

        Some(Range::Single(SingleValueRange::from([min, max])))
    }
}

impl ArrayExt for FixedSizeListArray {
    fn domain(&self) -> Option<Range> {
        todo!()
    }
}

impl<O> ArrayExt for GenericListArray<O>
where
    O: OffsetSizeTrait,
{
    fn domain(&self) -> Option<Range> {
        todo!()
    }
}
