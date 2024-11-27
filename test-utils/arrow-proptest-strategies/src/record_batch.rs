use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use arrow_schema::{Schema, SchemaRef};
use proptest::collection::SizeRange;
use proptest::prelude::*;

use crate::array::{prop_array, ColumnParameters};

#[derive(Clone, Debug)]
pub struct RecordBatchParameters {
    /// Strategy for choosing a number of rows.
    pub num_rows: BoxedStrategy<usize>,
    /// Strategy for choosing the number of elements in variable-length column elements.
    pub num_collection_elements: SizeRange,
    /// Whether to allow elements of collection types such as `DataType::LargeList`
    /// to be null. Defaults to `true`.
    pub allow_null_collection_element: bool,
}

impl RecordBatchParameters {
    pub fn column_parameters(&self) -> ColumnParameters {
        ColumnParameters {
            num_rows: self.num_rows.clone(),
            num_collection_elements: self.num_collection_elements.clone(),
            allow_null_values: true,
            allow_null_collection_element: self.allow_null_collection_element,
        }
    }
}

impl Default for RecordBatchParameters {
    fn default() -> Self {
        Self {
            num_rows: (0..=4usize).boxed(),
            num_collection_elements: (0..=4usize).into(),
            allow_null_collection_element: true,
        }
    }
}

pub fn prop_record_batch(
    schema: BoxedStrategy<Schema>,
    params: RecordBatchParameters,
) -> impl Strategy<Value = RecordBatch> {
    schema.prop_flat_map(move |s| {
        prop_record_batch_for_schema(params.clone(), Arc::new(s))
    })
}

pub fn prop_record_batch_for_schema(
    params: RecordBatchParameters,
    schema: SchemaRef,
) -> impl Strategy<Value = RecordBatch> {
    params
        .num_rows
        .clone()
        .prop_flat_map(move |num_rows| {
            let column_params = ColumnParameters {
                num_rows: Just(num_rows).boxed(),
                ..params.column_parameters()
            };
            let columns = schema
                .fields
                .iter()
                .map(move |field| {
                    prop_array(column_params.clone(), Arc::clone(field)).boxed()
                })
                .collect::<Vec<BoxedStrategy<Arc<dyn Array>>>>();

            (Just(Arc::clone(&schema)), columns)
        })
        .prop_map(|(schema, columns)| {
            RecordBatch::try_new(schema, columns).unwrap()
        })
}
#[cfg(test)]
mod tests {
    use super::*;

    proptest! {
        #[test]
        fn strategy_validity(_ in prop_record_batch(
                crate::schema::prop_arrow_schema(Default::default()).boxed(),
                Default::default())
        ) {
            // NB: empty, this just checks that we produce correct record batches
        }
    }
}
