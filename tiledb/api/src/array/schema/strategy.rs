use proptest::prelude::*;

use crate::array::attribute::strategy::*;
use crate::array::domain::strategy::*;
use crate::array::{ArrayType, DomainData, Layout, SchemaData};
use crate::filter::list::FilterListData;
use crate::filter::strategy::*;
use crate::filter::{CompressionData, CompressionType, FilterData};

pub fn prop_array_type() -> impl Strategy<Value = ArrayType> {
    prop_oneof![Just(ArrayType::Dense), Just(ArrayType::Sparse),]
}

pub fn prop_cell_order(array_type: ArrayType) -> impl Strategy<Value = Layout> {
    match array_type {
        ArrayType::Sparse => prop_oneof![
            Just(Layout::Unordered),
            Just(Layout::RowMajor),
            Just(Layout::ColumnMajor),
            Just(Layout::Hilbert),
        ]
        .boxed(),
        ArrayType::Dense => prop_oneof![
            Just(Layout::Unordered),
            Just(Layout::RowMajor),
            Just(Layout::ColumnMajor),
        ]
        .boxed(),
    }
}

pub fn prop_tile_order() -> impl Strategy<Value = Layout> {
    prop_oneof![
        Just(Layout::Unordered),
        Just(Layout::RowMajor),
        Just(Layout::ColumnMajor),
    ]
}

pub fn prop_coordinate_filters(
    domain: &DomainData,
) -> impl Strategy<Value = FilterListData> {
    /*
     * See tiledb/array_schema/array_schema.cc for the rules.
     * - DoubleDelta compressor is disallowed on floating-point dimensions
     *   with no filters
     */
    let mut has_unfiltered_float_dimension = false;
    for dim in domain.dimension.iter() {
        if dim.datatype.is_real_type() && dim.filters.is_empty() {
            has_unfiltered_float_dimension = true;
            break;
        }
    }

    prop_filter_pipeline().prop_filter(
        "Floating-point dimension cannot have DOUBLE DELTA compression",
        move |fl| {
            !(has_unfiltered_float_dimension
                && fl.iter().any(|f| {
                    matches!(
                        f,
                        FilterData::Compression(CompressionData {
                            kind: CompressionType::DoubleDelta,
                            ..
                        })
                    )
                }))
        },
    )
}

pub fn prop_schema_for_domain(
    array_type: ArrayType,
    domain: DomainData,
) -> impl Strategy<Value = SchemaData> {
    const MIN_ATTRS: usize = 1;
    const MAX_ATTRS: usize = 32;

    let allow_duplicates = match array_type {
        ArrayType::Dense => Just(false).boxed(),
        ArrayType::Sparse => any::<bool>().boxed(),
    };

    (
        any::<u64>(),
        prop_cell_order(array_type),
        prop_tile_order(),
        allow_duplicates,
        proptest::collection::vec(prop_attribute(), MIN_ATTRS..=MAX_ATTRS),
        prop_coordinate_filters(&domain),
        prop_filter_pipeline(),
        prop_filter_pipeline(),
    )
        .prop_map(
            move |(
                capacity,
                cell_order,
                tile_order,
                allow_duplicates,
                attributes,
                coordinate_filters,
                offsets_filters,
                nullity_filters,
            )| {
                SchemaData {
                    array_type,
                    domain: domain.clone(),
                    capacity: Some(capacity),
                    cell_order: Some(cell_order),
                    tile_order: Some(tile_order),
                    allow_duplicates: Some(allow_duplicates),
                    attributes,
                    coordinate_filters,
                    offsets_filters,
                    nullity_filters,
                }
            },
        )
}

pub fn prop_schema() -> impl Strategy<Value = SchemaData> {
    prop_array_type().prop_flat_map(|array_type| {
        prop_domain_for_array_type(array_type).prop_flat_map(move |domain| {
            prop_schema_for_domain(array_type, domain)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Context, Factory};

    /// Test that the arbitrary schema construction always succeeds
    #[test]
    fn schema_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_schema in prop_schema())| {
            maybe_schema.create(&ctx)
                .expect("Error constructing arbitrary schema");
        });
    }

    #[test]
    fn schema_eq_reflexivity() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_schema in prop_schema())| {
            let schema = maybe_schema.create(&ctx)
                .expect("Error constructing arbitrary schema");
            assert_eq!(schema, schema);
        });
    }
}
