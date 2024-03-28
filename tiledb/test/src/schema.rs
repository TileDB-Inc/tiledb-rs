use proptest::prelude::*;
use tiledb::array::{ArrayType, DomainData, SchemaData};
use tiledb::filter::{CompressionData, CompressionType, FilterData};
use tiledb::filter_list::FilterListData;

use crate::strategy::LifetimeBoundStrategy;

pub fn arbitrary_array_type() -> impl Strategy<Value = ArrayType> {
    prop_oneof![Just(ArrayType::Dense), Just(ArrayType::Sparse),]
}

pub fn arbitrary_cell_order(
    array_type: ArrayType,
) -> impl Strategy<Value = tiledb::array::Layout> {
    use tiledb::array::Layout;
    match array_type {
        ArrayType::Sparse => prop_oneof![
            Just(Layout::Unordered),
            Just(Layout::RowMajor),
            Just(Layout::ColumnMajor),
            Just(Layout::Hilbert),
        ]
        .bind(),
        ArrayType::Dense => prop_oneof![
            Just(Layout::Unordered),
            Just(Layout::RowMajor),
            Just(Layout::ColumnMajor),
        ]
        .bind(),
    }
}

pub fn arbitrary_tile_order() -> impl Strategy<Value = tiledb::array::Layout> {
    use tiledb::array::Layout;
    prop_oneof![
        Just(Layout::Unordered),
        Just(Layout::RowMajor),
        Just(Layout::ColumnMajor),
    ]
}

pub fn arbitrary_coordinate_filters(
    domain: &DomainData,
) -> impl Strategy<Value = FilterListData> {
    /*
     * See tiledb/array_schema/array_schema.cc for the rules.
     * - DoubleDelta compressor is disallowed on floating-point dimensions with no filters
     */
    let mut has_unfiltered_float_dimension = false;
    for dim in domain.dimension.iter() {
        if dim.datatype.is_real_type() && dim.filters.is_empty() {
            has_unfiltered_float_dimension = true;
            break;
        }
    }

    crate::filter::arbitrary_list().prop_filter(
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

pub fn arbitrary_for_domain(
    array_type: ArrayType,
    domain: DomainData,
) -> impl Strategy<Value = SchemaData> {
    const MIN_ATTRS: usize = 1;
    const MAX_ATTRS: usize = 32;

    let allow_duplicates = match array_type {
        ArrayType::Dense => Just(false).bind(),
        ArrayType::Sparse => any::<bool>().bind(),
    };

    (
        any::<u64>(),
        arbitrary_cell_order(array_type),
        arbitrary_tile_order(),
        allow_duplicates,
        proptest::collection::vec(
            crate::attribute::arbitrary(),
            MIN_ATTRS..=MAX_ATTRS,
        ),
        arbitrary_coordinate_filters(&domain),
        crate::filter::arbitrary_list(),
        crate::filter::arbitrary_list(),
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

pub fn arbitrary() -> impl Strategy<Value = SchemaData> {
    arbitrary_array_type().prop_flat_map(|array_type| {
        crate::domain::arbitrary_for_array_type(array_type).prop_flat_map(
            move |domain| arbitrary_for_domain(array_type, domain),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tiledb::{Context, Factory};

    /// Test that the arbitrary schema construction always succeeds
    #[test]
    fn schema_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_schema in arbitrary())| {
            maybe_schema.create(&ctx).expect("Error constructing arbitrary schema");
        });
    }

    #[test]
    fn schema_eq_reflexivity() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_schema in arbitrary())| {
            let schema = maybe_schema.create(&ctx).expect("Error constructing arbitrary schema");
            assert_eq!(schema, schema);
        });
    }
}
