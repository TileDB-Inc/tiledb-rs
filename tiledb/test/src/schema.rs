use proptest::prelude::*;
use tiledb::array::{ArrayType, DomainData, SchemaData};

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

pub fn arbitrary_for_domain(
    array_type: ArrayType,
    domain: DomainData,
) -> impl Strategy<Value = SchemaData> {
    const MIN_ATTRS: usize = 1;
    const MAX_ATTRS: usize = 32;
    (
        arbitrary_cell_order(array_type),
        arbitrary_tile_order(),
        proptest::collection::vec(
            crate::attribute::arbitrary(),
            MIN_ATTRS..=MAX_ATTRS,
        ),
    )
        .prop_map(move |(cell_order, tile_order, attributes)| {
            SchemaData {
                array_type,
                domain: domain.clone(),
                capacity: None,
                cell_order: Some(cell_order),
                tile_order: Some(tile_order),
                allow_duplicates: None,
                attributes,
                coordinate_filters: vec![], /* TODO */
                offsets_filters: vec![],    /* TODO */
                nullity_filters: vec![],    /* TODO */
            }
        })
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
