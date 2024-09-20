use proptest::prelude::*;

use super::TileOrder;

impl Arbitrary for TileOrder {
    type Parameters = ();
    type Strategy = BoxedStrategy<TileOrder>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        prop_oneof![Just(TileOrder::RowMajor), Just(TileOrder::ColumnMajor),]
            .boxed()
    }
}

mod tests {
    use super::*;
    use util::assert_option_subset;
    use util::option::OptionSubset;

    use tiledb_test_utils::{self, TestArrayUri};

    use crate::array::schema::SchemaData;
    use crate::array::{Array, Schema};
    use crate::context::Context;
    use crate::error::Error;
    use crate::Factory;

    #[test]
    fn test_array_create() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(schema_spec in any::<SchemaData>())| {
            let schema_in = schema_spec.create(&ctx)
                .expect("Error constructing arbitrary schema");

            let test_uri = tiledb_test_utils::get_uri_generator().map_err(|e| Error::Other(e.to_string()))?;
            let uri = test_uri.with_path("array").map_err(|e| Error::Other(e.to_string()))?;

            Array::create(&ctx, &uri, schema_in)
                .expect("Error creating array");

            let schema_out = Schema::load(&ctx, &uri).expect("Error loading array schema");

            let schema_out_spec = SchemaData::try_from(&schema_out).expect("Error creating schema spec");
            assert_option_subset!(schema_spec, schema_out_spec);
        })
    }
}
