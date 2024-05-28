#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use util::assert_option_subset;
    use util::option::OptionSubset;

    use crate::array::schema::SchemaData;
    use crate::array::{Array, Schema};
    use crate::context::Context;
    use crate::test_util::{self, TestArrayUri};
    use crate::Factory;

    #[test]
    fn test_array_create() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(schema_spec in any::<SchemaData>())| {
            let schema_in = schema_spec.create(&ctx)
                .expect("Error constructing arbitrary schema");

            let test_uri = test_util::get_uri_generator()?;
            let uri = test_uri.with_path("array")?;

            Array::create(&ctx, &uri, schema_in)
                .expect("Error creating array");

            let schema_out = Schema::load(&ctx, &uri).expect("Error loading array schema");

            let schema_out_spec = SchemaData::try_from(&schema_out).expect("Error creating schema spec");
            assert_option_subset!(schema_spec, schema_out_spec);
        })
    }
}
