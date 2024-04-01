#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use tempdir::TempDir;

    use crate::array::*;
    use crate::context::Context;
    use crate::Factory;

    #[test]
    fn test_array_create() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_schema in crate::array::schema::strategy::prop_schema(Default::default()))| {
            let schema = maybe_schema.create(&ctx)
                .expect("Error constructing arbitrary schema");
            assert_eq!(schema, schema);

            let array_create = {
                let tempdir = TempDir::new("test_array_create").expect("Error creating temp dir");
                let uri = String::from("file:///") + tempdir.path().join("array").to_str().unwrap();

                Array::create(&ctx, &uri, schema)
            };
            array_create.expect("Error creating array from arbitrary schema");
        });
    }
}
