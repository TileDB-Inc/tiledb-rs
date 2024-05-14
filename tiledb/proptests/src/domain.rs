use std::collections::HashSet;

use proptest::prelude::*;
use proptest::test_runner::TestRng;

use tiledb::array::domain::DomainData;
use tiledb::array::schema::SchemaData;
use tiledb::array::ArrayType;
use tiledb::Result as TileDBResult;

use crate::datatype;
use crate::dimension;
use crate::util;

pub fn generate(
    rng: &mut TestRng,
    schema: &SchemaData,
    field_names: &mut HashSet<String>,
) -> TileDBResult<DomainData> {
    let mut dims = Vec::new();
    let num_dims = rng.gen_range(1..8);
    if matches!(schema.array_type, ArrayType::Dense) {
        let datatype =
            util::choose(rng, &datatype::dense_dimension_datatypes_vec());
        for _ in 0..num_dims {
            dims.push(dimension::generate(rng, schema, datatype, field_names)?)
        }
    } else {
        for _ in 0..num_dims {
            let datatype =
                util::choose(rng, &datatype::sparse_dimension_datatypes_vec());
            dims.push(dimension::generate(rng, schema, datatype, field_names)?)
        }
    }

    Ok(DomainData { dimension: dims })
}
