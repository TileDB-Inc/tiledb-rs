use proptest::collection::vec;
use proptest::prelude::*;
use proptest::test_runner::TestRng;

use tiledb::array::domain::DomainData;
use tiledb::array::ArrayType;

use crate::datatype;
use crate::dimension;
use crate::util;

pub fn generate(rng: &mut TestRng, array_type: ArrayType) -> DomainData {
    let mut dims = Vec::new();
    let num_dims = rng.gen_range(1..8);
    if matches!(array_type, ArrayType::Dense) {
        let datatype =
            util::choose(rng, &datatype::dense_dimension_datatypes_vec());
        for _ in 0..num_dims {
            dims.push(dimension::generate(datatype))
        }
    } else {
        for _ in 0..num_dims {
            let datatype =
                util::choose(rng, &datatype::sparse_dimension_datatypes_vec());
            dims.push(dimension::generate(datatype))
        }
    }
}
