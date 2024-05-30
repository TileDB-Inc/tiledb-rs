use std::collections::HashSet;
use std::iter::IntoIterator;

use itertools::structs::Combinations;
use itertools::Itertools;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRng;

use tiledb::array::dimension::DimensionData;
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

#[derive(Debug)]
pub struct DomainValueTree {
    domain: DomainData,
    iter: Combinations<std::vec::IntoIter<DimensionData>>,
    current: Option<Vec<DimensionData>>,
    len: usize,
}

impl DomainValueTree {
    pub fn new(domain: DomainData) -> Self {
        assert!(!domain.dimension.is_empty());
        let mut iter = domain.dimension.clone().into_iter().combinations(1);
        let current = iter.next();
        assert!(current.is_some());
        Self {
            domain,
            iter,
            current,
            len: 1,
        }
    }
}

impl ValueTree for DomainValueTree {
    type Value = DomainData;

    fn current(&self) -> Self::Value {
        self.domain.clone()
    }

    fn simplify(&mut self) -> bool {
        false
    }

    fn complicate(&mut self) -> bool {
        false
    }
}
