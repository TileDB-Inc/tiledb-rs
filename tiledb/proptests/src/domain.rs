use proptest::collection::vec;
use proptest::prelude::*;

use tiledb::array::domain::DomainData;
use tiledb::array::ArrayType;

use crate::datatype as pt_datatype;
use crate::dimension as pt_dimension;

pub fn prop_domain_data(
    array_type: ArrayType,
) -> impl Strategy<Value = DomainData> {
    if matches!(array_type, ArrayType::Dense) {
        let datatype = pt_datatype::prop_dense_dimension_datatypes();
        datatype
            .prop_flat_map(|datatype| {
                vec(pt_dimension::prop_dimension_data_for_type(datatype), 1..8)
                    .prop_flat_map(|dims| Just(DomainData { dimension: dims }))
            })
            .boxed()
    } else {
        vec(pt_dimension::prop_dimension_data(array_type), 1..8)
            .prop_flat_map(|dims| Just(DomainData { dimension: dims }))
            .boxed()
    }
}
