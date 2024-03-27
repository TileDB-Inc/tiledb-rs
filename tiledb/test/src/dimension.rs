use std::fmt::Debug;

use num_traits::{Bounded, Num};
use proptest::prelude::*;
use serde_json::json;
use tiledb::array::{ArrayType, DimensionData};

use tiledb::{fn_typed, Datatype};

use crate::strategy::LifetimeBoundStrategy;

/// Construct a strategy to generate valid (domain, extent) pairs.
/// A valid output satisfies
/// `lower < lower + extent <= upper < upper + extent <= type_limit`.
fn arbitrary_range_and_extent<T>() -> impl Strategy<Value = ([T; 2], T)>
where
    T: Num
        + Bounded
        + Clone
        + Copy
        + Debug
        + std::fmt::Display
        + PartialOrd
        + std::ops::Sub<Output = T>
        + 'static,
    std::ops::Range<T>: Strategy<Value = T>,
{
    /*
     * First generate the upper bound.
     * Then generate the lower bound.
     * Then generate the extent.
     */
    let one = <T as num_traits::One>::one();
    let lower_limit = <T as Bounded>::min_value();
    let upper_limit = <T as Bounded>::max_value();
    std::ops::Range::<T> {
        start: lower_limit + one + one + one, // Needs this much space for lower bound
        end: upper_limit - one, // The extent is at least one, so we cannot match the upper limit
    }
    .prop_flat_map(move |upper_bound| {
        (
            std::ops::Range::<T> {
                start: lower_limit + one,
                end: upper_bound - one, // extent is at least one, cannot match upper bound
            },
            Just(upper_bound),
        )
    })
    .prop_flat_map(move |(lower_bound, upper_bound)| {
        (
            Just([lower_bound, upper_bound]),
            std::ops::Range::<T> {
                start: one,
                end: {
                    let zero = <T as num_traits::Zero>::zero();
                    let extent_limit = if lower_bound >= zero {
                        upper_bound - lower_bound
                    } else if upper_bound >= zero {
                        if upper_limit + lower_bound > upper_bound {
                            upper_bound - lower_bound
                        } else {
                            upper_limit - upper_bound
                        }
                    } else {
                        upper_bound - lower_bound
                    };

                    if upper_limit - extent_limit < upper_bound {
                        upper_limit - upper_bound
                    } else {
                        extent_limit
                    }
                },
            },
        )
    })
}

pub fn arbitrary_for_type(
    datatype: Datatype,
) -> impl Strategy<Value = DimensionData> {
    fn_typed!(arbitrary_range_and_extent, datatype =>
        (crate::attribute::arbitrary_name(), arbitrary_range_and_extent).prop_map(move |(name, values)| {
            DimensionData {
                name, datatype,
                domain: [json!(values.0[0]), json!(values.0[1])],
                extent: json!(values.1),
                cell_val_num: None,
                filters: vec![] /* TODO */
            }
    }).bind())
}

pub fn arbitrary_for_array_type(
    array_type: ArrayType,
) -> impl Strategy<Value = DimensionData> {
    match array_type {
        ArrayType::Dense => {
            crate::datatype::arbitrary_for_dense_dimension().boxed()
        }
        ArrayType::Sparse => crate::datatype::arbitrary_implemented().boxed(),
    }
    .prop_flat_map(arbitrary_for_type)
}

pub fn arbitrary() -> impl Strategy<Value = DimensionData> {
    prop_oneof![Just(ArrayType::Dense), Just(ArrayType::Sparse)]
        .prop_flat_map(arbitrary_for_array_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tiledb::{Context, Factory};

    /// Test that the arbitrary dimension construction always succeeds
    #[test]
    fn dimension_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_dimension in arbitrary())| {
            maybe_dimension.create(&ctx).expect("Error constructing arbitrary dimension");
        });
    }

    #[test]
    fn dimension_eq_reflexivity() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_dimension in arbitrary())| {
            let dimension = maybe_dimension.create(&ctx).expect("Error constructing arbitrary attribute");
            assert_eq!(dimension, dimension);
        });
    }
}
