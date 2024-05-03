use std::fmt::Debug;
use std::rc::Rc;

use num_traits::{Bounded, Num};
use proptest::prelude::*;
use serde_json::json;

use tiledb_utils::numbers::{
    NextDirection, NextNumericValue, SmallestPositiveValue,
};

use crate::array::{ArrayType, DimensionData};
use crate::datatype::strategy::*;
use crate::datatype::LogicalType;
use crate::filter::list::FilterListData;
use crate::filter::strategy::Requirements as FilterRequirements;
use crate::{fn_typed, Datatype};

pub fn prop_dimension_name() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[a-zA-Z0-9_]*")
        .expect("Error creating dimension name strategy")
}

/// Construct a strategy to generate valid (domain, extent) pairs.
/// A valid output satisfies
/// `lower < lower + extent <= upper < upper + extent <= type_limit`.
fn prop_range_and_extent<T>() -> impl Strategy<Value = ([T; 2], T)>
where
    T: Num
        + Bounded
        + NextNumericValue
        + SmallestPositiveValue
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
        // Needs this much space for lower bound and extent
        start: lower_limit + one + one + one,
        // The extent is at least one, so we cannot match the upper limit
        end: upper_limit - one,
    }
    .prop_flat_map(move |upper_bound| {
        (
            std::ops::Range::<T> {
                start: lower_limit + one,
                // Correctly generating an extent means we need to have room
                // for at least a range of one. This means that we need to
                // leave room between the lower and upper bound. Normally this
                // would mean `upper_bound - one`, however the resolution of
                // large floating point values may be so large that
                // `x - 1 == x`. This leaves us having to implement a "next
                // value" trait to ensure there's a logical gap.
                end: upper_bound.next_numeric_value(NextDirection::Down),
            },
            Just(upper_bound),
        )
    })
    .prop_flat_map(move |(lower_bound, upper_bound)| {
        let extent_limit = {
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
        };

        // A Rust range is half open which means that we have guarantee the
        // end value is strictly > than the lower limit.
        let extent_limit = if extent_limit <= T::smallest_positive_value() {
            extent_limit + T::smallest_positive_value()
        } else {
            extent_limit
        };

        (
            Just([lower_bound, upper_bound]),
            std::ops::Range::<T> {
                start: T::smallest_positive_value(),
                end: extent_limit,
            },
        )
    })
}

pub fn prop_dimension_for_datatype(
    datatype: Datatype,
) -> impl Strategy<Value = DimensionData> {
    fn_typed!(datatype, LT, {
        type DT = <LT as LogicalType>::PhysicalType;
        let name = prop_dimension_name();
        let range_and_extent = prop_range_and_extent::<DT>();
        let filters = any_with::<FilterListData>(Rc::new(FilterRequirements {
            input_datatype: Some(datatype),
            ..Default::default()
        }));
        (name, range_and_extent, filters)
            .prop_map(move |(name, values, filters)| DimensionData {
                name,
                datatype,
                domain: [json!(values.0[0]), json!(values.0[1])],
                extent: json!(values.1),
                cell_val_num: None,
                filters: if filters.is_empty() {
                    None
                } else {
                    Some(filters)
                },
            })
            .boxed()
    })
}

pub fn prop_dimension_for_array_type(
    array_type: ArrayType,
) -> impl Strategy<Value = DimensionData> {
    match array_type {
        ArrayType::Dense => prop_datatype_for_dense_dimension().boxed(),
        ArrayType::Sparse => prop_datatype_implemented().boxed(),
    }
    .prop_flat_map(prop_dimension_for_datatype)
}

pub fn prop_dimension() -> impl Strategy<Value = DimensionData> {
    prop_oneof![Just(ArrayType::Dense), Just(ArrayType::Sparse)]
        .prop_flat_map(prop_dimension_for_array_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Context, Factory};
    use util::assert_option_subset;
    use util::option::OptionSubset;

    /// Test that the arbitrary dimension construction always succeeds
    #[test]
    fn test_prop_dimension() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_dimension in prop_dimension())| {
            maybe_dimension.create(&ctx)
                .expect("Error constructing arbitrary dimension");
        });
    }

    #[test]
    fn dimension_eq_reflexivity() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(dimension in prop_dimension())| {
            assert_eq!(dimension, dimension);
            assert_option_subset!(dimension, dimension);

            let dimension = dimension
                .create(&ctx).expect("Error constructing arbitrary attribute");
            assert_eq!(dimension, dimension);
        });
    }
}
