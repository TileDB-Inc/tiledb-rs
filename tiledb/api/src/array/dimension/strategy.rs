use std::fmt::Debug;
use std::rc::Rc;

use num_traits::{Bounded, FromPrimitive, Num};
use proptest::prelude::*;

use tiledb_utils::numbers::{
    NextDirection, NextNumericValue, SmallestPositiveValue,
};

use crate::array::dimension::DimensionConstraints;
use crate::array::{ArrayType, CellValNum, DimensionData};
use crate::datatype::physical::BitsOrd;
use crate::datatype::strategy::*;
use crate::datatype::LogicalType;
use crate::filter::list::FilterListData;
use crate::filter::strategy::{
    Requirements as FilterRequirements, StrategyContext as FilterContext,
};
use crate::{fn_typed, Datatype};

#[derive(Clone)]
pub struct Requirements {
    pub array_type: Option<ArrayType>,
    pub datatype: Option<Datatype>,
    pub extent_limit: usize,
}

impl Default for Requirements {
    fn default() -> Self {
        Requirements {
            array_type: None,
            datatype: None,
            extent_limit: 1024 * 16,
        }
    }
}

pub fn prop_dimension_name() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[a-zA-Z0-9_]*")
        .expect("Error creating dimension name strategy")
}

/// Construct a strategy to generate valid (domain, extent) pairs.
/// A valid output satisfies
/// `lower < lower + extent <= upper < upper + extent <= type_limit`.
fn prop_range_and_extent<T>(
    requirements_extent_limit: usize,
) -> impl Strategy<Value = ([T; 2], Option<T>)>
where
    T: Num
        + BitsOrd
        + Bounded
        + FromPrimitive
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
        let (extent_limit, would_overflow) = {
            let zero = <T as num_traits::Zero>::zero();

            let mut would_overflow = false;
            let extent_limit = if lower_bound >= zero {
                upper_bound - lower_bound
            } else if upper_bound >= zero {
                if upper_limit + lower_bound > upper_bound {
                    upper_bound - lower_bound
                } else {
                    would_overflow = true;
                    upper_limit - upper_bound
                }
            } else {
                upper_bound - lower_bound
            };

            if upper_limit - extent_limit < upper_bound {
                (upper_limit - upper_bound, would_overflow)
            } else {
                (extent_limit, would_overflow)
            }
        };

        // A Rust range is half open which means that we have guarantee the
        // end value is strictly > than the lower limit.
        let extent_limit = if extent_limit <= T::smallest_positive_value() {
            extent_limit + T::smallest_positive_value()
        } else {
            extent_limit
        };

        // see SC-47322, we need to prevent the extent from getting too big
        // because core does not treat it for memory allocations
        let extent_limit_limit = {
            match T::from_usize(requirements_extent_limit) {
                Some(t) => t,
                None => {
                    /* the type range is small enough that we need not worry */
                    upper_limit
                }
            }
        };
        if matches!(
            extent_limit_limit.bits_cmp(&extent_limit),
            std::cmp::Ordering::Less
        ) {
            return (
                Just([lower_bound, upper_bound]),
                std::ops::Range::<T> {
                    start: T::smallest_positive_value(),
                    end: extent_limit_limit,
                }
                .prop_map(Some),
            )
                .boxed();
        }

        // Bug SC-47034: Core does not correctly handle ranges on signed
        // dimensions when the size of the range overflows the signed type's
        // range. I.e., [-70i8, 121] has a range of 191 which is larger than
        // the maximum byte value 127i8. Our round trip tests rely on getting
        // correct values from core. To avoid triggering the bug we force an
        // extent when overflow would happen.
        if would_overflow {
            return (
                Just([lower_bound, upper_bound]),
                std::ops::Range::<T> {
                    start: T::smallest_positive_value(),
                    end: extent_limit,
                }
                .prop_map(|extent| Some(extent)),
            )
                .boxed();
        }

        (
            Just([lower_bound, upper_bound]),
            proptest::option::of(std::ops::Range::<T> {
                start: T::smallest_positive_value(),
                end: extent_limit,
            }),
        )
            .boxed()
    })
}

fn prop_dimension_for_datatype(
    datatype: Datatype,
    params: Requirements,
) -> impl Strategy<Value = DimensionData> {
    let cell_val_num = if datatype.is_string_type() {
        CellValNum::Var
    } else {
        CellValNum::single()
    };
    fn_typed!(datatype, LT, {
        type DT = <LT as LogicalType>::PhysicalType;
        let name = prop_dimension_name();
        let range_and_extent = if !datatype.is_string_type() {
            prop_range_and_extent::<DT>(params.extent_limit)
                .prop_map(Some)
                .boxed()
        } else {
            Just(None).boxed()
        };
        let filters = any_with::<FilterListData>(Rc::new(FilterRequirements {
            input_datatype: Some(datatype),
            context: Some(FilterContext::Dimension(datatype, cell_val_num)),
            ..Default::default()
        }));
        (name, range_and_extent, filters)
            .prop_map(move |(name, values, filters)| {
                let constraints = match values {
                    Some((dom, extent)) => {
                        DimensionConstraints::from((dom, extent))
                    }
                    None => DimensionConstraints::StringAscii,
                };
                DimensionData {
                    name,
                    datatype,
                    constraints,
                    cell_val_num: Some(cell_val_num),
                    filters: if filters.is_empty() {
                        None
                    } else {
                        Some(filters)
                    },
                }
            })
            .boxed()
    })
}

fn prop_dimension_for_array_type(
    array_type: ArrayType,
    params: Requirements,
) -> impl Strategy<Value = DimensionData> {
    match array_type {
        ArrayType::Dense => {
            any_with::<Datatype>(DatatypeContext::DenseDimension)
        }
        ArrayType::Sparse => {
            any_with::<Datatype>(DatatypeContext::SparseDimension)
        }
    }
    .prop_flat_map(move |datatype| {
        prop_dimension_for_datatype(datatype, params.clone())
    })
}

impl Arbitrary for DimensionData {
    type Parameters = Requirements;
    type Strategy = BoxedStrategy<DimensionData>;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        if let Some(datatype) = params.datatype {
            prop_dimension_for_datatype(datatype, params).boxed()
        } else if let Some(array_type) = params.array_type {
            prop_dimension_for_array_type(array_type, params).boxed()
        } else {
            prop_oneof![Just(ArrayType::Dense), Just(ArrayType::Sparse)]
                .prop_flat_map(move |array_type| {
                    prop_dimension_for_array_type(array_type, params.clone())
                })
                .boxed()
        }
    }
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

        proptest!(|(maybe_dimension in any::<DimensionData>())| {
            maybe_dimension.create(&ctx)
                .expect("Error constructing arbitrary dimension");
        });
    }

    #[test]
    fn dimension_eq_reflexivity() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(dimension in any::<DimensionData>())| {
            assert_eq!(dimension, dimension);
            assert_option_subset!(dimension, dimension);

            let dimension = dimension
                .create(&ctx).expect("Error constructing arbitrary attribute");
            assert_eq!(dimension, dimension);
        });
    }
}
