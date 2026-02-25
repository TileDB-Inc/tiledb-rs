use std::fmt::Debug;

use num_traits::{Bounded, FromPrimitive, Num};
use proptest::prelude::*;
use tiledb_utils::numbers::{
    NextDirection, NextNumericValue, SmallestPositiveValue,
};

use super::*;
use crate::datatype::physical::strategy::PhysicalValueStrategy;
use crate::datatype::physical::BitsOrd;
use crate::range::{Range, VarValueRange};

impl DimensionConstraints {
    /// Returns a strategy for generating values which fall within the domain.
    pub fn value_strategy(&self) -> PhysicalValueStrategy {
        crate::dimension_constraints_go!(
            self,
            DT,
            domain,
            _,
            PhysicalValueStrategy::from((domain[0]..=domain[1]).boxed()),
            PhysicalValueStrategy::from(any::<u8>().boxed())
        )
    }

    /// Returns a strategy for generating subarray ranges which fall within
    /// the domain.
    ///
    /// `num_cells_bound` is an optional restriction on the number of possible values
    /// which the strategy is allowed to return.
    ///
    /// If `num_cells_bound` is `None`, then this function always returns `Some`.
    pub fn subarray_strategy(
        &self,
        num_cells_bound: Option<usize>,
    ) -> Option<proptest::strategy::BoxedStrategy<crate::range::Range>> {
        crate::dimension_constraints_go!(
            self,
            DT,
            domain,
            _,
            {
                let cell_bound = num_cells_bound
                    .map(|bound| DT::try_from(bound).unwrap_or(DT::MAX))
                    .unwrap_or(DT::MAX);

                let domain_lower = domain[0];
                let domain_upper = domain[1];
                let strat =
                    (domain_lower..=domain_upper).prop_flat_map(move |lb| {
                        let ub = std::cmp::min(
                            domain_upper,
                            lb.checked_add(cell_bound).unwrap_or(DT::MAX),
                        );
                        (Just(lb), lb..=ub).prop_map(|(min, max)| {
                            Range::Single(SingleValueRange::from(&[min, max]))
                        })
                    });
                Some(strat.boxed())
            },
            {
                if num_cells_bound.is_some() {
                    /*
                     * This can be implemented, but there's some ambiguity about
                     * what it should mean when precision goes out the window,
                     * so wait until there's a use case to decide.
                     */
                    return None;
                }

                let domain_lower = domain[0];
                let domain_upper = domain[1];
                let strat =
                    (domain_lower..=domain_upper).prop_flat_map(move |lb| {
                        (Just(lb), (lb..=domain_upper)).prop_map(
                            |(min, max)| {
                                Range::Single(SingleValueRange::from(&[
                                    min, max,
                                ]))
                            },
                        )
                    });
                Some(strat.boxed())
            },
            {
                // DimensionConstraints::StringAscii
                let strat_bound =
                    proptest::string::string_regex("[ -~]*").unwrap().boxed();

                if num_cells_bound.is_some() {
                    /*
                     * This is not tractible unless there is a bound on the string length.
                     * There isn't one since `StringAscii` is only allowed as a dimension
                     * type in sparse arrays.
                     */
                    return None;
                }

                let strat = (strat_bound.clone(), strat_bound).prop_map(
                    |(ascii1, ascii2)| {
                        let (lb, ub) = if ascii1 < ascii2 {
                            (ascii1, ascii2)
                        } else {
                            (ascii2, ascii1)
                        };
                        Range::Var(VarValueRange::from((lb, ub)))
                    },
                );
                Some(strat.boxed())
            }
        )
    }
}

#[derive(Clone)]
pub struct Requirements {
    pub datatype: Option<DimensionType>,
    pub extent_limit: usize,
}

impl Default for Requirements {
    fn default() -> Self {
        Requirements {
            datatype: None,
            extent_limit: 1024 * 16,
        }
    }
}

#[derive(
    proptest_derive::Arbitrary, Clone, Copy, Debug, Eq, Hash, PartialEq,
)]
pub enum DimensionType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    StringAscii,
}

impl DimensionType {
    fn dimension_strategy(
        &self,
        extent_limit: usize,
    ) -> impl Strategy<Value = DimensionConstraints> + use<> {
        match self {
            Self::UInt8 => prop_range_and_extent::<u8>(extent_limit)
                .prop_map(DimensionConstraints::from)
                .boxed(),
            Self::UInt16 => prop_range_and_extent::<u16>(extent_limit)
                .prop_map(DimensionConstraints::from)
                .boxed(),
            Self::UInt32 => prop_range_and_extent::<u32>(extent_limit)
                .prop_map(DimensionConstraints::from)
                .boxed(),
            Self::UInt64 => prop_range_and_extent::<u64>(extent_limit)
                .prop_map(DimensionConstraints::from)
                .boxed(),
            Self::Int8 => prop_range_and_extent::<i8>(extent_limit)
                .prop_map(DimensionConstraints::from)
                .boxed(),
            Self::Int16 => prop_range_and_extent::<i16>(extent_limit)
                .prop_map(DimensionConstraints::from)
                .boxed(),
            Self::Int32 => prop_range_and_extent::<i32>(extent_limit)
                .prop_map(DimensionConstraints::from)
                .boxed(),
            Self::Int64 => prop_range_and_extent::<i64>(extent_limit)
                .prop_map(DimensionConstraints::from)
                .boxed(),
            Self::Float32 => prop_range_and_extent::<f32>(extent_limit)
                .prop_map(DimensionConstraints::from)
                .boxed(),
            Self::Float64 => prop_range_and_extent::<f64>(extent_limit)
                .prop_map(DimensionConstraints::from)
                .boxed(),
            Self::StringAscii => {
                Just(DimensionConstraints::StringAscii).boxed()
            }
        }
    }
}

impl Arbitrary for DimensionConstraints {
    type Parameters = Requirements;
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(p: Self::Parameters) -> Self::Strategy {
        if let Some(dt) = p.datatype {
            dt.dimension_strategy(p.extent_limit).boxed()
        } else {
            any::<DimensionType>()
                .prop_flat_map(move |dt| dt.dimension_strategy(p.extent_limit))
                .boxed()
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatype::PhysicalValue;

    fn instance_value_strategy_integrity(
        constraints: DimensionConstraints,
        value: PhysicalValue,
    ) {
        fn case<T>(lb: T, ub: T, value: T)
        where
            T: PartialOrd,
        {
            assert!(lb <= value);
            assert!(value <= ub);
        }

        type C = DimensionConstraints;
        type P = PhysicalValue;
        match (constraints, value) {
            (C::UInt8([lb, ub], _), P::UInt8(v)) => case(lb, ub, v),
            (C::UInt16([lb, ub], _), P::UInt16(v)) => case(lb, ub, v),
            (C::UInt32([lb, ub], _), P::UInt32(v)) => case(lb, ub, v),
            (C::UInt64([lb, ub], _), P::UInt64(v)) => case(lb, ub, v),
            (C::Int8([lb, ub], _), P::Int8(v)) => case(lb, ub, v),
            (C::Int16([lb, ub], _), P::Int16(v)) => case(lb, ub, v),
            (C::Int32([lb, ub], _), P::Int32(v)) => case(lb, ub, v),
            (C::Int64([lb, ub], _), P::Int64(v)) => case(lb, ub, v),
            (C::Float32([lb, ub], _), P::Float32(v)) => case(lb, ub, v),
            (C::Float64([lb, ub], _), P::Float64(v)) => case(lb, ub, v),
            (C::StringAscii, _) => (),
            (_, _) => unreachable!(),
        }
    }

    fn instance_subarray_strategy_integrity(
        constraints: DimensionConstraints,
        value: Range,
    ) {
        type C = DimensionConstraints;
        type R = Range;
        type S = SingleValueRange;

        fn case<T>(l_outer: T, u_outer: T, l_inner: T, u_inner: T)
        where
            T: PartialOrd,
        {
            assert!(l_outer <= l_inner);
            assert!(l_inner <= u_inner);
            assert!(u_inner <= u_outer);
        }

        match (constraints, value) {
            (C::UInt8([l1, u1], _), R::Single(S::UInt8(l2, u2))) => {
                case(l1, u1, l2, u2)
            }
            (C::UInt16([l1, u1], _), R::Single(S::UInt16(l2, u2))) => {
                case(l1, u1, l2, u2)
            }
            (C::UInt32([l1, u1], _), R::Single(S::UInt32(l2, u2))) => {
                case(l1, u1, l2, u2)
            }
            (C::UInt64([l1, u1], _), R::Single(S::UInt64(l2, u2))) => {
                case(l1, u1, l2, u2)
            }
            (C::Int8([l1, u1], _), R::Single(S::Int8(l2, u2))) => {
                case(l1, u1, l2, u2)
            }
            (C::Int16([l1, u1], _), R::Single(S::Int16(l2, u2))) => {
                case(l1, u1, l2, u2)
            }
            (C::Int32([l1, u1], _), R::Single(S::Int32(l2, u2))) => {
                case(l1, u1, l2, u2)
            }
            (C::Int64([l1, u1], _), R::Single(S::Int64(l2, u2))) => {
                case(l1, u1, l2, u2)
            }
            (C::Float32([l1, u1], _), R::Single(S::Float32(l2, u2))) => {
                case(l1, u1, l2, u2)
            }
            (C::Float64([l1, u1], _), R::Single(S::Float64(l2, u2))) => {
                case(l1, u1, l2, u2)
            }
            (C::StringAscii, R::Var(VarValueRange::UInt8(_, _))) => {
                // this page intentionally left blank
            }
            (_, _) => unreachable!(),
        }
    }

    proptest! {
        #[test]
        fn proptest_value_strategy_integrity(
            (constraints, value) in any::<DimensionConstraints>()
                .prop_flat_map(|c| (Just(c.clone()), c.value_strategy()))
        ) {
            instance_value_strategy_integrity(constraints, value)
        }

        #[test]
        fn proptest_subarray_strategy_integrity(
            (constraints, subarray) in any::<DimensionConstraints>()
                .prop_flat_map(|c| (Just(c.clone()), c.subarray_strategy(None).unwrap()))
        ) {
            instance_subarray_strategy_integrity(constraints, subarray)
        }
    }
}
