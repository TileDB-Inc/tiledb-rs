use std::fmt::Debug;
use std::rc::Rc;

use num_traits::{Bounded, FromPrimitive, Num};
use proptest::prelude::*;
use proptest::strategy::ValueTree;

use strategy_ext::StrategyExt;
use tiledb_common::array::ArrayType;
use tiledb_common::datatype::Datatype;
use tiledb_common::datatype::physical::BitsOrd;
use tiledb_common::datatype::physical::strategy::PhysicalValueStrategy;
use tiledb_common::datatype::strategy::*;
use tiledb_common::physical_type_go;
use tiledb_utils::numbers::{
    NextDirection, NextNumericValue, SmallestPositiveValue,
};

use crate::array::dimension::DimensionConstraints;
use crate::array::dimension::DimensionData;
use crate::filter::strategy::{
    FilterPipelineStrategy, FilterPipelineValueTree,
    Requirements as FilterRequirements, StrategyContext as FilterContext,
};

impl DimensionData {
    /// Returns a strategy for generating values of this dimension's type
    /// which fall within the domain of this dimension.
    pub fn value_strategy(&self) -> PhysicalValueStrategy {
        use proptest::prelude::*;
        use tiledb_common::dimension_constraints_go;

        dimension_constraints_go!(
            self.constraints,
            DT,
            ref domain,
            _,
            PhysicalValueStrategy::from((domain[0]..=domain[1]).boxed()),
            {
                assert_eq!(self.datatype, Datatype::StringAscii);
                PhysicalValueStrategy::from(any::<u8>().boxed())
            }
        )
    }

    /// Returns a strategy for generating subarray ranges which fall within
    /// the domain of this dimension.
    ///
    /// `cell_bound` is an optional restriction on the number of possible values
    /// which the strategy is allowed to return.
    ///
    /// If `cell_bound` is `None`, then this function always returns `Some`.
    pub fn subarray_strategy(
        &self,
        cell_bound: Option<usize>,
    ) -> Option<proptest::strategy::BoxedStrategy<tiledb_common::range::Range>>
    {
        use proptest::prelude::Just;
        use proptest::strategy::Strategy;
        use tiledb_common::dimension_constraints_go;
        use tiledb_common::range::{Range, SingleValueRange, VarValueRange};

        dimension_constraints_go!(
            self.constraints,
            DT,
            ref domain,
            _,
            {
                let cell_bound = cell_bound
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
                if cell_bound.is_some() {
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

                if cell_bound.is_some() {
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
    pub array_type: Option<ArrayType>,
    pub datatype: Option<Datatype>,
    pub extent_limit: usize,
    pub filters: Option<Rc<FilterRequirements>>,
}

impl Default for Requirements {
    fn default() -> Self {
        Requirements {
            array_type: None,
            datatype: None,
            extent_limit: 1024 * 16,
            filters: None,
        }
    }
}

pub fn prop_dimension_name() -> impl Strategy<Value = String> {
    // SC-48077: bug with "" for dimension name, prevent for now
    proptest::string::string_regex("[a-zA-Z0-9_]+")
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
    let constraints = physical_type_go!(datatype, DT, {
        if !datatype.is_string_type() {
            prop_range_and_extent::<DT>(params.extent_limit)
                .prop_map(DimensionConstraints::from)
                .boxed()
        } else {
            Just(DimensionConstraints::StringAscii).boxed()
        }
    });

    constraints.prop_flat_map(move |constraints| {
        let filter_req = FilterRequirements {
            input_datatype: Some(datatype),
            context: Some(FilterContext::Dimension(
                datatype,
                constraints.cell_val_num(),
            )),
            ..params
                .filters
                .as_ref()
                .map(|rc| rc.as_ref().clone())
                .unwrap_or_default()
        };
        let filters = FilterPipelineStrategy::new(Rc::new(filter_req));
        (prop_dimension_name(), Just(constraints), filters)
            .prop_map(move |(name, constraints, filters)| DimensionData {
                name,
                datatype,
                constraints,
                filters: if filters.is_empty() {
                    None
                } else {
                    Some(filters)
                },
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
        .value_tree_map(|vt| DimensionValueTree::new(vt.current()))
        .boxed()
    }
}

#[derive(Clone, Debug)]
pub struct DimensionValueTree {
    name: String,
    datatype: Datatype,
    constraints: Just<DimensionConstraints>, // TODO: this should be shrinkable
    filters: Option<FilterPipelineValueTree>,
}

impl DimensionValueTree {
    pub fn new(dimension: DimensionData) -> Self {
        Self {
            name: dimension.name,
            datatype: dimension.datatype,
            constraints: Just(dimension.constraints),
            filters: dimension.filters.map(FilterPipelineValueTree::new),
        }
    }
}

impl ValueTree for DimensionValueTree {
    type Value = DimensionData;

    fn current(&self) -> Self::Value {
        DimensionData {
            name: self.name.clone(),
            datatype: self.datatype,
            constraints: self.constraints.current(),
            filters: self.filters.as_ref().map(|p| p.current()),
        }
    }

    fn simplify(&mut self) -> bool {
        self.constraints.simplify()
            || self.filters.as_mut().map(|p| p.simplify()).unwrap_or(false)
    }

    fn complicate(&mut self) -> bool {
        self.constraints.complicate()
            || self
                .filters
                .as_mut()
                .map(|p| p.complicate())
                .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use proptest::prelude::*;
    use proptest::strategy::Strategy;
    use tiledb_common::range::{Range, SingleValueRange};

    use super::Requirements;
    use super::*;

    #[test]
    fn subarray_strategy_dense() {
        let req = Requirements {
            array_type: Some(ArrayType::Dense),
            ..Default::default()
        };
        let strat = (
            any_with::<DimensionData>(req),
            prop_oneof![Just(None), any::<usize>().prop_map(Some)],
        )
            .prop_flat_map(|(d, cell_bound)| {
                let subarray_strat = d
                    .subarray_strategy(cell_bound)
                    .expect("Dense dimension must have a subarray strategy");
                (Just(Rc::new(d)), Just(cell_bound), subarray_strat)
            });

        proptest!(|((d, cell_bound, s) in strat)| {
            if let Some(bound) = cell_bound {
                assert!(s.num_cells().unwrap() <= bound as u128);
            }
            if let Some(num_cells) = d.constraints.num_cells() {
                assert!(s.num_cells().unwrap() <= num_cells);
            }
            let Range::Single(s) = s else {
                unreachable!("Unexpected range for dense dimension: {:?}", s)
            };
            let (start, end) = match s {
                SingleValueRange::Int8(start, end) => {
                    let DimensionConstraints::Int8([lb, ub], _) = d.constraints else { unreachable!() };
                    assert!(lb <= start);
                    assert!(end <= ub);
                    (start as i128, end as i128)
                }
                SingleValueRange::Int16(start, end) => {
                    let DimensionConstraints::Int16([lb, ub], _) = d.constraints else { unreachable!() };
                    assert!(lb <= start);
                    assert!(end <= ub);
                    (start as i128, end as i128)
                }
                SingleValueRange::Int32(start, end) => {
                    let DimensionConstraints::Int32([lb, ub], _) = d.constraints else { unreachable!() };
                    assert!(lb <= start);
                    assert!(end <= ub);
                    (start as i128, end as i128)
                }
                SingleValueRange::Int64(start, end) => {
                    let DimensionConstraints::Int64([lb, ub], _) = d.constraints else { unreachable!() };
                    assert!(lb <= start);
                    assert!(end <= ub);
                    (start as i128, end as i128)
                }
                SingleValueRange::UInt8(start, end) => {
                    let DimensionConstraints::UInt8([lb, ub], _) = d.constraints else { unreachable!() };
                    assert!(lb <= start);
                    assert!(end <= ub);
                    (start as i128, end as i128)
                }
                SingleValueRange::UInt16(start, end) => {
                    let DimensionConstraints::UInt16([lb, ub], _) = d.constraints else { unreachable!() };
                    assert!(lb <= start);
                    assert!(end <= ub);
                    (start as i128, end as i128)
                }
                SingleValueRange::UInt32(start, end) => {
                    let DimensionConstraints::UInt32([lb, ub], _) = d.constraints else { unreachable!() };
                    assert!(lb <= start);
                    assert!(end <= ub);
                    (start as i128, end as i128)
                }
                SingleValueRange::UInt64(start, end) => {
                    let DimensionConstraints::UInt64([lb, ub], _) = d.constraints else { unreachable!() };
                    assert!(lb <= start);
                    assert!(end <= ub);
                    (start as i128, end as i128)
                },
                s => unreachable!("Unexpected range type for dense dimension: {:?}", s)
            };
            assert_eq!(Some((end - start + 1) as u128), s.num_cells());
        });
    }
}
