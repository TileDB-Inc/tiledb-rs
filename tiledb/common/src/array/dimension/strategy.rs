use proptest::prelude::*;

use super::*;
use crate::datatype::physical::strategy::PhysicalValueStrategy;
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
