#[cfg(feature = "option-subset")]
use tiledb_utils::option::OptionSubset;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use tiledb_common::array::dimension::DimensionConstraints;
use tiledb_common::array::CellValNum;
use tiledb_common::datatype::Datatype;
use tiledb_common::filter::FilterData;

/// Encapsulation of data needed to construct a Dimension
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct DimensionData {
    pub name: String,
    pub datatype: Datatype,
    pub constraints: DimensionConstraints,

    /// Optional filters to apply to the dimension. If None or Some(empty),
    /// then filters will be inherited from the schema's `coordinate_filters`
    /// field when the array is constructed.
    pub filters: Option<Vec<FilterData>>,
}

impl DimensionData {
    pub fn cell_val_num(&self) -> CellValNum {
        self.constraints.cell_val_num()
    }
}

#[cfg(any(test, feature = "proptest-strategies"))]
impl DimensionData {
    /// Returns a strategy for generating values of this dimension's type
    /// which fall within the domain of this dimension.
    pub fn value_strategy(&self) -> crate::query::strategy::FieldValueStrategy {
        use crate::query::strategy::FieldValueStrategy;
        use proptest::prelude::*;

        dimension_constraints_go!(
            self.constraints,
            DT,
            ref domain,
            _,
            FieldValueStrategy::from((domain[0]..=domain[1]).boxed()),
            {
                assert_eq!(self.datatype, Datatype::StringAscii);
                FieldValueStrategy::from(any::<u8>().boxed())
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
    ) -> Option<proptest::strategy::BoxedStrategy<crate::range::Range>> {
        use proptest::prelude::Just;
        use proptest::strategy::Strategy;

        use crate::range::{Range, VarValueRange};

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

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;
