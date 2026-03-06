use std::fmt::Debug;
use std::rc::Rc;

use proptest::prelude::*;
use proptest::strategy::ValueTree;

use strategy_ext::StrategyExt;
use tiledb_common::array::ArrayType;
use tiledb_common::datatype::Datatype;
use tiledb_common::datatype::physical::strategy::PhysicalValueStrategy;
use tiledb_common::datatype::strategy::*;

use crate::array::dimension::DimensionConstraints;
use crate::array::dimension::DimensionData;
use crate::filter::strategy::{
    FilterPipelineStrategy, FilterPipelineValueTree,
    Requirements as FilterRequirements, StrategyContext as FilterContext,
};

pub use tiledb_common::array::dimension::strategy::{
    DimensionConstraintType, Requirements as DimensionConstraintRequirements,
};

impl DimensionData {
    /// Returns a strategy for generating values of this dimension's type
    /// which fall within the domain of this dimension.
    pub fn value_strategy(&self) -> PhysicalValueStrategy {
        self.constraints.value_strategy()
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
        self.constraints.subarray_strategy(cell_bound)
    }
}

#[derive(Clone)]
pub struct Requirements {
    pub name: Option<BoxedStrategy<String>>,
    pub array_type: Option<ArrayType>,
    pub datatype: Option<Datatype>,
    pub extent_limit: usize,
    pub filters: Option<Rc<FilterRequirements>>,
}

impl Default for Requirements {
    fn default() -> Self {
        Requirements {
            name: None,
            array_type: None,
            datatype: None,
            extent_limit: 1024 * 16,
            filters: None,
        }
    }
}

const DIMENSION_NAME_REGEX: &str = "[a-zA-Z0-9_]+";

pub fn prop_dimension_name() -> impl Strategy<Value = String> {
    // SC-48077: bug with "" for dimension name, prevent for now
    proptest::string::string_regex(DIMENSION_NAME_REGEX)
        .expect("Error creating dimension name strategy")
}

fn prop_dimension_for_datatype(
    datatype: Datatype,
    params: Requirements,
) -> impl Strategy<Value = DimensionData> {
    let constraints = {
        let dimension_type = DimensionConstraintType::try_from(datatype)
            .expect("Invalid dimension type");
        let params = DimensionConstraintRequirements {
            datatype: Some(dimension_type),
            extent_limit: params.extent_limit,
        };
        any_with::<DimensionConstraints>(params)
    };

    let strat_dimension_name =
        params.name.clone().unwrap_or(prop_dimension_name().boxed());

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
        (strat_dimension_name.clone(), Just(constraints), filters)
            .prop_map(move |(name, constraints, filters)| DimensionData {
                name,
                datatype,
                constraints,
                filters,
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
    filters: FilterPipelineValueTree,
}

impl DimensionValueTree {
    pub fn new(dimension: DimensionData) -> Self {
        Self {
            name: dimension.name,
            datatype: dimension.datatype,
            constraints: Just(dimension.constraints),
            filters: FilterPipelineValueTree::new(dimension.filters),
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
            filters: self.filters.current(),
        }
    }

    fn simplify(&mut self) -> bool {
        self.constraints.simplify() || self.filters.simplify()
    }

    fn complicate(&mut self) -> bool {
        self.constraints.complicate() || self.filters.complicate()
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use proptest::prelude::*;
    use proptest::strategy::Strategy;
    use regex::Regex;
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

    const LOWERCASE_NAME_REGEX: &str = "[a-z]+";

    fn strat_requirements_name() -> impl Strategy<Value = DimensionData> {
        let r = Requirements {
            name: proptest::string::string_regex(LOWERCASE_NAME_REGEX)
                .expect("Unexpected invalid regex")
                .boxed()
                .into(),
            ..Default::default()
        };
        any_with::<DimensionData>(r)
    }

    proptest! {
        #[test]
        fn default_name(dimension in any::<DimensionData>()) {
            assert!(Regex::new(DIMENSION_NAME_REGEX).unwrap().is_match(&dimension.name));
        }

        #[test]
        fn requirements_name(dimension in strat_requirements_name()) {
            assert!(Regex::new(LOWERCASE_NAME_REGEX).unwrap().is_match(&dimension.name));
        }
    }
}
