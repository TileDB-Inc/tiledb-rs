use std::fmt::Debug;

use num_traits::{Bounded, Num};
use proptest::prelude::*;
use serde_json::json;

use tiledb::array::dimension::DimensionData;
use tiledb::array::schema::{ArrayType, CellValNum};
use tiledb::datatype::{Datatype, LogicalType};
use tiledb::fn_typed;

use tiledb_utils::numbers::{
    NextDirection, NextNumericValue, SmallestPositiveValue,
};

use crate::datatype as pt_datatype;
use crate::filter::list as pt_list;

pub fn prop_dimension_name() -> impl Strategy<Value = String> {
    let name = proptest::string::string_regex("[a-zA-Z0-9][a-zA-Z0-9_]*")
        .expect("Error creating dimension name property.");
    name.prop_flat_map(|name| {
        if name.starts_with("__") {
            Just("d".to_string() + &name)
        } else {
            Just(name)
        }
    })
}

/// Construct a strategy to generate valid (domain, extent) pairs.
/// A valid output satisfies
/// `lower < lower + extent <= upper < upper + extent <= type_limit`.
///
/// I was too lazy to refigure out the math so I just stole the one that
/// Ryan wrote on main.
fn domain_and_extent_impl<T>() -> impl Strategy<Value = ([T; 2], T)>
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

pub fn add_domain_and_extent(
    dim: DimensionData,
) -> BoxedStrategy<DimensionData> {
    if matches!(dim.datatype, Datatype::StringAscii) {
        return Just(dim).boxed();
    }

    let prop = fn_typed!(dim.datatype, LT, {
        type DT = <LT as LogicalType>::PhysicalType;
        domain_and_extent_impl::<DT>()
            .prop_flat_map(|(range, extent)| {
                let range = Some([json!(range[0]), json!(range[1])]);
                let extent = Some(json!(extent));
                Just((range, extent))
            })
            .boxed()
    });

    (Just(dim), prop)
        .prop_flat_map(|(mut dim, (domain, extent))| {
            dim.domain = domain;
            dim.extent = extent;
            Just(dim)
        })
        .boxed()
}

fn add_cell_val_num(
    dim: DimensionData,
) -> impl Strategy<Value = DimensionData> {
    let prop = if matches!(dim.datatype, Datatype::StringAscii) {
        Just(CellValNum::Var)
    } else {
        Just(CellValNum::single())
    };

    (Just(dim), prop).prop_map(|(mut dim, cvn)| {
        dim.cell_val_num = Some(cvn);
        dim
    })
}

fn add_filters(dim: DimensionData) -> impl Strategy<Value = DimensionData> {
    // I'm forcing all "optional" values to be set on generation. Once I have
    // complete schemas being generated I think I'm going to add a custom
    // strategy for schemas so that shrinking starts setting random options
    // to None.
    let cvn = dim.cell_val_num.unwrap();
    let filters = pt_list::prop_filter_list(dim.datatype, cvn, 8);
    (Just(dim), filters).prop_flat_map(|(mut dim, filters)| {
        dim.filters = Some(filters);
        Just(dim)
    })
}

pub fn prop_dimension_data_for_type(
    datatype: Datatype,
) -> BoxedStrategy<DimensionData> {
    let name = prop_dimension_name();
    (name, Just(datatype))
        .prop_flat_map(|(name, datatype)| {
            let dim = DimensionData {
                name,
                datatype,
                ..Default::default()
            };

            add_domain_and_extent(dim)
                .prop_flat_map(add_cell_val_num)
                .prop_flat_map(add_filters)
        })
        .boxed()
}

pub fn prop_dimension_data(
    array_type: ArrayType,
) -> BoxedStrategy<DimensionData> {
    let name = prop_dimension_name();
    let datatype = if matches!(array_type, ArrayType::Dense) {
        pt_datatype::prop_dense_dimension_datatypes().boxed()
    } else {
        pt_datatype::prop_sparse_dimension_datatypes().boxed()
    };
    (name, datatype)
        .prop_flat_map(|(name, datatype)| {
            let dim = DimensionData {
                name,
                datatype,
                ..Default::default()
            };

            add_domain_and_extent(dim)
                .prop_flat_map(add_cell_val_num)
                .prop_flat_map(add_filters)
        })
        .boxed()
}
