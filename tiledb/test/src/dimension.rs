use num_traits::{Bounded, Num};
use proptest::prelude::*;
use std::fmt::Debug;
use tiledb::array::{Dimension, DimensionBuilder};
use tiledb::context::Context;
use tiledb::fn_typed;
use tiledb::Result as TileDBResult;

use crate::strategy::LifetimeBoundStrategy;

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
    let one = <T as num_traits::One>::one();
    let lower_limit = <T as Bounded>::min_value();
    let upper_limit = <T as Bounded>::max_value();
    std::ops::Range::<T> {
        start: lower_limit + one + one,
        end: upper_limit - one,
    }
    .prop_flat_map(move |upper_bound| {
        (
            std::ops::Range::<T> {
                start: lower_limit + one,
                end: upper_bound - one,
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

                    let extent_bound =
                        if upper_limit - extent_limit < upper_bound {
                            upper_limit - upper_bound
                        } else {
                            extent_limit
                        };
                    extent_bound
                },
            },
        )
    })
}

pub fn arbitrary<'ctx>(
    context: &'ctx Context,
) -> impl Strategy<Value = TileDBResult<Dimension>> {
    (
        crate::datatype::arbitrary_conv(),
        crate::attribute::arbitrary_name(),
    )
        .prop_flat_map(|(dt, name)| {
            fn_typed!(arbitrary_range_and_extent, dt =>
                (Just(dt), Just(name), arbitrary_range_and_extent).prop_map(|(dt, name, values)| {
                    DimensionBuilder::new(context, name.as_ref(), dt, &values.0, &values.1)
                        .map(|b| b.build())
            }).bind())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that the arbitrary attribute construction always succeeds
    #[test]
    fn dimension_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(maybe_dimension in arbitrary(&ctx))| {
            maybe_dimension.expect("Error constructing arbitrary dimension");
        });
    }
}
