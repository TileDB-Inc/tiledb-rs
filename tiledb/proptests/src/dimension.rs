use std::collections::HashSet;

use proptest::test_runner::TestRng;
use rand::distributions::Uniform;
use rand::Rng;
use serde_json::json;

use tiledb::array::dimension::DimensionData;
use tiledb::array::schema::CellValNum;
use tiledb::array::schema::SchemaData;
use tiledb::datatype::{Datatype, LogicalType};
use tiledb::filter::list::FilterListData;
use tiledb::{fn_typed, Result as TileDBResult};

use tiledb_utils::numbers::SmallestPositiveValue;

use crate::filter_list;
use crate::util;

/// Construct a strategy to generate valid (domain, extent) pairs.
/// A valid output satisfies
/// `lower < lower + extent <= upper < upper + extent <= type_limit`.
///
/// Original credit for this goes to @rroelke who figured out the math for
/// generating these ranges.
fn domain_and_extent_impl(
    rng: &mut TestRng,
    datatype: Datatype,
) -> (Option<[serde_json::Value; 2]>, Option<serde_json::Value>) {
    // Either our use of the proptest library is not awesome, or its design
    // isn't awesome. But something seems wrong. I've been chasing performance
    // issues in test case generation/shrinkage of tests and its basically been
    // a lot of "Well, don't use that proptest thing" which seems not great.
    // I've gone and completely removed almost all proptest builtins from my
    // schema generator and replaced it with a direct generation approach.
    //
    // Unfortunately, one thing that proptest does have is a "give me any float"
    // behavior that doesn't appear to exist outside of that implemenation.
    // Which means I have to deal with generating random floats in a vaguely
    // crazy manner. This following code exists to avoid the issues with how
    // rand::distribution::Uniform is implemented since -f64::MAX..f64::MAX
    // breaks things.
    if matches!(datatype, Datatype::Float32 | Datatype::Float64) {
        let lower_bound = -1_000_000.0f32;
        let upper_bound = 1_000_000.0f32;
        let domain = Some([json!(lower_bound), json!(upper_bound)]);
        let extent = if rng.gen_bool(0.5) {
            Some(json!(rng.gen_range(1.0f32..2048.0f32)))
        } else {
            None
        };

        return (domain, extent);
    }

    fn_typed!(datatype, LT, {
        type DT = <LT as LogicalType>::PhysicalType;
        // lower_limit has +3 for the worst case when the upper bound is picked
        // as this extreme so that we have room for lower bound and extent.
        let lower_limit = DT::MIN + 3 as DT;
        // upper_limit is -1 so that we have enough room for a minimum extent
        // of one.
        let upper_limit = DT::MAX - 1 as DT;

        let upper_bound =
            rng.sample(Uniform::new_inclusive(lower_limit, upper_limit));
        let lower_bound =
            rng.sample(Uniform::new_inclusive(DT::MIN + 1 as DT, upper_bound));

        let domain = Some([json!(lower_bound), json!(upper_bound)]);

        let extent = if rng.gen_bool(0.5) {
            let extent_limit = {
                let zero = 0 as DT;
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
            let extent_limit = if extent_limit <= DT::smallest_positive_value()
            {
                extent_limit + DT::smallest_positive_value()
            } else {
                extent_limit
            };

            let extent = rng.sample(Uniform::new_inclusive(
                DT::smallest_positive_value(),
                extent_limit,
            ));

            Some(json!(extent))
        } else {
            None
        };

        (domain, extent)
    })
}

fn gen_domain_and_extent(
    rng: &mut TestRng,
    datatype: Datatype,
) -> (Option<[serde_json::Value; 2]>, Option<serde_json::Value>) {
    if matches!(datatype, Datatype::StringAscii) {
        return (None, None);
    }

    domain_and_extent_impl(rng, datatype)
}

fn gen_cell_val_num(
    rng: &mut TestRng,
    datatype: Datatype,
) -> Option<CellValNum> {
    if rng.gen_bool(0.5) {
        if matches!(datatype, Datatype::StringAscii) {
            Some(CellValNum::Var)
        } else {
            Some(CellValNum::single())
        }
    } else {
        None
    }
}

fn gen_filter_list(
    rng: &mut TestRng,
    schema: &SchemaData,
    dim: &DimensionData,
) -> TileDBResult<Option<FilterListData>> {
    if rng.gen_bool(0.5) {
        Ok(Some(filter_list::gen_for_dimension(rng, schema, dim)?))
    } else {
        Ok(None)
    }
}

pub fn generate(
    rng: &mut TestRng,
    schema: &SchemaData,
    datatype: Datatype,
    field_names: &mut HashSet<String>,
) -> TileDBResult<DimensionData> {
    let name = util::gen_name(rng, field_names);
    let mut dim = DimensionData {
        name,
        datatype,
        ..Default::default()
    };

    let (domain, extent) = gen_domain_and_extent(rng, datatype);
    dim.domain = domain;
    dim.extent = extent;
    dim.cell_val_num = gen_cell_val_num(rng, datatype);
    dim.filters = gen_filter_list(rng, schema, &dim)?;
    Ok(dim)
}
