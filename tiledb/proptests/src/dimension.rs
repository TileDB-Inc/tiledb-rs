use proptest::test_runner::TestRng;
use rand::distributions::Uniform;
use rand::Rng;
use serde_json::json;

use tiledb::array::dimension::DimensionData;
use tiledb::array::schema::CellValNum;
use tiledb::datatype::{Datatype, LogicalType};
use tiledb::filter::list::FilterListData;
use tiledb::fn_typed;

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
        let extent_limit = if extent_limit <= DT::smallest_positive_value() {
            extent_limit + DT::smallest_positive_value()
        } else {
            extent_limit
        };

        let extent = rng.sample(Uniform::new_inclusive(
            DT::smallest_positive_value(),
            extent_limit,
        ));

        let domain = [json!(lower_bound), json!(upper_bound)];
        let extent = json!(extent);

        (Some(domain), Some(extent))
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
    datatype: Datatype,
) -> Option<FilterListData> {
    let cell_val_num = if matches!(datatype, Datatype::StringAscii) {
        CellValNum::Var
    } else {
        CellValNum::single()
    };
    if rng.gen_bool(0.5) {
        Some(filter_list::gen_for_dimension(datatype, cell_val_num, 8))
    } else {
        None
    }
}

// pub struct DimensionData {
//     pub name: String,
//     pub datatype: Datatype,
//     pub domain: Option<[serde_json::value::Value; 2]>,
//     pub extent: Option<serde_json::value::Value>,
//     pub cell_val_num: Option<CellValNum>,
//     pub filters: Option<FilterListData>,
// }

pub fn generate(rng: &mut TestRng, datatype: Datatype) -> DimensionData {
    let name = util::gen_name(rng);
    let mut dim = DimensionData {
        name,
        datatype,
        ..Default::default()
    };

    let (domain, extent) = gen_domain_and_extent(rng, datatype);
    dim.domain = domain;
    dim.extent = extent;
    dim.cell_val_num = gen_cell_val_num(rng, datatype);
    dim.filters = gen_filter_list(rng, datatype);
    dim
}
