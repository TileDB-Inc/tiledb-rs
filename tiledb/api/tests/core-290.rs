use std::collections::HashMap;
use std::ops::RangeInclusive;

use cells::write::DenseWriteInput;
use cells::{Cells, FieldData};
use itertools::Itertools;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;
use strategy_ext::StrategyExt;
use tiledb_api::array::fragment_info::Builder as FragmentMetadataBuilder;
use tiledb_api::array::schema::Schema;
use tiledb_api::array::Array;
use tiledb_api::config::Config;
use tiledb_api::filter::FilterListBuilder;
use tiledb_api::query::condition::strategy::QueryConditionValueTree;
use tiledb_api::query::{
    Query, QueryBuilder, QueryConditionExpr, QueryLayout, ReadBuilder,
    ReadQuery, ReadQueryBuilder, ToWriteQuery, WriteBuilder,
};
use tiledb_api::Context;
use tiledb_common::array::dimension::DimensionConstraints;
use tiledb_common::array::{ArrayType, CellOrder, CellValNum, Mode, TileOrder};
use tiledb_common::filter::{CompressionData, CompressionType, FilterData};
use tiledb_common::range::SingleValueRange;
use tiledb_common::Datatype;
use uri::TestArrayUri;

fn repro_schema(ctx: &Context) -> anyhow::Result<Schema> {
    const ARRAY_URI: &'static str = "/home/ryan/Documents/tiledb/stories/core-290/consolidate_err/consolidate_err_db/";

    Ok(Array::open(ctx, ARRAY_URI, Mode::Read)?.schema()?)
}

fn repro_consolidation_config() -> anyhow::Result<Config> {
    Ok(Config::new()?
        .with("sm.consolidation.mode", "fragments")?
        .with("sm.vacuum.mode", "fragments")?
        .with("sm.compute_concurrency_level", "1")?
        .with("sm.io_concurrency_level", "1")?
        .with("sm.consolidation.step_size_ratio", "0.5")?
        .with("sm.consolidation.step_min_frags", "2")?
        .with("sm.consolidation.step_max_frags", "1024")?
        .with("sm.consolidation.amplification", "1.1")?
        .with("sm.consolidation.max_fragment_size", "1073741824")?)
}

#[derive(Debug)]
struct Core290Input {
    fragments: Vec<Core290Write>,
}

impl Arbitrary for Core290Input {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        proptest::collection::vec(
            strat_core_290_write_input(1282876277, 1282880241),
            1..=1024,
        )
        .prop_map(|fragments| Core290Input { fragments })
        .boxed()
    }
}

#[derive(Debug)]
struct Core290Write {
    z: RangeInclusive<u64>,
    s: RangeInclusive<u64>,
}

impl Core290Write {
    fn to_write_input(&self) -> DenseWriteInput {
        let num_z = self.z.end() - self.z.start() + 1;
        let num_s = self.s.end() - self.s.start() + 1;
        let num_amplitude = (num_z * num_s) as usize;

        DenseWriteInput {
            layout: CellOrder::RowMajor,
            data: Cells::new(HashMap::from([(
                "amplitude".to_owned(),
                (0..num_amplitude)
                    .map(|i| i as f32)
                    .collect::<Vec<f32>>()
                    .into(),
            )])),
            subarray: vec![
                SingleValueRange::from(self.z.clone()),
                SingleValueRange::UInt64(0, 0),
                SingleValueRange::UInt64(0, 0),
                SingleValueRange::from(self.s.clone()),
            ],
        }
    }
}

fn strat_core_290_write_input(
    z_min: u64,
    z_max: u64,
) -> impl Strategy<Value = Core290Write> {
    // the only description we get is "frequent small writes"
    // for now perhaps we assume that each write is a "hyper-row" of the 4D array
    let strat_range_inclusive = |lb, ub, limit| {
        (lb..=ub)
            .prop_flat_map(move |lb| {
                if let Some(limit) = limit {
                    let ub_limit = std::cmp::min(lb + limit, ub);
                    (Just(lb), lb..=ub_limit)
                } else {
                    (Just(lb), lb..=ub)
                }
            })
            .prop_map(|(lb, ub)| lb..=ub)
    };

    let z_range = strat_range_inclusive(z_min, z_max, Some(128));
    let s_range = strat_range_inclusive(0, 65535u64, None);

    (z_range, s_range).prop_map(|(z, s)| Core290Write { z, s })
}

fn instance(input: Core290Input) -> anyhow::Result<()> {
    let test_uri = uri::get_uri_generator()?;
    let uri = test_uri.with_path("array")?;

    let ctx = Context::new()?;

    Array::create(&ctx, &uri, repro_schema(&ctx)?)?;

    let _ = input
        .fragments
        .into_iter()
        .map(|w290| w290.to_write_input())
        .try_fold(Array::open(&ctx, &uri, Mode::Write)?, |a, f| {
            let w = f.attach_write(WriteBuilder::new(a)?)?.build();
            w.submit()?;
            w.finalize()
        })?;

    Ok(Array::consolidate(
        &ctx,
        &uri,
        Some(&repro_consolidation_config()?),
    )?)
}

#[test]
fn example_core290() -> anyhow::Result<()> {
    let input = Core290Input {
        fragments: vec![Core290Write {
            z: 1282876277..=1282876674,
            s: 0..=65535,
        }],
    };
    instance(input)
}

proptest! {
    #[test]
    fn proptest_core290(input in any::<Core290Input>()) {
        instance(input).expect("Error in instance")
    }
}
