use std::rc::Rc;

use proptest::prelude::*;

use crate::array::schema::{FieldData as SchemaField, SchemaData};
use crate::array::CellValNum;
use crate::query::read::aggregate::AggregateFunction;
use crate::query::QueryLayout;
use crate::Datatype;

pub enum AggregateFunctionContext {
    Field(SchemaField),
    Schema(Rc<SchemaData>),
}

impl Arbitrary for AggregateFunction {
    type Parameters = Option<AggregateFunctionContext>;
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        match params {
            None => unimplemented!(), /* not hard but not important */
            Some(AggregateFunctionContext::Field(f)) => {
                let arg = || f.name().to_string();
                let mut strats = vec![Just(AggregateFunction::Count)];
                if f.nullability().unwrap_or(true) {
                    strats.push(Just(AggregateFunction::NullCount(arg())));
                }

                let datatype = f.datatype();
                let cell_val_num =
                    f.cell_val_num().unwrap_or(CellValNum::single());

                let mut try_agg = |agg: AggregateFunction| {
                    if !agg
                        .result_type_impl(Some((datatype, cell_val_num)))
                        .is_some()
                    {
                        return;
                    }
                    strats.push(Just(agg));
                };
                try_agg(AggregateFunction::Sum(arg()));

                if !is_unsupported_min_max_datatype(datatype) {
                    try_agg(AggregateFunction::Min(arg()));
                    try_agg(AggregateFunction::Max(arg()));
                }

                if datatype != Datatype::Boolean
                    && (datatype.is_integral_type() || datatype.is_real_type())
                {
                    try_agg(AggregateFunction::Mean(arg()));
                }

                proptest::strategy::Union::new(strats).no_shrink().boxed()
            }
            Some(AggregateFunctionContext::Schema(s)) => s
                .strat_field()
                .prop_flat_map(|f| {
                    any_with::<Self>(Some(AggregateFunctionContext::Field(f)))
                })
                .boxed(),
        }
    }
}

/// Returns whether a datatype is supported for the min/max aggregation.
// See `apply_with_type` in the core library
fn is_unsupported_min_max_datatype(dt: Datatype) -> bool {
    matches!(
        dt,
        Datatype::Boolean
            | Datatype::Char
            | Datatype::StringAscii
            | Datatype::StringUtf8
            | Datatype::StringUtf16
            | Datatype::StringUtf32
            | Datatype::StringUcs2
            | Datatype::StringUcs4
            | Datatype::Blob
            | Datatype::GeometryWkt
            | Datatype::GeometryWkb
    )
}

#[cfg(test)]
mod tests {
    use proptest::test_runner::TestRunner;
    use tiledb_test_utils::TestArrayUri;

    use super::*;
    use crate::array::{Array, Mode};
    use crate::datatype::physical::BitsOrd;
    use crate::error::Error;
    use crate::query::read::{AggregateQueryBuilder, ReadBuilder};
    use crate::query::strategy::{Cells, FieldData};
    use crate::query::write::strategy::{
        SparseWriteInput, SparseWriteParameters,
    };
    use crate::query::{Query, QueryBuilder, ReadQuery, WriteBuilder};
    use crate::{
        typed_field_data_go, Context, Factory, Result as TileDBResult,
    };

    /// This test should fail when SC-52312 is resolved.
    /// When that happens we can update the strategies to
    /// yield more function types per attribute/dimension.
    #[test]
    fn sc_52312() {
        todo!()
    }

    /// Test that all aggregate functions produced by
    /// the `Arbitrary` implementation do not result in errors in queries.
    #[test]
    fn strategy_validity() {
        // schema with all datatypes used in attributes and dimensions
        let schema = Rc::new(crate::tests::examples::sparse_all::schema(
            Default::default(),
        ));

        let test_uri = tiledb_test_utils::get_uri_generator()
            .map_err(|e| Error::Other(e.to_string()))
            .unwrap();
        let uri = test_uri
            .with_path("aggregate_strategy_validity")
            .map_err(|e| Error::Other(e.to_string()))
            .unwrap();

        let c: Context = Context::new().unwrap();
        {
            let s = schema.create(&c).unwrap();
            Array::create(&c, &uri, s).unwrap()
        }

        let mut runner = TestRunner::new(Default::default());

        // generate test data
        let input = {
            let strat_input =
                any_with::<SparseWriteInput>(SparseWriteParameters {
                    schema: Some(Rc::clone(&schema)),
                    ..Default::default()
                });

            strat_input.new_tree(&mut runner).unwrap().current()
        };

        // insert to the array
        {
            let w = input
                .attach_write(
                    WriteBuilder::new(
                        Array::open(&c, &uri, Mode::Write).unwrap(),
                    )
                    .unwrap(),
                )
                .unwrap()
                .build();
            w.submit().unwrap();
            w.finalize().unwrap();
        }

        let strat_agg = any_with::<AggregateFunction>(Some(
            AggregateFunctionContext::Schema(Rc::clone(&schema)),
        ))
        .no_shrink();

        runner
            .run(&strat_agg, |agg| {
                Ok(do_validate_agg(&c, &uri, &input.data, agg))
            })
            .unwrap_or_else(|e| panic!("{}\nWrite input = {:?}", e, input));
    }

    /// Test that an aggregate function produced by the `Arbitrary`
    /// implementation is valid within the schema that parameterized it
    fn do_validate_agg(
        context: &Context,
        uri: &str,
        contents: &Cells,
        agg: AggregateFunction,
    ) {
        match agg {
            AggregateFunction::Count => {
                do_validate_agg_count(context, uri, contents)
            }
            AggregateFunction::NullCount(ref s) => {
                do_validate_agg_null_count(context, uri, s, contents)
            }
            AggregateFunction::Mean(ref s) => {
                do_validate_agg_mean(context, uri, s, contents)
            }
            AggregateFunction::Min(ref s) => {
                do_validate_agg_min_max(context, uri, s, true, contents)
            }
            AggregateFunction::Max(ref s) => {
                do_validate_agg_min_max(context, uri, s, false, contents)
            }
            AggregateFunction::Sum(ref s) => {
                do_validate_agg_sum(context, uri, s, contents)
            }
        }
    }

    /// Helper function for starting to build aggregate queries on sparse schema
    fn rstart(context: &Context, uri: &str) -> TileDBResult<ReadBuilder> {
        let a = Array::open(context, uri, Mode::Read).unwrap();
        ReadBuilder::new(a).and_then(|b| b.layout(QueryLayout::Unordered))
    }

    /// Validate `AggregateFunction::Count`
    fn do_validate_agg_count(context: &Context, uri: &str, contents: &Cells) {
        let mut q = rstart(context, uri)
            .unwrap()
            .count()
            .map(|b| b.build())
            .unwrap();
        let (count, _) = q.execute().expect("Count aggregate unsupported");
        assert_eq!(Some(contents.len() as u64), count);
    }

    /// Validate `AggregateFunction::NullCount(field)`
    fn do_validate_agg_null_count(
        context: &Context,
        uri: &str,
        field: &str,
        contents: &Cells,
    ) {
        let expect_null_count =
            contents.fields().get(field).unwrap().null_count() as u64;

        let mut q = rstart(context, uri)
            .unwrap()
            .null_count(field)
            .map(|b| b.build())
            .unwrap();
        let (actual_null_count, _) =
            q.execute().expect("Null count aggregate unsupported");

        assert_eq!(Some(expect_null_count), actual_null_count);
    }

    /// Validate `AggregateFunction::Mean(field)`
    fn do_validate_agg_mean(
        context: &Context,
        uri: &str,
        field: &str,
        contents: &Cells,
    ) {
        let field_data = contents.fields().get(field).unwrap();

        macro_rules! integral_expect_mean {
            ($FIELD_TYPE:ty) => {{
                let field_as_type =
                    Vec::<$FIELD_TYPE>::try_from(field_data.clone()).unwrap();
                let expected_sum = field_as_type
                    .into_iter()
                    .map(|v| v as u64)
                    .try_fold(0u64, |a, b| a.checked_add(b));

                expected_sum.map(|v| v as f64)
            }};
        }
        let expect_sum = match field_data {
            FieldData::UInt8(_) => integral_expect_mean!(u8),
            FieldData::UInt16(_) => integral_expect_mean!(u16),
            FieldData::UInt32(_) => integral_expect_mean!(u32),
            FieldData::UInt64(_) => integral_expect_mean!(u64),
            FieldData::Int8(_) => integral_expect_mean!(i8),
            FieldData::Int16(_) => integral_expect_mean!(i16),
            FieldData::Int32(_) => integral_expect_mean!(i32),
            FieldData::Int64(_) => integral_expect_mean!(i64),
            FieldData::Float32(ref values) => {
                Some(values.iter().map(|f| *f as f64).sum::<f64>())
            }
            FieldData::Float64(ref values) => Some(values.iter().sum::<f64>()),
            _ => unreachable!(),
        };

        if let Some(expect_sum) = expect_sum {
            let expect_mean = expect_sum / (field_data.len() as f64);

            let mut q = rstart(context, uri)
                .unwrap()
                .mean(field)
                .map(|b| b.build())
                .unwrap();
            let (actual_mean, _) =
                q.execute().expect("Mean aggregate unsupported");

            let delta = expect_mean.to_bits() - actual_mean.unwrap().to_bits();
            assert!(
                delta <= 100,
                "expect_mean = {}, actual_mean = {}",
                expect_mean,
                actual_mean.unwrap()
            );
        }
    }

    /// Validate `AggregateFunction::Sum(field)`
    fn do_validate_agg_sum(
        context: &Context,
        uri: &str,
        field: &str,
        contents: &Cells,
    ) {
        let field_data = contents.fields().get(field).unwrap();

        macro_rules! integral_expect_sum {
            ($FIELD_TYPE:ty, $AGG_TYPE:ty) => {{
                let mut q = rstart(context, uri)
                    .unwrap()
                    .sum::<$AGG_TYPE>(field)
                    .map(|b| b.build())
                    .unwrap();
                let (actual_sum, _) = q.execute().unwrap();

                let field_as_type =
                    Vec::<$FIELD_TYPE>::try_from(field_data.clone()).unwrap();
                let expected_sum = field_as_type
                    .into_iter()
                    .map(|v| v as $AGG_TYPE)
                    .try_fold(0 as $AGG_TYPE, |a, b| a.checked_add(b));

                if let Some(expected_sum) = expected_sum {
                    assert_eq!(Some(expected_sum), actual_sum);
                } else {
                    // otherwise we have an overflow, probably we don't
                    // want to assert on undefined behavior in core
                }
            }};
        }

        match field_data {
            FieldData::UInt8(_) => integral_expect_sum!(u8, u64),
            FieldData::UInt16(_) => integral_expect_sum!(u16, u64),
            FieldData::UInt32(_) => integral_expect_sum!(u32, u64),
            FieldData::UInt64(_) => integral_expect_sum!(u64, u64),
            FieldData::Int8(_) => integral_expect_sum!(i8, i64),
            FieldData::Int16(_) => integral_expect_sum!(i16, i64),
            FieldData::Int32(_) => integral_expect_sum!(i32, i64),
            FieldData::Int64(_) => integral_expect_sum!(i64, i64),
            FieldData::Float32(ref field_data) => {
                let expected_sum =
                    field_data.iter().map(|v| *v as f64).sum::<f64>();

                let mut q = rstart(context, uri)
                    .unwrap()
                    .sum::<f64>(field)
                    .map(|b| b.build())
                    .unwrap();
                let (actual_sum, _) = q.execute().unwrap();

                if let Some(actual_sum) = actual_sum {
                    if expected_sum != actual_sum {
                        // classic floating point precision issues
                        let delta = actual_sum - expected_sum;
                        assert!(delta <= 0.00000000001);
                    }
                } else {
                    assert_eq!(0, contents.len());
                }
            }
            FieldData::Float64(ref field_data) => {
                let expected_sum = field_data.iter().sum::<f64>();

                let mut q = rstart(context, uri)
                    .unwrap()
                    .sum::<f64>(field)
                    .map(|b| b.build())
                    .unwrap();
                let (actual_sum, _) = q.execute().unwrap();
                let actual_sum = actual_sum.unwrap();

                if expected_sum != actual_sum {
                    // classic floating point precision issues
                    let delta = actual_sum - expected_sum;
                    assert!(delta <= 0.00000000001);
                }
            }
            _ => {
                // NB: this test is not "what happens if we run this aggregate",
                // but "are all the aggregates produced by the strategy valid",
                // meaning we should be unreachable rather than checking an error
                unreachable!()
            }
        }
    }

    /// Validate `AggregateFunction::Min(field)` or `AggregateFunction::Max(field)`
    fn do_validate_agg_min_max(
        context: &Context,
        uri: &str,
        field: &str,
        is_min: bool,
        contents: &Cells,
    ) {
        let agg = if is_min {
            AggregateFunction::Min(field.to_owned())
        } else {
            AggregateFunction::Max(field.to_owned())
        };

        let field_data = contents.fields().get(field).unwrap();

        /* return type is the same as the argument type */
        typed_field_data_go!(
            field_data,
            _DT,
            ref _field_data,
            {
                let expect_extremum = if is_min {
                    _field_data.iter().min_by(BitsOrd::bits_cmp).copied()
                } else {
                    _field_data.iter().max_by(BitsOrd::bits_cmp).copied()
                };

                let mut q = rstart(context, uri)
                    .unwrap()
                    .apply_aggregate::<_DT>(agg)
                    .map(|b| b.build())
                    .unwrap();
                let (actual_extremum, _) = q.execute().unwrap();

                assert_eq!(expect_extremum, actual_extremum);
            },
            unreachable!()
        );
    }
}
