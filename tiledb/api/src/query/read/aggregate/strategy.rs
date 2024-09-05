use std::rc::Rc;

use proptest::prelude::*;

use crate::array::schema::{FieldData as SchemaField, SchemaData};
use crate::array::CellValNum;
use crate::query::read::aggregate::AggregateFunction;
use crate::Datatype;

/// Context in which an aggregate function will be run,
/// for [`AggregateFunction`]'s [`Arbitrary`] implementation.
/// Function arguments will be selected from the fields available in the context.
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
                let strats =
                    supported_aggregate_functions(&f).into_iter().map(Just);
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

fn supported_aggregate_functions(
    field: &SchemaField,
) -> Vec<AggregateFunction> {
    let arg = || field.name().to_string();

    let mut aggs = Vec::new();

    if !is_unsupported_null_count_field(field) {
        aggs.push(AggregateFunction::NullCount(arg()));
    }

    let datatype = field.datatype();
    let cell_val_num = field.cell_val_num().unwrap_or(CellValNum::single());

    let mut try_agg = |agg: AggregateFunction| {
        if agg
            .result_type_impl(Some((datatype, cell_val_num)))
            .is_none()
        {
            return;
        }
        aggs.push(agg);
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

    aggs
}

/// Returns whether a field is supported for the null count aggregation.
fn is_unsupported_null_count_field(f: &SchemaField) -> bool {
    if !f.nullability().unwrap_or(false) {
        // SC-52312: error on non-nullable fields
        true
    } else if f
        .cell_val_num()
        .unwrap_or(CellValNum::single())
        .is_var_sized()
    {
        // SC-53791: also error on most Var attributes seemingly.
        // Datatypes not in this list cannot have null count run on them when they are var sized.
        !matches!(f.datatype(), Datatype::Char | Datatype::StringAscii)
    } else {
        // SC-53791: it's not just Var attributes
        // Datatypes not in this list cannot have null count run on them when they are single-value
        // cells.
        matches!(
            f.datatype(),
            Datatype::StringUtf8
                | Datatype::StringUtf16
                | Datatype::StringUtf32
                | Datatype::StringUcs2
                | Datatype::StringUcs4
                | Datatype::Blob
                | Datatype::GeometryWkb
                | Datatype::GeometryWkt
        )
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

    use super::*;
    use crate::array::{Array, Mode};
    use crate::datatype::physical::BitsOrd;
    use crate::error::Error;
    use crate::query::read::{AggregateQueryBuilder, ReadBuilder};
    use crate::query::strategy::{Cells, FieldData};
    use crate::query::QueryLayout;
    use crate::query::{QueryBuilder, ReadQuery};
    use crate::tests::examples::TestArray;
    use crate::{
        physical_type_go, typed_field_data_go, Context, Result as TileDBResult,
    };

    /// Test that all aggregate functions produced by
    /// the `Arbitrary` implementation do not result in errors in queries.
    #[test]
    fn strategy_validity() {
        // schema with all datatypes used in attributes and dimensions
        let schema = Rc::new(crate::tests::examples::sparse_all::schema(
            Default::default(),
        ));
        let mut array = TestArray::new(
            "is_unsupported_min_max_datatype",
            Rc::clone(&schema),
        )
        .unwrap();

        let mut runner = TestRunner::new(Default::default());

        // generate test data
        let input = array.arbitrary_input(&mut runner);

        // insert to the array
        array.try_insert(&input).unwrap();

        let strat_agg = any_with::<AggregateFunction>(Some(
            AggregateFunctionContext::Schema(Rc::clone(&schema)),
        ))
        .no_shrink();

        runner
            .run(&strat_agg, |agg| {
                do_validate_agg(&array.context, &array.uri, input.cells(), agg);
                Ok(())
            })
            .unwrap_or_else(|e| panic!("{}\nWrite input = {:?}", e, input));
    }

    /// Test that an aggregate function produced by the `Arbitrary`
    /// implementation is valid within the schema that parameterized it
    /// (and returns correct results)
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

    /// Test that anything filtered out by `is_unsupported_null_count_field` actually does
    /// get an error when the null count aggregate is run on it.
    #[test]
    fn is_unsupported_null_count_field() {
        // schema with all datatypes used in attributes and dimensions
        let schema = Rc::new(crate::tests::examples::sparse_all::schema(
            Default::default(),
        ));
        let mut array = TestArray::new(
            "is_unsupported_min_max_datatype",
            Rc::clone(&schema),
        )
        .unwrap();

        let mut runner = TestRunner::new(Default::default());

        // generate test data
        let input = {
            let mut input = array.arbitrary_input(&mut runner);
            input.cells_mut().truncate(0);
            input
        };

        // insert to the array
        array.try_insert(&input).unwrap();

        for field in schema.fields() {
            if input.cells().is_empty() && field.nullability().unwrap_or(false)
            {
                let q = rstart(&array.context, &array.uri)
                    .unwrap()
                    .null_count(field.name())
                    .map(|b| b.build());
                let r = q.and_then(|mut q| q.execute());
                assert!(matches!(r, Ok((Some(0), ()))),
                "For field {}: Expected Ok(Some(0)) but found {:?} on empty array",
                field.name(), r);
            } else if super::is_unsupported_null_count_field(&field) {
                let q = rstart(&array.context, &array.uri)
                    .unwrap()
                    .null_count(field.name())
                    .map(|b| b.build());

                let r = q.and_then(|mut q| q.execute());
                assert!(
                    matches!(r, Err(Error::LibTileDB(_))),
                    "For field {}: Expected Err but found {:?} for input {:?}",
                    field.name(),
                    r,
                    input
                );
            } else {
                do_validate_agg_null_count(
                    &array.context,
                    &array.uri,
                    field.name(),
                    input.cells(),
                )
            }
        }
    }

    /// Test that anything filtered out by `is_unsupported_min_max_datatype` actually does
    /// get an error when min or max aggregate is run on it.
    #[test]
    fn is_unsupported_min_max_datatype() {
        // schema with all datatypes used in attributes and dimensions
        let schema = Rc::new(crate::tests::examples::sparse_all::schema(
            Default::default(),
        ));
        let mut array = TestArray::new(
            "is_unsupported_min_max_datatype",
            Rc::clone(&schema),
        )
        .unwrap();

        let mut runner = TestRunner::new(Default::default());

        // generate test data
        let input = array.arbitrary_input(&mut runner);

        // insert to the array
        array.try_insert(&input).unwrap();

        for field in schema.fields() {
            if field
                .cell_val_num()
                .unwrap_or(CellValNum::single())
                .is_var_sized()
            {
                // not supported yet, result sizing prevents constructing query
                // core *does* support this in some way,
                // we're probably best off treating this as a follow-up story.
                // See test-cppapi-aggregates.cc
                continue;
            }
            if super::is_unsupported_min_max_datatype(field.datatype()) {
                physical_type_go!(field.datatype(), DT, {
                    let q = rstart(&array.context, &array.uri)
                        .unwrap()
                        .min::<DT>(field.name())
                        .map(|b| b.build());

                    let r = q.and_then(|mut q| q.execute());
                    assert!(
                        matches!(r, Err(Error::LibTileDB(_))),
                        "For field {}: Expected Err but found {:?}",
                        field.name(),
                        r
                    );
                });
            } else {
                do_validate_agg_min_max(
                    &array.context,
                    &array.uri,
                    field.name(),
                    false,
                    input.cells(),
                )
            }
        }
    }
}
