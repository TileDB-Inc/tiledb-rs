use std::rc::Rc;

use arrow_schema::{self, DataType, Field, IntervalUnit, Schema, TimeUnit};
use proptest::prelude::*;
use strategy_ext::StrategyExt;

#[derive(Clone, Debug)]
pub struct SchemaParameters {
    pub num_fields: proptest::collection::SizeRange,
    pub field_names: BoxedStrategy<String>,
    pub field_type: BoxedStrategy<DataType>,
}

impl Default for SchemaParameters {
    fn default() -> Self {
        SchemaParameters {
            num_fields: (1..32).into(),
            field_names: proptest::string::string_regex("[a-zA-Z0-9_]*")
                .unwrap()
                .prop_without_replacement()
                .boxed(),
            field_type: prop_arrow_datatype(Default::default()).boxed(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct DataTypeParameters {
    pub fixed_binary_len: BoxedStrategy<i32>,
}

impl Default for DataTypeParameters {
    fn default() -> Self {
        DataTypeParameters {
            fixed_binary_len: (1..(128 * 1024)).boxed(),
        }
    }
}

pub fn prop_arrow_schema(
    params: SchemaParameters,
) -> impl Strategy<Value = Schema> {
    let strat_num_fields = params.num_fields.clone();
    proptest::collection::vec(
        prop_arrow_field(Rc::new(params)),
        strat_num_fields,
    )
    .prop_map(Schema::new)
}

pub fn prop_arrow_field(
    params: Rc<SchemaParameters>,
) -> impl Strategy<Value = Field> {
    let strat_name = params.field_names.clone();

    (strat_name, params.field_type.clone(), any::<bool>()).prop_map(
        |(name, datatype, nullable)| Field::new(name, datatype, nullable),
    )
}

pub fn prop_arrow_datatype(
    params: Rc<DataTypeParameters>,
) -> impl Strategy<Value = DataType> {
    let leaf = prop_oneof![
        Just(DataType::Null),
        Just(DataType::Int8),
        Just(DataType::Int16),
        Just(DataType::Int32),
        Just(DataType::Int64),
        Just(DataType::UInt8),
        Just(DataType::UInt16),
        Just(DataType::UInt32),
        Just(DataType::UInt64),
        Just(DataType::Float16),
        Just(DataType::Float32),
        Just(DataType::Float64),
        Just(DataType::Timestamp(TimeUnit::Second, None)),
        Just(DataType::Timestamp(TimeUnit::Millisecond, None)),
        Just(DataType::Timestamp(TimeUnit::Microsecond, None)),
        Just(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        Just(DataType::Date32),
        Just(DataType::Date64),
        Just(DataType::Time32(TimeUnit::Second)),
        Just(DataType::Time32(TimeUnit::Millisecond)),
        Just(DataType::Time64(TimeUnit::Microsecond)),
        Just(DataType::Time64(TimeUnit::Nanosecond)),
        Just(DataType::Duration(TimeUnit::Second)),
        Just(DataType::Duration(TimeUnit::Millisecond)),
        Just(DataType::Duration(TimeUnit::Nanosecond)),
        Just(DataType::Interval(IntervalUnit::YearMonth)),
        Just(DataType::Interval(IntervalUnit::DayTime)),
        Just(DataType::Interval(IntervalUnit::MonthDayNano)),
        Just(DataType::Binary),
        params
            .fixed_binary_len
            .clone()
            .prop_map(DataType::FixedSizeBinary),
        Just(DataType::LargeBinary),
        Just(DataType::Utf8),
        Just(DataType::LargeUtf8),
        prop_arrow_type_decimal128(),
        prop_arrow_type_decimal256(),
    ];

    // TODO: use `prop_recursive` for other datatypes
    leaf
}

/// Returns a strategy which produces `DataType`s for which
/// `DataType::is_numeric()` returns `true`.
pub fn prop_arrow_datatype_numeric() -> impl Strategy<Value = DataType> {
    prop_oneof![
        Just(DataType::Int8),
        Just(DataType::Int16),
        Just(DataType::Int32),
        Just(DataType::Int64),
        Just(DataType::UInt8),
        Just(DataType::UInt16),
        Just(DataType::UInt32),
        Just(DataType::UInt64),
        Just(DataType::Float16),
        Just(DataType::Float32),
        Just(DataType::Float64),
        prop_arrow_type_decimal128(),
        prop_arrow_type_decimal256(),
    ]
}

/// Returns a strategy which produces `DataType`s for which
/// `DataType::is_integer()` returns `true`.
pub fn prop_arrow_datatype_integer() -> impl Strategy<Value = DataType> {
    prop_oneof![
        Just(DataType::Int8),
        Just(DataType::Int16),
        Just(DataType::Int32),
        Just(DataType::Int64),
        Just(DataType::UInt8),
        Just(DataType::UInt16),
        Just(DataType::UInt32),
        Just(DataType::UInt64),
    ]
}

fn prop_arrow_type_decimal128() -> impl Strategy<Value = DataType> {
    (1..=arrow_schema::DECIMAL128_MAX_PRECISION).prop_flat_map(|precision| {
        (
            Just(precision),
            (0..precision.clamp(0, arrow_schema::DECIMAL128_MAX_SCALE as u8)),
        )
            .prop_map(|(precision, scale)| {
                DataType::Decimal128(precision, scale as i8)
            })
    })
}

fn prop_arrow_type_decimal256() -> impl Strategy<Value = DataType> {
    (1..=arrow_schema::DECIMAL256_MAX_PRECISION).prop_flat_map(|precision| {
        (
            Just(precision),
            (0..precision.clamp(0, arrow_schema::DECIMAL256_MAX_SCALE as u8)),
        )
            .prop_map(|(precision, scale)| {
                DataType::Decimal256(precision, scale as i8)
            })
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn do_unique_names(schema: Schema) {
        let field_names =
            schema.fields.iter().map(|f| f.name()).collect::<Vec<_>>();
        let unique_names = field_names.iter().collect::<HashSet<_>>();
        assert_eq!(
            field_names.len(),
            unique_names.len(),
            "field_names = {:?}",
            field_names
        );
    }

    proptest! {
        #[test]
        fn unique_names(schema in prop_arrow_schema(Default::default())) {
            do_unique_names(schema);
        }

        #[test]
        fn is_numeric(datatype in prop_arrow_datatype_numeric()) {
            assert!(datatype.is_numeric());
        }

        #[test]
        fn is_integer(datatype in prop_arrow_datatype_integer()) {
            assert!(datatype.is_integer());
        }
    }
}
