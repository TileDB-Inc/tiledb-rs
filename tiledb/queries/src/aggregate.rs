use tiledb::datatype::PhysicalValue;
use tiledb::physical_type_go;
use tiledb::query::read::aggregate::*;
use tiledb::query::read::ReadStepOutput;
use tiledb::query::{BuilderBase, Query, QueryBase, QueryBuilder, ReadQuery};
use tiledb::{Array, Result as TileDBResult};

/// An `AggregateQueryBuilder` blanket implementation that provides extra adapters
/// and methods for running aggregate queries.
pub trait AggregateQueryBuilderExt: AggregateQueryBuilder {
    fn aggregate_physical_value(
        self,
        agg_function: AggregateFunction,
    ) -> TileDBResult<AggregatePhysicalValueBuilder<Self>> {
        let schema = self.base().array().schema()?;
        let aggregate_result_type = agg_function.result_type(&schema)?;
        let agg_builder = physical_type_go!(
            aggregate_result_type,
            DT,
            AggregatePhysicalValueBuilder::from(
                self.apply_aggregate::<DT>(agg_function)?
            )
        );
        Ok(agg_builder)
    }
}

impl<B> AggregateQueryBuilderExt for B where B: AggregateQueryBuilder {}

/// Wraps an `AggregateBuilder` to transform the result of the query
/// it will construct into a `PhysicalValue`.
#[derive(Debug)]
pub enum AggregatePhysicalValueBuilder<B> {
    UInt8(AggregateBuilder<u8, B>),
    UInt16(AggregateBuilder<u16, B>),
    UInt32(AggregateBuilder<u32, B>),
    UInt64(AggregateBuilder<u64, B>),
    Int8(AggregateBuilder<i8, B>),
    Int16(AggregateBuilder<i16, B>),
    Int32(AggregateBuilder<i32, B>),
    Int64(AggregateBuilder<i64, B>),
    Float32(AggregateBuilder<f32, B>),
    Float64(AggregateBuilder<f64, B>),
}

macro_rules! aggregate_physical_value_builder_go {
    ($expr:expr, $DT:ident, $inner:pat, $then:expr) => {
        match $expr {
            AggregatePhysicalValueBuilder::UInt8($inner) => {
                type $DT = u8;
                $then
            }
            AggregatePhysicalValueBuilder::UInt16($inner) => {
                type $DT = u16;
                $then
            }
            AggregatePhysicalValueBuilder::UInt32($inner) => {
                type $DT = u32;
                $then
            }
            AggregatePhysicalValueBuilder::UInt64($inner) => {
                type $DT = u64;
                $then
            }
            AggregatePhysicalValueBuilder::Int8($inner) => {
                type $DT = i8;
                $then
            }
            AggregatePhysicalValueBuilder::Int16($inner) => {
                type $DT = i16;
                $then
            }
            AggregatePhysicalValueBuilder::Int32($inner) => {
                type $DT = i32;
                $then
            }
            AggregatePhysicalValueBuilder::Int64($inner) => {
                type $DT = i64;
                $then
            }
            AggregatePhysicalValueBuilder::Float32($inner) => {
                type $DT = f32;
                $then
            }
            AggregatePhysicalValueBuilder::Float64($inner) => {
                type $DT = f64;
                $then
            }
        }
    };
}

impl<B> QueryBuilder for AggregatePhysicalValueBuilder<B>
where
    B: QueryBuilder,
{
    type Query = AggregatePhysicalValueQuery<B::Query>;

    fn base(&self) -> &BuilderBase {
        aggregate_physical_value_builder_go!(self, _DT, builder, builder.base())
    }

    fn build(self) -> Self::Query {
        aggregate_physical_value_builder_go!(
            self,
            _DT,
            builder,
            AggregatePhysicalValueQuery::from(builder.build())
        )
    }
}

impl<B> AggregateQueryBuilder for AggregatePhysicalValueBuilder<B> where
    B: QueryBuilder
{
}

/// Wraps an `AggregateQuery` to transform its result into a `PhysicalValue`.
#[derive(Debug)]
pub enum AggregatePhysicalValueQuery<Q> {
    UInt8(AggregateQuery<u8, Q>),
    UInt16(AggregateQuery<u16, Q>),
    UInt32(AggregateQuery<u32, Q>),
    UInt64(AggregateQuery<u64, Q>),
    Int8(AggregateQuery<i8, Q>),
    Int16(AggregateQuery<i16, Q>),
    Int32(AggregateQuery<i32, Q>),
    Int64(AggregateQuery<i64, Q>),
    Float32(AggregateQuery<f32, Q>),
    Float64(AggregateQuery<f64, Q>),
}

macro_rules! aggregate_physical_value_query_go {
    ($expr:expr, $DT:ident, $inner:pat, $then:expr) => {
        match $expr {
            AggregatePhysicalValueQuery::UInt8($inner) => {
                type $DT = u8;
                $then
            }
            AggregatePhysicalValueQuery::UInt16($inner) => {
                type $DT = u16;
                $then
            }
            AggregatePhysicalValueQuery::UInt32($inner) => {
                type $DT = u32;
                $then
            }
            AggregatePhysicalValueQuery::UInt64($inner) => {
                type $DT = u64;
                $then
            }
            AggregatePhysicalValueQuery::Int8($inner) => {
                type $DT = i8;
                $then
            }
            AggregatePhysicalValueQuery::Int16($inner) => {
                type $DT = i16;
                $then
            }
            AggregatePhysicalValueQuery::Int32($inner) => {
                type $DT = i32;
                $then
            }
            AggregatePhysicalValueQuery::Int64($inner) => {
                type $DT = i64;
                $then
            }
            AggregatePhysicalValueQuery::Float32($inner) => {
                type $DT = f32;
                $then
            }
            AggregatePhysicalValueQuery::Float64($inner) => {
                type $DT = f64;
                $then
            }
        }
    };
}

impl<Q> Query for AggregatePhysicalValueQuery<Q>
where
    Q: Query,
{
    fn base(&self) -> &QueryBase {
        aggregate_physical_value_query_go!(self, _DT, reader, reader.base())
    }

    fn finalize(self) -> TileDBResult<Array>
    where
        Self: Sized,
    {
        aggregate_physical_value_query_go!(self, _DT, reader, reader.finalize())
    }
}

impl<Q> ReadQuery for AggregatePhysicalValueQuery<Q>
where
    Q: ReadQuery,
{
    type Intermediate = ();
    type Final = (Option<PhysicalValue>, Q::Final);

    fn step(
        &mut self,
    ) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>> {
        aggregate_physical_value_query_go!(self, _DT, reader, {
            let step_result = reader.step()?;
            let enum_result = match step_result {
                ReadStepOutput::Final((return_val, base_q)) => {
                    ReadStepOutput::Final((
                        return_val.map(PhysicalValue::from),
                        base_q,
                    ))
                }
                _ => unreachable!("Expected ReadStepOutput::Final."),
            };
            Ok(enum_result)
        })
    }
}

macro_rules! aggregate_physical_value_traits {
    ($T:ty, $Variant:ident) => {
        impl<B> From<AggregateBuilder<$T, B>>
            for AggregatePhysicalValueBuilder<B>
        where
            B: AggregateQueryBuilder,
        {
            fn from(value: AggregateBuilder<$T, B>) -> Self {
                AggregatePhysicalValueBuilder::$Variant(value)
            }
        }

        impl<B> TryFrom<AggregatePhysicalValueBuilder<B>>
            for AggregateBuilder<$T, B>
        where
            B: AggregateQueryBuilder,
        {
            type Error = AggregatePhysicalValueBuilder<B>;

            fn try_from(
                value: AggregatePhysicalValueBuilder<B>,
            ) -> Result<Self, Self::Error> {
                if let AggregatePhysicalValueBuilder::$Variant(value) = value {
                    Ok(value)
                } else {
                    Err(value)
                }
            }
        }

        impl<Q> From<AggregateQuery<$T, Q>> for AggregatePhysicalValueQuery<Q>
        where
            Q: Query,
        {
            fn from(value: AggregateQuery<$T, Q>) -> Self {
                AggregatePhysicalValueQuery::$Variant(value)
            }
        }

        impl<Q> TryFrom<AggregatePhysicalValueQuery<Q>>
            for AggregateQuery<$T, Q>
        where
            Q: Query,
        {
            type Error = AggregatePhysicalValueQuery<Q>;

            fn try_from(
                value: AggregatePhysicalValueQuery<Q>,
            ) -> Result<Self, Self::Error> {
                if let AggregatePhysicalValueQuery::$Variant(value) = value {
                    Ok(value)
                } else {
                    Err(value)
                }
            }
        }
    };
}

aggregate_physical_value_traits!(u8, UInt8);
aggregate_physical_value_traits!(u16, UInt16);
aggregate_physical_value_traits!(u32, UInt32);
aggregate_physical_value_traits!(u64, UInt64);
aggregate_physical_value_traits!(i8, Int8);
aggregate_physical_value_traits!(i16, Int16);
aggregate_physical_value_traits!(i32, Int32);
aggregate_physical_value_traits!(i64, Int64);
aggregate_physical_value_traits!(f32, Float32);
aggregate_physical_value_traits!(f64, Float64);
