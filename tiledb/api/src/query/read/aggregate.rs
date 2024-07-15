use super::*;

use anyhow::anyhow;
use ffi::{
    tiledb_channel_operation_t, tiledb_channel_operator_t,
    tiledb_query_channel_t,
};
use std::mem;
use std::ffi::CString;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::marker::PhantomData;

use crate::array::Schema;
use crate::datatype::PhysicalType;
use crate::error::Error as TileDBError;
use crate::{Datatype, Result as TileDBResult};

#[derive(Debug, PartialEq)]
pub struct AggregateTypedBuilder<B, T> {
    base: B,
    agg_str: CString,
    attr_str: Option<CString>,
    attr_type: PhantomData<T>
}

#[derive(Debug, PartialEq)]
pub struct AggregateTypedReader<Q, T> {
    base: Q,
    agg_str: CString,
    _attr_str: Option<CString>, // Unused because the C API uses this memory to store the attribute name.
    data: T,
    data_size: u64,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum AggregateResultHandle {
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
}

macro_rules! agg_result_handle_go {
    ($expr:expr, $DT:ident, $inner:pat, $then:expr) => {
        match $expr {
            AggregateResultHandle::UInt8($inner) => {
                type $DT = u8;
                $then
            }
            AggregateResultHandle::UInt16($inner) => {
                type $DT = u16;
                $then
            }
            AggregateResultHandle::UInt32($inner) => {
                type $DT = u32;
                $then
            }
            AggregateResultHandle::UInt64($inner) => {
                type $DT = u64;
                $then
            }
            AggregateResultHandle::Int8($inner) => {
                type $DT = i8;
                $then
            }
            AggregateResultHandle::Int16($inner) => {
                type $DT = i16;
                $then
            }
            AggregateResultHandle::Int32($inner) => {
                type $DT = i32;
                $then
            }
            AggregateResultHandle::Int64($inner) => {
                type $DT = i64;
                $then
            }
            AggregateResultHandle::Float32($inner) => {
                type $DT = f32;
                $then
            }
            AggregateResultHandle::Float64($inner) => {
                type $DT = f64;
                $then
            }
        }
    };
}

macro_rules! agg_handle_from_type_impl {
    ($ty:ty, $constructor:expr) => {
        impl From<$ty> for AggregateResultHandle {
            fn from(val : $ty) -> Self {
                $constructor(val)
            }
        }
    };
}

agg_handle_from_type_impl!(i8, AggregateResultHandle::Int8);
agg_handle_from_type_impl!(i16, AggregateResultHandle::Int16);
agg_handle_from_type_impl!(i32, AggregateResultHandle::Int32);
agg_handle_from_type_impl!(i64, AggregateResultHandle::Int64);
agg_handle_from_type_impl!(u8, AggregateResultHandle::UInt8);
agg_handle_from_type_impl!(u16, AggregateResultHandle::UInt16);
agg_handle_from_type_impl!(u32, AggregateResultHandle::UInt32);
agg_handle_from_type_impl!(u64, AggregateResultHandle::UInt64);
agg_handle_from_type_impl!(f32, AggregateResultHandle::Float32);
agg_handle_from_type_impl!(f64, AggregateResultHandle::Float64);

#[derive(Debug, PartialEq)]
pub enum AggregateEnumBuilder<B> {
    UInt8(AggregateTypedBuilder<B, u8>),
    UInt16(AggregateTypedBuilder<B, u16>),
    UInt32(AggregateTypedBuilder<B, u32>),
    UInt64(AggregateTypedBuilder<B, u64>),
    Int8(AggregateTypedBuilder<B, i8>),
    Int16(AggregateTypedBuilder<B, i16>),
    Int32(AggregateTypedBuilder<B, i32>),
    Int64(AggregateTypedBuilder<B, i64>),
    Float32(AggregateTypedBuilder<B, f32>),
    Float64(AggregateTypedBuilder<B, f64>)
}

macro_rules! agg_enum_builder_go {
    ($expr:expr, $DT:ident, $inner:pat, $then:expr) => {
        match $expr {
            AggregateEnumBuilder::UInt8($inner) => {
                type $DT = u8;
                $then
            }
            AggregateEnumBuilder::UInt16($inner) => {
                type $DT = u16;
                $then
            }
            AggregateEnumBuilder::UInt32($inner) => {
                type $DT = u32;
                $then
            }
            AggregateEnumBuilder::UInt64($inner) => {
                type $DT = u64;
                $then
            }
            AggregateEnumBuilder::Int8($inner) => {
                type $DT = i8;
                $then
            }
            AggregateEnumBuilder::Int16($inner) => {
                type $DT = i16;
                $then
            }
            AggregateEnumBuilder::Int32($inner) => {
                type $DT = i32;
                $then
            }
            AggregateEnumBuilder::Int64($inner) => {
                type $DT = i64;
                $then
            }
            AggregateEnumBuilder::Float32($inner) => {
                type $DT = f32;
                $then
            }
            AggregateEnumBuilder::Float64($inner) => {
                type $DT = f64;
                $then
            }
        }
    };
}

#[derive(Debug, PartialEq)]
pub enum AggregateEnumReader<Q> {
    UInt8(AggregateTypedReader<Q, u8>),
    UInt16(AggregateTypedReader<Q, u16>),
    UInt32(AggregateTypedReader<Q, u32>),
    UInt64(AggregateTypedReader<Q, u64>),
    Int8(AggregateTypedReader<Q, i8>),
    Int16(AggregateTypedReader<Q, i16>),
    Int32(AggregateTypedReader<Q, i32>),
    Int64(AggregateTypedReader<Q, i64>),
    Float32(AggregateTypedReader<Q, f32>),
    Float64(AggregateTypedReader<Q, f64>)
}

macro_rules! agg_enum_reader_go {
    ($expr:expr, $DT:ident, $inner:pat, $then:expr) => {
        match $expr {
            AggregateEnumReader::UInt8($inner) => {
                type $DT = u8;
                $then
            }
            AggregateEnumReader::UInt16($inner) => {
                type $DT = u16;
                $then
            }
            AggregateEnumReader::UInt32($inner) => {
                type $DT = u32;
                $then
            }
            AggregateEnumReader::UInt64($inner) => {
                type $DT = u64;
                $then
            }
            AggregateEnumReader::Int8($inner) => {
                type $DT = i8;
                $then
            }
            AggregateEnumReader::Int16($inner) => {
                type $DT = i16;
                $then
            }
            AggregateEnumReader::Int32($inner) => {
                type $DT = i32;
                $then
            }
            AggregateEnumReader::Int64($inner) => {
                type $DT = i64;
                $then
            }
            AggregateEnumReader::Float32($inner) => {
                type $DT = f32;
                $then
            }
            AggregateEnumReader::Float64($inner) => {
                type $DT = f64;
                $then
            }
        }
    };
}


impl<B, T> QueryBuilder for AggregateTypedBuilder<B, T>
where
    B: QueryBuilder,
    T: PhysicalType,
{
    type Query = AggregateTypedReader<B::Query, T>;

    fn base(&self) -> &BuilderBase {
        self.base.base()
    }

    fn build(self) -> Self::Query {
        AggregateTypedReader::<B::Query, T> {
            base: self.base.build(),
            agg_str: self.agg_str,
            _attr_str: self.attr_str,
            data: T::default(),
            data_size: mem::size_of::<T>() as u64
        }
    }
}

impl <B> QueryBuilder for AggregateEnumBuilder<B>
where B: QueryBuilder {
    type Query = AggregateEnumReader<B::Query>;

    fn base(&self) -> &BuilderBase {
        agg_enum_builder_go!(self, _DT, builder, builder.base.base())
    }

    fn build(self) -> Self::Query {
        match self {
            AggregateEnumBuilder::UInt8(agg_builder) => {
                AggregateEnumReader::UInt8(agg_builder.build())
            }
            AggregateEnumBuilder::UInt16(agg_builder) => {
                AggregateEnumReader::UInt16(agg_builder.build())
            }
            AggregateEnumBuilder::UInt32(agg_builder) => {
                AggregateEnumReader::UInt32(agg_builder.build())
            }
            AggregateEnumBuilder::UInt64(agg_builder) => {
                AggregateEnumReader::UInt64(agg_builder.build())
            }
            AggregateEnumBuilder::Int8(agg_builder) => {
                AggregateEnumReader::Int8(agg_builder.build())
            }
            AggregateEnumBuilder::Int16(agg_builder) => {
                AggregateEnumReader::Int16(agg_builder.build())
            }
            AggregateEnumBuilder::Int32(agg_builder) => {
                AggregateEnumReader::Int32(agg_builder.build())
            }
            AggregateEnumBuilder::Int64(agg_builder) => {
                AggregateEnumReader::Int64(agg_builder.build())
            }
            AggregateEnumBuilder::Float32(agg_builder) => {
                AggregateEnumReader::Float32(agg_builder.build())
            }
            AggregateEnumBuilder::Float64(agg_builder) => {
                AggregateEnumReader::Float64(agg_builder.build())
            }
        }
    }
}

impl<Q, T> Query for AggregateTypedReader<Q, T>
where
    Q: Query,
{
    fn base(&self) -> &QueryBase {
        self.base.base()
    }

    fn finalize(self) -> TileDBResult<Array>
    where
        Self: Sized,
    {
        self.base.finalize()
    }
}

impl<Q> Query for AggregateEnumReader<Q>
where
    Q: Query,
{
    fn base(&self) -> &QueryBase {
        agg_enum_reader_go!(self, _DT, reader, reader.base.base())
    }

    fn finalize(self) -> TileDBResult<Array>
    where
        Self: Sized,
    {
        agg_enum_reader_go!(self, _DT, reader, reader.base.finalize())
    }
}

impl<Q, T> ReadQuery for AggregateTypedReader<Q, T>
where
    Q: ReadQuery,
    T: Copy,
{
    type Intermediate = ();
    type Final = (T, Q::Final);

    fn step(
        &mut self,
    ) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>> {
        // Register the data buffer (set data buffer)
        let context = self.base().context();
        let cquery = **self.base().cquery();
        let location_ptr = &mut self.data as *mut T;
        let c_bufptr = location_ptr as *mut std::ffi::c_void;
        let c_sizeptr = &mut self.data_size as *mut u64;
        let agg_str: &CString = &self.agg_str;
        let agg_c_ptr = agg_str.as_c_str().as_ptr();

        context.capi_call(|ctx| unsafe {
            ffi::tiledb_query_set_data_buffer(
                ctx, cquery, agg_c_ptr, c_bufptr, c_sizeptr,
            )
        })?;

        let base_result = self.base.step()?;

        // There are no intermediate results for aggregates since the buffer size should be one
        // element (and therefore, no space constraints).
        let (return_val, base_q) = match base_result {
            ReadStepOutput::Final(base_q) => (self.data, base_q),
            ReadStepOutput::Intermediate(_) => {
                unreachable!("Aggregate step function.")
            }
            ReadStepOutput::NotEnoughSpace => {
                unreachable!("Aggregate step function.")
            }
        };

        Ok(ReadStepOutput::Final((return_val, base_q)))
    }
}

impl<Q> ReadQuery for AggregateEnumReader<Q>
where
    Q: ReadQuery,
{
    type Intermediate = ();
    type Final = (AggregateResultHandle, Q::Final);

    fn step(&mut self) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>> {
        agg_enum_reader_go!(self, _DT, reader, {
            let step_result = reader.step()?;
            let enum_result = match step_result {
                ReadStepOutput::Final((return_val, base_q)) => ReadStepOutput::Final((AggregateResultHandle::from(return_val), base_q)),
                _ => unreachable!("Aggregate enum step function")
            };
            Ok(enum_result)
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AggregateType {
    Count,
    Max(String),
    Mean(String),
    Min(String),
    NullCount(String),
    Sum(String),
}

impl Display for AggregateType {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        <Self as Debug>::fmt(self, f)
    }
}

fn get_datatype_from_attr(schema: &Schema, attr_name: &String) -> TileDBResult<Datatype> {
    let attr = schema.field(attr_name)?;
    let attr_type = attr.datatype()?;
    Ok(attr_type)
}

fn aggregate_type(
    agg_type: &AggregateType,
    schema: &Schema
) -> TileDBResult<Datatype> {
    match agg_type {
        AggregateType::Count | AggregateType::NullCount(_) => Ok(Datatype::UInt64),
        AggregateType::Mean(_) => Ok(Datatype::Float64),
        AggregateType::Sum(attr_name) => {
            let attr_type = get_datatype_from_attr(schema, &attr_name)?;
            if attr_type == Datatype::Int8
                || attr_type == Datatype::Int16
                || attr_type == Datatype::Int32
                || attr_type == Datatype::Int64
            {
                Ok(Datatype::Int64)
            }
            else if attr_type == Datatype::UInt8
                || attr_type == Datatype::UInt16
                || attr_type == Datatype::UInt32
                || attr_type == Datatype::UInt64
            {
                Ok(Datatype::UInt64)
            }
            else if attr_type == Datatype::Float32 || attr_type == Datatype::Float64
            {
                Ok(Datatype::Float64)
            }
            else {
                Err(TileDBError::InvalidArgument(anyhow!("Invalid attribute type.")))
            }
        }
        AggregateType::Min(attr_name) | AggregateType::Max(attr_name) => {
            let attr_type = get_datatype_from_attr(schema, &attr_name)?;
            Ok(attr_type)
        }
    }
}

/// Trait for query types which can have an aggregate channel placed on top of them.
pub trait AggregateBuilderTrait: QueryBuilder {
    fn apply_typed_aggregate<T: 'static>(
        self,
        agg_type: AggregateType
    ) -> TileDBResult<AggregateTypedBuilder<Self, T>> {
        let expected_type = aggregate_type(&agg_type, &self.base().array().schema()?)?;
        if !expected_type.is_compatible_type::<T>() {
            return Err(TileDBError::InvalidArgument(anyhow!(
                expected_type.to_string()
                    + " result type is not equivalent to the passed in type."
            )));
        }

        let (agg_name, attr_name) = match agg_type {
            AggregateType::Count => (cstring!("Count"), None),
            AggregateType::NullCount(ref name) => {
                (
                    cstring!("NullCount"),
                    Some(cstring!(name.as_str())),
                )
            }
            AggregateType::Sum(ref name) => {
                (cstring!("Sum"), Some(cstring!(name.as_str())))
            }
            AggregateType::Max(ref name) => {
                (cstring!("Max"), Some(cstring!(name.as_str())))
            }
            AggregateType::Min(ref name) => {
                (cstring!("Min"), Some(cstring!(name.as_str())))
            }
            AggregateType::Mean(ref name) => {
                (cstring!("Mean"), Some(cstring!(name.as_str())))
            }
        };

        // Put aggregate C API functions here (channel initialization and setup)
        // So far only count
        let context = self.base().context();
        let cquery = **self.base().cquery();
        let mut default_channel: *mut tiledb_query_channel_t = out_ptr!();
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_query_get_default_channel(
                ctx,
                cquery,
                &mut default_channel,
            )
        })?;

        // C API functionality
        let mut agg_operator: *const tiledb_channel_operator_t = out_ptr!();
        let mut agg_operation: *mut tiledb_channel_operation_t = out_ptr!();
        let c_agg_name = agg_name.as_c_str().as_ptr();

        if agg_type == AggregateType::Count {
            context.capi_call(|ctx| unsafe {
                ffi::tiledb_aggregate_count_get(
                    ctx,
                    core::ptr::addr_of_mut!(agg_operation)
                        as *mut *const tiledb_channel_operation_t,
                )
            })?;
        } else {
            let c_attr_name: *const i8 =
                attr_name.as_ref().unwrap().as_c_str().as_ptr();
            match agg_type {
                AggregateType::NullCount(_) => {
                    context.capi_call(|ctx| unsafe {
                        ffi::tiledb_channel_operator_null_count_get(
                            ctx,
                            &mut agg_operator,
                        )
                    })?;
                }
                AggregateType::Sum(_) => {
                    context.capi_call(|ctx| unsafe {
                        ffi::tiledb_channel_operator_sum_get(
                            ctx,
                            &mut agg_operator,
                        )
                    })?;
                }
                AggregateType::Max(_) => {
                    context.capi_call(|ctx| unsafe {
                        ffi::tiledb_channel_operator_max_get(
                            ctx,
                            &mut agg_operator,
                        )
                    })?;
                }
                AggregateType::Min(_) => {
                    context.capi_call(|ctx| unsafe {
                        ffi::tiledb_channel_operator_min_get(
                            ctx,
                            &mut agg_operator,
                        )
                    })?;
                }
                AggregateType::Mean(_) => {
                    context.capi_call(|ctx| unsafe {
                        ffi::tiledb_channel_operator_mean_get(
                            ctx,
                            &mut agg_operator,
                        )
                    })?;
                }
                AggregateType::Count => unreachable!(),
            };
            context.capi_call(|ctx| unsafe {
                ffi::tiledb_create_unary_aggregate(
                    ctx,
                    cquery,
                    agg_operator,
                    c_attr_name,
                    &mut agg_operation,
                )
            })?;
        }

        // Apply aggregate to the default channel.
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_channel_apply_aggregate(
                ctx,
                default_channel,
                c_agg_name,
                agg_operation,
            )
        })?;

        Ok(AggregateTypedBuilder::<Self, T> {
            base: self,
            agg_str: agg_name,
            attr_str: attr_name,
            attr_type: PhantomData
        })
    }
}

pub trait AggregateEnumBuilderTrait : QueryBuilder {
    fn apply_enum_aggregate(
        self,
        agg_type: AggregateType
    ) -> TileDBResult<AggregateEnumBuilder<Self>> 
    where Self : AggregateBuilderTrait
    {
        let schema = self.base().array().schema()?;
        let aggregate_result_type = aggregate_type(&agg_type, &schema)?;
        let agg_builder = match aggregate_result_type {
            Datatype::UInt8 => AggregateEnumBuilder::UInt8(self.apply_typed_aggregate::<u8>(agg_type)?),
            Datatype::UInt16 => AggregateEnumBuilder::UInt16(self.apply_typed_aggregate::<u16>(agg_type)?),
            Datatype::UInt32 => AggregateEnumBuilder::UInt32(self.apply_typed_aggregate::<u32>(agg_type)?),
            Datatype::UInt64 => AggregateEnumBuilder::UInt64(self.apply_typed_aggregate::<u64>(agg_type)?),
            Datatype::Int8 => AggregateEnumBuilder::Int8(self.apply_typed_aggregate::<i8>(agg_type)?),
            Datatype::Int16 => AggregateEnumBuilder::Int16(self.apply_typed_aggregate::<i16>(agg_type)?),
            Datatype::Int32 => AggregateEnumBuilder::Int32(self.apply_typed_aggregate::<i32>(agg_type)?),
            Datatype::Int64 => AggregateEnumBuilder::Int64(self.apply_typed_aggregate::<i64>(agg_type)?),
            Datatype::Float32 => AggregateEnumBuilder::Float32(self.apply_typed_aggregate::<f32>(agg_type)?),
            Datatype::Float64 => AggregateEnumBuilder::Float64(self.apply_typed_aggregate::<f64>(agg_type)?),
            _ => unreachable!("aggregate_type function should return a numeric type.")
        };
        Ok(agg_builder)
    }
}

impl AggregateBuilderTrait for ReadBuilder {}
impl<B: QueryBuilder, T> AggregateBuilderTrait for AggregateTypedBuilder<B, T> where
    T: PhysicalType
{
}
impl <B : QueryBuilder> AggregateBuilderTrait for AggregateEnumBuilder<B> {}

impl AggregateEnumBuilderTrait for ReadBuilder {}
impl <B: QueryBuilder, T> AggregateEnumBuilderTrait for AggregateTypedBuilder<B, T> where
T: PhysicalType
{
}
impl <B : QueryBuilder> AggregateEnumBuilderTrait for AggregateEnumBuilder<B> {}