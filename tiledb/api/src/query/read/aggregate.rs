use super::*;

use anyhow::anyhow;
use ffi::{
    tiledb_channel_operation_t, tiledb_channel_operator_t,
    tiledb_query_channel_t,
};
use std::any::type_name;
use std::ffi::CString;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::marker::PhantomData;
use std::mem;

use crate::array::Schema;
use crate::datatype::PhysicalType;
use crate::error::Error as TileDBError;
use crate::{Datatype, Result as TileDBResult};

#[derive(Debug, PartialEq)]
pub struct AggregateBuilder<T, B> {
    base: B,
    agg_str: CString,
    attr_str: Option<CString>,
    attr_type: PhantomData<T>,
}

#[derive(Debug, PartialEq)]
pub struct AggregateQuery<T, Q> {
    base: Q,
    agg_str: CString,
    _attr_str: Option<CString>, // Unused because the C API uses this memory to store the attribute name.
    data: T,
    data_size: u64,
}

impl<T, B> QueryBuilder for AggregateBuilder<T, B>
where
    B: QueryBuilder,
    T: PhysicalType,
{
    type Query = AggregateQuery<T, B::Query>;

    fn base(&self) -> &BuilderBase {
        self.base.base()
    }

    fn build(self) -> Self::Query {
        AggregateQuery::<T, B::Query> {
            base: self.base.build(),
            agg_str: self.agg_str,
            _attr_str: self.attr_str,
            data: T::default(),
            data_size: mem::size_of::<T>() as u64,
        }
    }
}

impl<T, Q> Query for AggregateQuery<T, Q>
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

impl<T, Q> ReadQuery for AggregateQuery<T, Q>
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
        let location_ptr = &mut self.data as *mut T;

        let c_query = **self.base().cquery();
        let c_bufptr = location_ptr as *mut std::ffi::c_void;
        let c_sizeptr = &mut self.data_size as *mut u64;
        let agg_str: &CString = &self.agg_str;
        let agg_c_ptr = agg_str.as_c_str().as_ptr();

        context.capi_call(|ctx| unsafe {
            ffi::tiledb_query_set_data_buffer(
                ctx, c_query, agg_c_ptr, c_bufptr, c_sizeptr,
            )
        })?;

        let base_result = self.base.step()?;

        // There are no intermediate results for aggregates since the buffer size should be one
        // element (and therefore, no space constraints).
        let (return_val, base_q) = match base_result {
            ReadStepOutput::Final(base_q) => (self.data, base_q),
            ReadStepOutput::Intermediate(_) => {
                unreachable!("Expected ReadStepOutput::Final.")
            }
            ReadStepOutput::NotEnoughSpace => {
                unreachable!("Expected ReadStepOutput::Final.")
            }
        };

        Ok(ReadStepOutput::Final((return_val, base_q)))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AggregateFunction {
    Count,
    Max(String),
    Mean(String),
    Min(String),
    NullCount(String),
    Sum(String),
}

impl AggregateFunction {
    pub fn result_type(&self, schema: &Schema) -> TileDBResult<Datatype> {
        match self {
            AggregateFunction::Count | AggregateFunction::NullCount(_) => {
                Ok(Datatype::UInt64)
            }
            AggregateFunction::Mean(_) => Ok(Datatype::Float64),
            AggregateFunction::Sum(attr_name) => {
                let attr_type = get_datatype_from_attr(schema, attr_name)?;
                if matches!(
                    attr_type,
                    Datatype::Int8
                        | Datatype::Int16
                        | Datatype::Int32
                        | Datatype::Int64
                ) {
                    Ok(Datatype::Int64)
                } else if matches!(
                    attr_type,
                    Datatype::UInt8
                        | Datatype::UInt16
                        | Datatype::UInt32
                        | Datatype::UInt64
                ) {
                    Ok(Datatype::UInt64)
                } else if matches!(
                    attr_type,
                    Datatype::Float32 | Datatype::Float64
                ) {
                    Ok(Datatype::Float64)
                } else {
                    Err(TileDBError::InvalidArgument(anyhow!(format!(
                    "aggregate_type: field has invalid non-numeric datatype {}",
                    attr_type
                ))))
                }
            }
            AggregateFunction::Min(attr_name)
            | AggregateFunction::Max(attr_name) => {
                let attr_type = get_datatype_from_attr(schema, attr_name)?;
                Ok(attr_type)
            }
        }
    }
}

impl Display for AggregateFunction {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        <Self as Debug>::fmt(self, f)
    }
}

fn get_datatype_from_attr(
    schema: &Schema,
    attr_name: &String,
) -> TileDBResult<Datatype> {
    let attr = schema.field(attr_name)?;
    let attr_type = attr.datatype()?;
    Ok(attr_type)
}

/// Trait for query types which can have an aggregate channel placed on top of them.
pub trait AggregateQueryBuilder: QueryBuilder {
    fn apply_aggregate<T>(
        self,
        agg_function: AggregateFunction,
    ) -> TileDBResult<AggregateBuilder<T, Self>>
    where
        T: PhysicalType,
    {
        let expected_type =
            agg_function.result_type(&self.base().array().schema()?)?;
        if !expected_type.is_compatible_type::<T>() {
            return Err(TileDBError::Datatype(
                crate::error::DatatypeErrorKind::TypeMismatch {
                    user_type: String::from(type_name::<T>()),
                    tiledb_type: expected_type,
                },
            ));
        }

        let (agg_name, attr_name) = match agg_function {
            AggregateFunction::Count => (cstring!("Count"), None),
            AggregateFunction::NullCount(ref name) => {
                (cstring!("NullCount"), Some(cstring!(name.as_str())))
            }
            AggregateFunction::Sum(ref name) => {
                (cstring!("Sum"), Some(cstring!(name.as_str())))
            }
            AggregateFunction::Max(ref name) => {
                (cstring!("Max"), Some(cstring!(name.as_str())))
            }
            AggregateFunction::Min(ref name) => {
                (cstring!("Min"), Some(cstring!(name.as_str())))
            }
            AggregateFunction::Mean(ref name) => {
                (cstring!("Mean"), Some(cstring!(name.as_str())))
            }
        };

        let context = self.base().context();
        let c_query = **self.base().cquery();

        let mut c_channel: *mut tiledb_query_channel_t = out_ptr!();
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_query_get_default_channel(ctx, c_query, &mut c_channel)
        })?;

        // C API functionality
        let mut c_agg_operator: *const tiledb_channel_operator_t = out_ptr!();
        let mut c_agg_operation: *mut tiledb_channel_operation_t = out_ptr!();
        let c_agg_name = agg_name.as_c_str().as_ptr();

        // The if statement and match statement are in different arms because of the agg_operation
        // variable takes in different types in the respective functions.
        if agg_function == AggregateFunction::Count {
            context.capi_call(|ctx| unsafe {
                ffi::tiledb_aggregate_count_get(
                    ctx,
                    core::ptr::addr_of_mut!(c_agg_operation)
                        as *mut *const tiledb_channel_operation_t,
                )
            })?;
        } else {
            let c_attr_name: *const i8 =
                attr_name.as_ref().unwrap().as_c_str().as_ptr();
            match agg_function {
                AggregateFunction::Count => unreachable!(
                    "AggregateFunction::Count handled in above case, found {:?}",
                    agg_function
                ),
                AggregateFunction::NullCount(_) => {
                    context.capi_call(|ctx| unsafe {
                        ffi::tiledb_channel_operator_null_count_get(
                            ctx,
                            &mut c_agg_operator,
                        )
                    })?;
                }
                AggregateFunction::Sum(_) => {
                    context.capi_call(|ctx| unsafe {
                        ffi::tiledb_channel_operator_sum_get(
                            ctx,
                            &mut c_agg_operator,
                        )
                    })?;
                }
                AggregateFunction::Max(_) => {
                    context.capi_call(|ctx| unsafe {
                        ffi::tiledb_channel_operator_max_get(
                            ctx,
                            &mut c_agg_operator,
                        )
                    })?;
                }
                AggregateFunction::Min(_) => {
                    context.capi_call(|ctx| unsafe {
                        ffi::tiledb_channel_operator_min_get(
                            ctx,
                            &mut c_agg_operator,
                        )
                    })?;
                }
                AggregateFunction::Mean(_) => {
                    context.capi_call(|ctx| unsafe {
                        ffi::tiledb_channel_operator_mean_get(
                            ctx,
                            &mut c_agg_operator,
                        )
                    })?;
                }
            };
            context.capi_call(|ctx| unsafe {
                ffi::tiledb_create_unary_aggregate(
                    ctx,
                    c_query,
                    c_agg_operator,
                    c_attr_name,
                    &mut c_agg_operation,
                )
            })?;
        }

        context.capi_call(|ctx| unsafe {
            ffi::tiledb_channel_apply_aggregate(
                ctx,
                c_channel,
                c_agg_name,
                c_agg_operation,
            )
        })?;

        Ok(AggregateBuilder::<T, Self> {
            base: self,
            agg_str: agg_name,
            attr_str: attr_name,
            attr_type: PhantomData,
        })
    }

    /// Function to get the count of elements in an array.
    fn count(self) -> TileDBResult<AggregateBuilder<u64, Self>> {
        self.apply_aggregate::<u64>(AggregateFunction::Count)
    }

    /// Function that gets the count of null values in the data corresponding to a
    /// certain attribute, specified by attr_name.
    fn null_count(
        self,
        attr_name: String,
    ) -> TileDBResult<AggregateBuilder<u64, Self>> {
        self.apply_aggregate::<u64>(AggregateFunction::NullCount(attr_name))
    }

    /// Function that gets the average of the data corresponding to a
    /// certain attribute, specified by attr_name.
    fn mean(
        self,
        attr_name: String,
    ) -> TileDBResult<AggregateBuilder<f64, Self>> {
        self.apply_aggregate::<f64>(AggregateFunction::Mean(attr_name))
    }

    /// Function that gets the sum of the data corresponding to a
    /// certain attribute, specified by attr_name. This function also takes in a
    /// type argument which should correspond to the type of the attribute:
    /// Attributes of types i8, i16, i32, i64 => i64 sum type
    /// Attributes of types u8, u16, u32, u64 => u64 sum type
    /// Attributes of types f32, f64 => f64 sum type
    fn sum<T>(
        self,
        attr_name: String,
    ) -> TileDBResult<AggregateBuilder<T, Self>>
    where
        T: PhysicalType,
    {
        self.apply_aggregate::<T>(AggregateFunction::Sum(attr_name))
    }

    /// Function that gets the min of the data corresponding to a
    /// certain attribute, specified by attr_name. This function also takes in a
    /// type argument, which should be the type of the attribute.
    fn min<T>(
        self,
        attr_name: String,
    ) -> TileDBResult<AggregateBuilder<T, Self>>
    where
        T: PhysicalType,
    {
        self.apply_aggregate::<T>(AggregateFunction::Min(attr_name))
    }

    /// Function that gets the max of the data corresponding to a
    /// certain attribute, specified by attr_name. This function also takes in a
    /// type argument, which should be the type of the attribute.
    fn max<T>(
        self,
        attr_name: String,
    ) -> TileDBResult<AggregateBuilder<T, Self>>
    where
        T: PhysicalType,
    {
        self.apply_aggregate::<T>(AggregateFunction::Max(attr_name))
    }
}

impl AggregateQueryBuilder for ReadBuilder {}

impl<T, B: QueryBuilder> AggregateQueryBuilder for AggregateBuilder<T, B> where
    T: PhysicalType
{
}
