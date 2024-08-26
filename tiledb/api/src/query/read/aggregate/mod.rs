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

use crate::array::{CellValNum, Schema};
use crate::datatype::PhysicalType;
use crate::error::Error as TileDBError;
use crate::{Datatype, Result as TileDBResult};

/// Describes an aggregate function to apply to an array
/// or field (dimension or attribute) of an array.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AggregateFunction {
    /// Counts the number of cells.
    Count,
    /// Counts the number of NULL values of the argument field.
    NullCount(String),
    /// Computes the minimum value of the argument field.
    Min(String),
    /// Computes the maximum value of the argument field.
    Max(String),
    /// Computes the sum of the argument field.
    Sum(String),
    /// Computes
    Mean(String),
}

impl AggregateFunction {
    pub fn argument_name(&self) -> Option<&str> {
        match self {
            Self::Count => None,
            Self::NullCount(ref s)
            | Self::Min(ref s)
            | Self::Max(ref s)
            | Self::Sum(ref s)
            | Self::Mean(ref s) => Some(s.as_ref()),
        }
    }

    pub(crate) fn result_type_impl(
        &self,
        argument_type: Option<(Datatype, CellValNum)>,
    ) -> Option<Datatype> {
        let is_unit = match argument_type.map(|(_, cvn)| cvn) {
            Some(CellValNum::Fixed(nz)) if nz.get() == 1 => true,
            _ => false,
        };

        match self {
            AggregateFunction::Count | AggregateFunction::NullCount(_) => {
                Some(Datatype::UInt64)
            }
            AggregateFunction::Mean(_) => {
                if is_unit {
                    Some(Datatype::Float64)
                } else {
                    None
                }
            }
            AggregateFunction::Min(_) | AggregateFunction::Max(_) => {
                if is_unit {
                    Some(argument_type.unwrap().0)
                } else {
                    None
                }
            }
            AggregateFunction::Sum(_) => {
                if is_unit {
                    let argument_type = argument_type.unwrap().0;
                    match argument_type {
                        Datatype::Int8
                        | Datatype::Int16
                        | Datatype::Int32
                        | Datatype::Int64 => Some(Datatype::Int64),
                        Datatype::UInt8
                        | Datatype::UInt16
                        | Datatype::UInt32
                        | Datatype::UInt64 => Some(Datatype::UInt64),
                        Datatype::Float32 | Datatype::Float64 => {
                            Some(Datatype::Float64)
                        }
                        _ => None,
                    }
                } else {
                    None
                }
            }
        }
    }

    /// Returns the result `Datatype` of this function when applied
    /// to an array described by `schema`.
    pub fn result_type(&self, schema: &Schema) -> TileDBResult<Datatype> {
        if let Some(arg) = self.argument_name() {
            let f = schema.field(arg.to_owned())?;
            let datatype = f.datatype()?;
            let cell_val_num = f.cell_val_num()?;
            match self.result_type_impl(Some((datatype, cell_val_num))) {
                Some(datatype) => Ok(datatype),
                None => Err(TileDBError::InvalidArgument(anyhow!(format!(
                    "aggregate_type: field '{}' has invalid datatype and cell val num ({}, {})",
                    self.argument_name().unwrap(),
                    datatype, cell_val_num
                )))),
            }
        } else {
            Ok(self.result_type_impl(None).unwrap())
        }
    }
}

impl Display for AggregateFunction {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        <Self as Debug>::fmt(self, f)
    }
}

/// Query builder adapter for constructing queries with aggregate functions.
#[derive(Debug, PartialEq)]
pub struct AggregateBuilder<T, B> {
    base: B,
    agg_str: CString,
    field_str: Option<CString>,
    field_type: PhantomData<T>,
}

/// Query adapter for running queries with aggregate functions.
#[derive(Debug, PartialEq)]
pub struct AggregateQuery<T, Q> {
    base: Q,
    agg_str: CString,
    // NB: C API uses this memory location to store the attribute name if any
    field_name: Option<CString>,
    data: T,
    data_size: u64,
    data_validity: Option<u8>,
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
            field_name: self.field_str,
            data: T::default(),
            data_size: mem::size_of::<T>() as u64,
            data_validity: None,
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
    type Final = (Option<T>, Q::Final);

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

        if let Some(field_name) = self.field_name.as_ref() {
            if self.agg_str.to_str().unwrap() != "NullCount"
                && self
                    .base()
                    .array()
                    .schema()?
                    .field(field_name.clone().into_string().unwrap())?
                    .nullability()?
            {
                self.data_validity = Some(1);

                let c_field_name = field_name.as_ptr();
                let c_validity =
                    self.data_validity.as_mut().unwrap() as *mut u8;
                let mut c_validity_size: u64 = std::mem::size_of::<u8>() as u64;

                context.capi_call(|ctx| unsafe {
                    ffi::tiledb_query_set_validity_buffer(
                        ctx,
                        c_query,
                        agg_c_ptr,
                        c_validity,
                        &mut c_validity_size as *mut u64,
                    )
                })?;
            }
        }

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

        let return_val = if matches!(self.data_validity, Some(0)) {
            None
        } else {
            Some(return_val)
        };

        Ok(ReadStepOutput::Final((return_val, base_q)))
    }
}

/// Trait for query types which can have an aggregate channel placed on top of them.
pub trait AggregateQueryBuilder: QueryBuilder {
    /// Adds an `AggregateFunction` computation to the result
    /// of this query. Returns an `Ok` if `T` is a comptabile type
    /// with the function result; otherwise returns `Err`.
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

        let (agg_name, field_name) = match agg_function {
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
            let c_field_name: *const i8 =
                field_name.as_ref().unwrap().as_c_str().as_ptr();
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
                    c_field_name,
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
            field_str: field_name,
            field_type: PhantomData,
        })
    }

    /// Adds a request for the number of results which satisfy the query predicates.
    fn count(self) -> TileDBResult<AggregateBuilder<u64, Self>> {
        self.apply_aggregate::<u64>(AggregateFunction::Count)
    }

    /// Adds a request for the number of null values of a nullable attribute.
    fn null_count(
        self,
        field_name: &str,
    ) -> TileDBResult<AggregateBuilder<u64, Self>> {
        self.apply_aggregate::<u64>(AggregateFunction::NullCount(
            field_name.to_owned(),
        ))
    }

    /// Adds a request for the average of the data of an attribute or dimension.
    fn mean(
        self,
        field_name: &str,
    ) -> TileDBResult<AggregateBuilder<f64, Self>> {
        self.apply_aggregate::<f64>(AggregateFunction::Mean(
            field_name.to_owned(),
        ))
    }

    /// Adds a request for the sum of the data of an attribute or dimension.
    /// The type of the sum result is given by the generic parameter `T`.
    /// `T` must be a compatible type with the attribute or dimension.
    /// If the attribute or dimension has type:
    /// - `i8`, `i16`, `i32`, or `i64`, then `T` must be `i64`.
    /// - `u8`, `u16`, `u32`, or `u64`, then `T` must be `u64`.
    /// - `f32` or `f64`, then `T` must be `f64`.
    fn sum<T>(self, field_name: &str) -> TileDBResult<AggregateBuilder<T, Self>>
    where
        T: PhysicalType,
    {
        self.apply_aggregate::<T>(AggregateFunction::Sum(field_name.to_owned()))
    }

    /// Adds a request for the minimum value of an attribute or dimension
    /// (within the cells satisfying query predicates, if any).
    /// The generic parameter `T` must be a type compatible with the
    /// attribute or dimension `Datatype`.
    fn min<T>(self, field_name: &str) -> TileDBResult<AggregateBuilder<T, Self>>
    where
        T: PhysicalType,
    {
        self.apply_aggregate::<T>(AggregateFunction::Min(field_name.to_owned()))
    }

    /// Adds a request for the maximum value of an attribute or dimension
    /// (within the cells satisfying query predicates, if any).
    /// The generic parameter `T` must be a type compatible with the
    /// attribute or dimension `Datatype`.
    fn max<T>(
        self,
        field_name: String,
    ) -> TileDBResult<AggregateBuilder<T, Self>>
    where
        T: PhysicalType,
    {
        self.apply_aggregate::<T>(AggregateFunction::Max(field_name))
    }
}

impl AggregateQueryBuilder for ReadBuilder {}

impl<T, B: QueryBuilder> AggregateQueryBuilder for AggregateBuilder<T, B> where
    T: PhysicalType
{
}

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;
