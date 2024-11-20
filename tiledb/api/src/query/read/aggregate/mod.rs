use super::*;

use anyhow::anyhow;
use ffi::{
    tiledb_channel_operation_t, tiledb_channel_operator_t,
    tiledb_query_channel_t,
};
use std::ffi::CString;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::marker::PhantomData;
use std::mem;

use crate::array::{CellValNum, Schema};
use crate::datatype::PhysicalType;
use crate::error::{DatatypeError, Error as TileDBError};
use crate::{Datatype, Result as TileDBResult};

/// Describes an aggregate function to apply to an array
/// or field (dimension or attribute) of an array.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
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
    /// Returns the name of the field which this function applies to.
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

    /// Returns a unique name for this aggregate function.
    pub fn aggregate_name(&self) -> String {
        match self {
            Self::Count => "Count".to_owned(),
            Self::NullCount(ref s) => format!("NullCount({})", s),
            Self::Min(ref s) => format!("Min({})", s),
            Self::Max(ref s) => format!("Max({})", s),
            Self::Sum(ref s) => format!("Sum({})", s),
            Self::Mean(ref s) => format!("Mean({})", s),
        }
    }

    /// Returns the result type of the aggregate operation.
    ///
    /// This is used to determine if the user's requested programmatic data type
    /// is compatible with the result type of the aggregate function.
    pub(crate) fn result_type_impl(
        &self,
        argument_type: Option<(Datatype, CellValNum)>,
    ) -> Option<Datatype> {
        let is_unit = matches!(argument_type.map(|(_, cvn)| cvn), Some(CellValNum::Fixed(nz)) if nz.get() == 1);

        match self {
            AggregateFunction::Count | AggregateFunction::NullCount(_) => {
                // see `CountAggregatorBase<ValidityPolicy>::copy_to_user_buffer`
                // in tiledb/sm/query/readers/aggregates/count_aggregator.cc
                Some(Datatype::UInt64)
            }
            AggregateFunction::Mean(_) => {
                // see `MeanAggregator<T>::copy_to_user_buffer` in
                // tiledb/sm/query/readers/aggregates/sum_aggregator.cc
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
                    // TODO: SC-54898
                    None
                }
            }
            AggregateFunction::Sum(_) => {
                // see tiledb/sm/query/readers/aggregates/sum_type.h
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

/// Encapsulates data needed to run an aggregate function in the C API.
#[cfg(feature = "raw")]
#[derive(Debug)]
pub struct AggregateFunctionHandle {
    function: AggregateFunction,
    // NB: C API uses this memory location to store the attribute name if any
    agg_name: CString,
    field_name: Option<CString>,
}

#[cfg(not(feature = "raw"))]
#[derive(Debug)]
struct AggregateFunctionHandle {
    function: AggregateFunction,
    // NB: C API uses this memory location to store the attribute name if any
    agg_name: CString,
    field_name: Option<CString>,
}

impl AggregateFunctionHandle {
    pub fn new(function: AggregateFunction) -> TileDBResult<Self> {
        let agg_name = cstring!(function.aggregate_name());
        let field_name = if let Some(arg) = function.argument_name() {
            Some(cstring!(arg))
        } else {
            None
        };

        Ok(AggregateFunctionHandle {
            function,
            agg_name,
            field_name,
        })
    }

    pub fn aggregate(&self) -> &AggregateFunction {
        &self.function
    }

    pub fn aggregate_name(&self) -> &std::ffi::CStr {
        &self.agg_name
    }

    pub fn field_name(&self) -> Option<&std::ffi::CStr> {
        self.field_name.as_ref().map(|c| c.deref())
    }
}

impl AggregateFunctionHandle {
    pub fn apply_to_raw_query(
        &self,
        context: &Context,
        c_query: *mut ffi::tiledb_query_t,
    ) -> TileDBResult<()> {
        let mut c_channel: *mut tiledb_query_channel_t = out_ptr!();
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_query_get_default_channel(ctx, c_query, &mut c_channel)
        })?;

        // C API functionality
        let mut c_agg_operator: *const tiledb_channel_operator_t = out_ptr!();
        let mut c_agg_operation: *mut tiledb_channel_operation_t = out_ptr!();
        let c_agg_name = self.agg_name.as_c_str().as_ptr();

        // The if statement and match statement are in different arms because of the agg_operation
        // variable takes in different types in the respective functions.
        if self.function == AggregateFunction::Count {
            context.capi_call(|ctx| unsafe {
                ffi::tiledb_aggregate_count_get(
                    ctx,
                    core::ptr::addr_of_mut!(c_agg_operation)
                        as *mut *const tiledb_channel_operation_t,
                )
            })?;
        } else {
            let c_field_name =
                self.field_name.as_ref().unwrap().as_c_str().as_ptr();
            match self.function {
                AggregateFunction::Count => unreachable!(
                    "AggregateFunction::Count handled in above case, found {:?}",
                    self.function
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
        })
    }
}

/// Query builder adapter for constructing queries with aggregate functions.
#[derive(Debug)]
pub struct AggregateBuilder<T, B> {
    base: B,
    handle: AggregateFunctionHandle,
    field_type: PhantomData<T>,
}

/// Query adapter for running queries with aggregate functions.
#[derive(Debug)]
pub struct AggregateQuery<T, Q> {
    base: Q,
    handle: AggregateFunctionHandle,
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
            handle: self.handle,
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
        let agg_str: &CString = &self.handle.agg_name;
        let agg_c_ptr = agg_str.as_c_str().as_ptr();

        context.capi_call(|ctx| unsafe {
            ffi::tiledb_query_set_data_buffer(
                ctx, c_query, agg_c_ptr, c_bufptr, c_sizeptr,
            )
        })?;

        if let Some(field_name) = self.handle.field_name.as_ref() {
            if !matches!(self.handle.function, AggregateFunction::NullCount(_))
                && self
                    .base()
                    .array()
                    .schema()?
                    .field(field_name.clone().into_string().unwrap())?
                    .nullability()?
            {
                self.data_validity = Some(1);

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
                DatatypeError::physical_type_incompatible::<T>(expected_type),
            ));
        }

        let handle = AggregateFunctionHandle::new(agg_function)?;

        let context = self.base().context();
        let c_query = **self.base().cquery();

        handle.apply_to_raw_query(&context, c_query)?;

        Ok(AggregateBuilder::<T, Self> {
            base: self,
            handle,
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
    fn max<T>(self, field_name: &str) -> TileDBResult<AggregateBuilder<T, Self>>
    where
        T: PhysicalType,
    {
        self.apply_aggregate::<T>(AggregateFunction::Max(field_name.to_owned()))
    }
}

impl AggregateQueryBuilder for ReadBuilder {}

impl<T, B: QueryBuilder> AggregateQueryBuilder for AggregateBuilder<T, B> where
    T: PhysicalType
{
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use uri::TestArrayUri;

    use super::*;
    use crate::tests::prelude::*;

    /// Initialize a quickstart array for aggregate testing.
    ///
    /// Overrides the attribute to be nullable.
    /// TODO: also put some actual nulls in there!
    fn quickstart_init(name: &str) -> TileDBResult<TestArray> {
        let mut array = TestArray::new(
            name,
            Rc::new({
                let mut b = crate::tests::examples::quickstart::Builder::new(
                    ArrayType::Sparse,
                )
                .with_cols(DimensionConstraints::Int32([5, 8], Some(4)));
                b.attribute().nullability = Some(true);
                b.build()
            }),
        )?;

        let rows = vec![1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4];
        let cols = vec![5, 6, 7, 8, 5, 6, 7, 8, 5, 6, 7, 8, 5, 6, 7, 8];
        let atts = vec![
            16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
        ];
        {
            let a = array.for_write()?;
            let query = WriteBuilder::new(a)?
                .data_typed("rows", &rows)?
                .data_typed("cols", &cols)?
                .data_typed("a", &atts)?
                .build();

            query.submit()?;
            query.finalize().map(|_| ())?;
        }

        Ok(array)
    }

    #[test]
    fn quickstart_aggregate_queries_single_function() -> TileDBResult<()> {
        let array = quickstart_init("single_function")?;

        let mut a = array.for_read()?;

        macro_rules! do_agg {
            ($function:expr, $datatype:ty) => {{
                let mut q = ReadBuilder::new(a)?
                    .apply_aggregate::<$datatype>($function)?
                    .build();
                let (r, _) = q.execute()?;
                #[allow(unused_assignments)]
                {
                    a = q.finalize()?;
                }
                r
            }};
        }

        // count
        {
            let count = do_agg!(AggregateFunction::Count, u64);
            assert_eq!(Some(16), count);
        }

        // NB: NullCount("rows") and NullCount("cols") are an error, see SC-52312

        // NullCount("a")
        {
            let null_count =
                do_agg!(AggregateFunction::NullCount("a".to_owned()), u64);
            assert_eq!(Some(0), null_count);
        }

        // Min("rows")
        {
            let min_row =
                do_agg!(AggregateFunction::Min("rows".to_owned()), i32);
            assert_eq!(Some(1), min_row);
        }
        // Min("cols")
        {
            let min_col =
                do_agg!(AggregateFunction::Min("cols".to_owned()), i32);
            assert_eq!(Some(5), min_col);
        }
        // Min("a")
        {
            let min_a = do_agg!(AggregateFunction::Min("a".to_owned()), i32);
            assert_eq!(Some(16), min_a);
        }

        // Max("rows")
        {
            let max_row =
                do_agg!(AggregateFunction::Max("rows".to_owned()), i32);
            assert_eq!(Some(4), max_row);
        }
        // Max("cols")
        {
            let max_col =
                do_agg!(AggregateFunction::Max("cols".to_owned()), i32);
            assert_eq!(Some(8), max_col);
        }
        // Max("a")
        {
            let max_a = do_agg!(AggregateFunction::Max("a".to_owned()), i32);
            assert_eq!(Some(31), max_a);
        }

        // Sum("rows")
        {
            let sum_row =
                do_agg!(AggregateFunction::Sum("rows".to_owned()), i64);
            assert_eq!(Some(40), sum_row);
        }
        // Sum("cols")
        {
            let sum_col =
                do_agg!(AggregateFunction::Sum("cols".to_owned()), i64);
            assert_eq!(Some(104), sum_col);
        }
        // Sum("a")
        {
            let sum_a = do_agg!(AggregateFunction::Sum("a".to_owned()), i64);
            assert_eq!(Some(376), sum_a);
        }

        // Mean("rows")
        {
            let mean_row =
                do_agg!(AggregateFunction::Mean("rows".to_owned()), f64);
            assert_eq!(Some(2.5), mean_row);
        }
        // Mean("cols")
        {
            let mean_col =
                do_agg!(AggregateFunction::Mean("cols".to_owned()), f64);
            assert_eq!(Some(6.5), mean_col);
        }
        // Mean("a")
        {
            let mean_a = do_agg!(AggregateFunction::Mean("a".to_owned()), f64);
            assert_eq!(Some(23.5), mean_a);
        }

        Ok(())
    }

    #[test]
    fn quickstart_aggregate_queries_multi_function() -> TileDBResult<()> {
        let a = quickstart_init("multi_function")?;
        let mut a = a.for_read()?;

        let mut q_rows = ReadBuilder::new(a)?
            .min::<i32>("rows")?
            .max::<i32>("rows")?
            .sum::<i64>("rows")?
            .mean("rows")?
            .build();
        let (mean_rows, (sum_rows, (max_rows, (min_rows, _)))) =
            q_rows.execute()?;
        a = q_rows.finalize()?;

        let mut q_cols = ReadBuilder::new(a)?
            .min::<i32>("cols")?
            .max::<i32>("cols")?
            .sum::<i64>("cols")?
            .mean("cols")?
            .build();
        let (mean_cols, (sum_cols, (max_cols, (min_cols, _)))) =
            q_cols.execute()?;
        a = q_cols.finalize()?;

        let mut q_a = ReadBuilder::new(a)?
            .count()?
            .null_count("a")?
            .min::<i32>("a")?
            .max::<i32>("a")?
            .sum::<i64>("a")?
            .mean("a")?
            .build();
        let (mean_a, (sum_a, (max_a, (min_a, (null_count_a, (count, _)))))) =
            q_a.execute()?;

        assert_eq!(Some(16), count);
        assert_eq!(Some(1), min_rows);
        assert_eq!(Some(4), max_rows);
        assert_eq!(Some(40), sum_rows);
        assert_eq!(Some(2.5), mean_rows);
        assert_eq!(Some(5), min_cols);
        assert_eq!(Some(8), max_cols);
        assert_eq!(Some(104), sum_cols);
        assert_eq!(Some(6.5), mean_cols);
        assert_eq!(Some(0), null_count_a);
        assert_eq!(Some(16), min_a);
        assert_eq!(Some(31), max_a);
        assert_eq!(Some(376), sum_a);
        assert_eq!(Some(23.5), mean_a);

        Ok(())
    }

    #[test]
    fn quickstart_aggregate_queries_same_function_different_args(
    ) -> TileDBResult<()> {
        let a = quickstart_init("same_function_different_args")?;

        let mut a = a.for_read()?;

        let mut q_min = ReadBuilder::new(a)?
            .min::<i32>("rows")?
            .min::<i32>("cols")?
            .min::<i32>("a")?
            .build();
        let (min_a, (min_cols, (min_rows, _))) = q_min.execute()?;
        a = q_min.finalize()?;

        let mut q_max = ReadBuilder::new(a)?
            .max::<i32>("rows")?
            .max::<i32>("cols")?
            .max::<i32>("a")?
            .build();
        let (max_a, (max_cols, (max_rows, _))) = q_max.execute()?;
        a = q_max.finalize()?;

        let mut q_sum = ReadBuilder::new(a)?
            .sum::<i64>("rows")?
            .sum::<i64>("cols")?
            .sum::<i64>("a")?
            .build();
        let (sum_a, (sum_cols, (sum_rows, _))) = q_sum.execute()?;
        a = q_sum.finalize()?;

        let mut q_mean = ReadBuilder::new(a)?
            .mean("rows")?
            .mean("cols")?
            .mean("a")?
            .build();
        let (mean_a, (mean_cols, (mean_rows, _))) = q_mean.execute()?;

        assert_eq!(Some(1), min_rows);
        assert_eq!(Some(4), max_rows);
        assert_eq!(Some(40), sum_rows);
        assert_eq!(Some(2.5), mean_rows);
        assert_eq!(Some(5), min_cols);
        assert_eq!(Some(8), max_cols);
        assert_eq!(Some(104), sum_cols);
        assert_eq!(Some(6.5), mean_cols);
        assert_eq!(Some(16), min_a);
        assert_eq!(Some(31), max_a);
        assert_eq!(Some(376), sum_a);
        assert_eq!(Some(23.5), mean_a);

        Ok(())
    }

    /// When this test fails, update `impl Arbitrary for AggregateFunction`
    #[test]
    fn sc_52312_null_count_on_dimension() -> TileDBResult<()> {
        let c = Context::new().unwrap();

        let schema = {
            let domain = {
                let dimension = DimensionBuilder::new(
                    &c,
                    "d",
                    Datatype::UInt64,
                    [0u64, 100u64],
                )?
                .build();
                DomainBuilder::new(&c)?.add_dimension(dimension)?.build()
            };
            let attribute = AttributeBuilder::new(&c, "a", Datatype::UInt64)?
                .nullability(false)?
                .build();

            SchemaBuilder::new(&c, ArrayType::Sparse, domain)?
                .add_attribute(attribute)?
                .build()?
        };

        let test_uri = uri::get_uri_generator()
            .map_err(|e| Error::Other(e.to_string()))?;

        let uri = test_uri
            .with_path("sc_52312")
            .map_err(|e| Error::Other(e.to_string()))?;

        Array::create(&c, &uri, schema)?;

        // try dimension
        {
            let a = Array::open(&c, &uri, Mode::Read)?;
            let r = ReadBuilder::new(a)?
                .layout(QueryLayout::Unordered)?
                .null_count("d");
            assert!(matches!(r, Err(Error::LibTileDB(_))));
        }

        // try attribute
        {
            let a = Array::open(&c, &uri, Mode::Read)?;
            let r = ReadBuilder::new(a)?
                .layout(QueryLayout::Unordered)?
                .null_count("a");
            assert!(matches!(r, Err(Error::LibTileDB(_))));
        }

        Ok(())
    }

    #[test]
    fn quickstart_aggregate_wrong_result_type() -> TileDBResult<()> {
        let array = quickstart_init("wrong_result_type")?;

        macro_rules! try_apply {
            ($function:expr, $datatype:ty) => {{
                ReadBuilder::new(array.for_read()?)?
                    .apply_aggregate::<$datatype>($function)
                    .and_then(|b| b.build().execute())
            }};
        }

        // Count: only u64
        {
            let e = try_apply!(AggregateFunction::Count, i64);
            assert!(matches!(
                e,
                Err(TileDBError::Datatype(
                    DatatypeError::PhysicalTypeIncompatible {
                        logical_type: Datatype::UInt64,
                        ..
                    }
                ))
            ));

            let e = try_apply!(AggregateFunction::Count, u32);
            assert!(matches!(
                e,
                Err(TileDBError::Datatype(
                    DatatypeError::PhysicalTypeIncompatible {
                        logical_type: Datatype::UInt64,
                        ..
                    }
                ))
            ));
        }

        // Null count only u64
        {
            let e =
                try_apply!(AggregateFunction::NullCount("a".to_owned()), i64);
            assert!(matches!(
                e,
                Err(TileDBError::Datatype(
                    DatatypeError::PhysicalTypeIncompatible {
                        logical_type: Datatype::UInt64,
                        ..
                    }
                ))
            ));

            let e =
                try_apply!(AggregateFunction::NullCount("a".to_owned()), u32);
            assert!(matches!(
                e,
                Err(TileDBError::Datatype(
                    DatatypeError::PhysicalTypeIncompatible {
                        logical_type: Datatype::UInt64,
                        ..
                    }
                ))
            ));
        }

        // Min/Max type must match
        {
            let e = try_apply!(AggregateFunction::Min("a".to_owned()), i64);
            assert!(matches!(
                e,
                Err(TileDBError::Datatype(
                    DatatypeError::PhysicalTypeIncompatible {
                        logical_type: Datatype::Int32,
                        ..
                    }
                ))
            ));

            let e = try_apply!(AggregateFunction::Max("a".to_owned()), u32);
            assert!(matches!(
                e,
                Err(TileDBError::Datatype(
                    DatatypeError::PhysicalTypeIncompatible {
                        logical_type: Datatype::Int32,
                        ..
                    }
                ))
            ));
        }

        // Sum must be 64 bits with same sign
        {
            let e = try_apply!(AggregateFunction::Sum("a".to_owned()), u64);
            assert!(matches!(
                e,
                Err(TileDBError::Datatype(
                    DatatypeError::PhysicalTypeIncompatible {
                        logical_type: Datatype::Int64,
                        ..
                    }
                ))
            ));

            let e = try_apply!(AggregateFunction::Sum("a".to_owned()), i32);
            assert!(matches!(
                e,
                Err(TileDBError::Datatype(
                    DatatypeError::PhysicalTypeIncompatible {
                        logical_type: Datatype::Int64,
                        ..
                    }
                ))
            ));
        }

        // Mean always must be f64
        {
            let e = try_apply!(AggregateFunction::Mean("a".to_owned()), i32);
            assert!(matches!(
                e,
                Err(TileDBError::Datatype(
                    DatatypeError::PhysicalTypeIncompatible {
                        logical_type: Datatype::Float64,
                        ..
                    }
                ))
            ));

            let e = try_apply!(AggregateFunction::Mean("a".to_owned()), f32);
            assert!(matches!(
                e,
                Err(TileDBError::Datatype(
                    DatatypeError::PhysicalTypeIncompatible {
                        logical_type: Datatype::Float64,
                        ..
                    }
                ))
            ));
        }

        Ok(())
    }

    /// When this test fails, update `impl Arbitrary for AggregateFunction`
    #[test]
    fn sc_53791_null_count_on_var_attribute() -> TileDBResult<()> {
        let c = Context::new().unwrap();

        let schema = {
            let domain = {
                let dimension = DimensionBuilder::new(
                    &c,
                    "d",
                    Datatype::UInt64,
                    [0u64, 100u64],
                )?
                .build();
                DomainBuilder::new(&c)?.add_dimension(dimension)?.build()
            };
            let attribute = AttributeBuilder::new(&c, "a", Datatype::UInt64)?
                .nullability(true)?
                .cell_val_num(CellValNum::Var)?
                .build();

            SchemaBuilder::new(&c, ArrayType::Sparse, domain)?
                .add_attribute(attribute)?
                .build()?
        };

        let test_uri = uri::get_uri_generator()
            .map_err(|e| Error::Other(e.to_string()))?;

        let uri = test_uri
            .with_path("sc_53791")
            .map_err(|e| Error::Other(e.to_string()))?;

        Array::create(&c, &uri, schema)?;

        // insert a cell
        {
            let values_d = vec![0u64];
            let values_a = vec![vec![0u64]];

            let a = Array::open(&c, &uri, Mode::Write)?;
            let q = WriteBuilder::new(a)?
                .data("d", &values_d)?
                .data("a", &values_a)?
                .build();

            q.submit()?;
            q.finalize()?;
        }

        // try query
        let a = Array::open(&c, &uri, Mode::Read)?;

        let mut q = ReadBuilder::new(a)?
            .layout(QueryLayout::Unordered)?
            .null_count("a")?
            .build();
        let r = q.execute();
        assert!(matches!(r, Err(Error::LibTileDB(_))));

        Ok(())
    }

    /// Test running min/max aggregates on empty input.
    #[test]
    fn sc_54468_min_max_non_nullable_empty_input() -> TileDBResult<()> {
        let a = TestArray::new(
            "min_max_empty_input",
            Rc::new(
                crate::tests::examples::quickstart::Builder::new(
                    ArrayType::Sparse,
                )
                .build(),
            ),
        )?;

        let mut q = ReadBuilder::new(a.for_read()?)?
            .min::<i32>("a")?
            .max::<i32>("a")?
            .build();
        let (a_max, (a_min, _)) = q.execute()?;

        // This is deliberately a wrong result.
        // We capture it here to track SC-54468.
        // When that issue is resolved this will begin to fail;
        // then update these to `None` and update all code which
        // references SC-54468.
        assert_eq!(Some(0), a_min);
        assert_eq!(Some(0), a_max);

        Ok(())
    }

    /// Test running sum aggregate on empty input.
    #[test]
    fn sc_54468_sum_non_nullable_empty_input() -> TileDBResult<()> {
        let a = TestArray::new(
            "sum_empty_input",
            Rc::new(
                crate::tests::examples::quickstart::Builder::new(
                    ArrayType::Sparse,
                )
                .build(),
            ),
        )?;

        let mut q = ReadBuilder::new(a.for_read()?)?.sum::<i64>("a")?.build();
        let (a_sum, _) = q.execute()?;

        // This is deliberately a wrong result.
        // We capture it here to track SC-54468.
        // When that issue is resolved this will begin to fail;
        // then update these to `None` and update all code which
        // references SC-54468.
        assert_eq!(Some(0), a_sum);

        Ok(())
    }
}
