use super::*;

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::marker::PhantomData;
use std::mem;
use std::pin::Pin;

use anyhow::Error;
use anyhow::anyhow;
use ffi::{tiledb_channel_operation_t, tiledb_channel_operator_t, tiledb_query_channel_t};
use paste::paste;

use crate::config::Config;
use crate::query::buffer::{BufferMut, QueryBuffersMut};
use crate::query::read::output::ScratchAllocator;
use crate::Result as TileDBResult;
use crate::error::Error as TileDBError;

mod callback;
pub mod output;
mod raw;
mod typed;

pub use callback::*;
pub use raw::*;
pub use typed::*;

/// Contains a return status and/or result from submitting a query.
#[derive(Clone, Debug)]
pub enum ReadStepOutput<I, F> {
    /// There was not enough space to hold any results.
    /// Allocate more space and try again.
    NotEnoughSpace,
    /// There was enough space for some, but not all, results.
    /// Contains the intermediate representation of those results.
    /// Re-submitting the query will advance to the next portion of results.
    Intermediate(I),
    /// Contains the final representation of the query results.
    /// Re-submitting the query again will start over from the beginning.
    Final(F),
}

impl<I, F> ReadStepOutput<I, F> {
    /// Returns `true` if the output is an intermediate query result.
    pub const fn is_intermediate(&self) -> bool {
        matches!(self, ReadStepOutput::Intermediate(_))
    }

    /// Returns `true` if the output is a final query result.
    pub const fn is_final(&self) -> bool {
        matches!(self, ReadStepOutput::Final(_))
    }

    /// Converts from `&ReadStepOutput<I, F>` to `ReadStepOutput<&I, &F>`.
    pub const fn as_ref(&self) -> ReadStepOutput<&I, &F> {
        match self {
            ReadStepOutput::NotEnoughSpace => ReadStepOutput::NotEnoughSpace,
            ReadStepOutput::Intermediate(ref i) => {
                ReadStepOutput::Intermediate(i)
            }
            ReadStepOutput::Final(ref f) => ReadStepOutput::Final(f),
        }
    }

    /// Converts from `&mut ReadStepOutput<I, F>` to `ReadStepOutput<&mut I, &mut F>`.
    pub fn as_mut(&mut self) -> ReadStepOutput<&mut I, &mut F> {
        match self {
            ReadStepOutput::NotEnoughSpace => ReadStepOutput::NotEnoughSpace,
            ReadStepOutput::Intermediate(ref mut i) => {
                ReadStepOutput::Intermediate(i)
            }
            ReadStepOutput::Final(ref mut f) => ReadStepOutput::Final(f),
        }
    }

    /// Maps a `ReadStepOutput<I, F>` to `ReadStepOutput<U, F>` by applying a
    /// function to an intermediate result value if able.
    pub fn map_i<U, FN>(self, f: FN) -> ReadStepOutput<U, F>
    where
        FN: FnOnce(I) -> U,
    {
        match self {
            ReadStepOutput::NotEnoughSpace => ReadStepOutput::NotEnoughSpace,
            ReadStepOutput::Intermediate(i) => {
                ReadStepOutput::Intermediate(f(i))
            }
            ReadStepOutput::Final(f) => ReadStepOutput::Final(f),
        }
    }

    /// Maps a `ReadStepOutput<I, F>` to `ReadStepOutput<I, U>` by applying a
    /// function to a final result value if able.
    pub fn map_f<U, FN>(self, f: FN) -> ReadStepOutput<I, U>
    where
        FN: FnOnce(F) -> U,
    {
        match self {
            ReadStepOutput::NotEnoughSpace => ReadStepOutput::NotEnoughSpace,
            ReadStepOutput::Intermediate(i) => ReadStepOutput::Intermediate(i),
            ReadStepOutput::Final(fr) => ReadStepOutput::Final(f(fr)),
        }
    }

    /// Returns the contained `Intermediate` result value, consuming `self`.
    ///
    /// # Panics
    ///
    /// Panics if the `self` value is not `Intermediate`.
    ///
    /// # Examples
    ///
    /// ```
    /// use tiledb::query::ReadStepOutput;
    /// let r = ReadStepOutput::<String, String>::Intermediate("tiledb".to_string());
    /// assert_eq!("tiledb", r.unwrap_intermediate());
    /// ```
    ///
    /// ```should_panic
    /// use tiledb::query::ReadStepOutput;
    /// let r = ReadStepOutput::<String, String>::Final("tiledb".to_string());
    /// assert_eq!("tiledb", r.unwrap_intermediate()); // fails
    /// ```
    pub fn unwrap_intermediate(self) -> I {
        match self {
            ReadStepOutput::Intermediate(i) => i,
            ReadStepOutput::NotEnoughSpace => panic!("Called `ReadStepOutput::unwrap_intermediate` on `NotEnoughSpace`"),
            ReadStepOutput::Final(_) => panic!("Called `ReadStepOutput::unwrap_intermediate` on `Final`"),
        }
    }

    /// Returns the contained `Final` result value, consuming `self`.
    ///
    /// # Panics
    ///
    /// Panics if the `self` value is not `Final`.
    ///
    /// # Examples
    ///
    /// ```
    /// use tiledb::query::ReadStepOutput;
    /// let r = ReadStepOutput::<String, String>::Final("tiledb".to_string());
    /// assert_eq!("tiledb", r.unwrap_final());
    /// ```
    ///
    /// ```should_panic
    /// use tiledb::query::ReadStepOutput;
    /// let r = ReadStepOutput::<String, String>::Intermediate("tiledb".to_string());
    /// assert_eq!("tiledb", r.unwrap_final()); // fails
    /// ```
    pub fn unwrap_final(self) -> F {
        match self {
            ReadStepOutput::Final(f) => f,
            ReadStepOutput::Intermediate(_) => panic!(
                "Called `ReadStepOutput::unwrap_final` on `Intermediate`"
            ),
            ReadStepOutput::NotEnoughSpace => panic!(
                "Called `ReadStepOutput::unwrap_final` on `NotEnoughSpace`"
            ),
        }
    }
}

impl<U> ReadStepOutput<U, U> {
    /// Consumes the `ReadStepOutput`, returning the enclosed output if one is available.
    pub fn into_inner(self) -> Option<U> {
        match self {
            ReadStepOutput::NotEnoughSpace => None,
            ReadStepOutput::Intermediate(i) => Some(i),
            ReadStepOutput::Final(f) => Some(f),
        }
    }
}

#[derive(Default)]
pub enum ScratchStrategy<'data, C> {
    #[default]
    AttributeDefault,
    RawBuffers(&'data RefCell<QueryBuffersMut<'data, C>>),
    CustomAllocator(Box<dyn ScratchAllocator<C> + 'data>),
}

impl<'data, C> From<&'data RefCell<QueryBuffersMut<'data, C>>>
    for ScratchStrategy<'data, C>
{
    fn from(value: &'data RefCell<QueryBuffersMut<'data, C>>) -> Self {
        ScratchStrategy::RawBuffers(value)
    }
}

/// Trait for runnable read queries.
pub trait ReadQuery: Query {
    type Intermediate;
    type Final;

    /// Run the query until it has filled up its scratch space.
    fn step(
        &mut self,
    ) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>>;

    /// Run the query to completion.
    /// Query adapters may interleave their operations
    /// between individual steps of the query.
    fn execute(&mut self) -> TileDBResult<Self::Final> {
        Ok(loop {
            if let ReadStepOutput::Final(result) = self.step()? {
                break result;
            }
        })
    }

    /// Convert this query into an iterator which yields an item
    /// for each step of the query.
    fn into_iter(self) -> ReadQueryIterator<Self::Intermediate, Self::Final>
    where
        Self: Sized + 'static,
    {
        ReadQueryIterator {
            query: Some(Box::new(self)),
        }
    }
}

macro_rules! fn_register_callback {
    ($fn:ident, $Callback:ty, $Builder:ident, $($U:ident),+) => {
        paste! {
            /// Register a callback to be run on query results
            /// which are written into the provided scratch space.
            fn $fn<T>(self,
                $(
                    ([< field_ $U:snake >],
                     [< scratch_ $U:snake >]):
                    (&str, ScratchStrategy<'data, <T as $Callback>::$U>),
                )+
                callback: T
            ) -> TileDBResult<$Builder<'data, T, Self>>
            where
                Self: Sized,
                T: $Callback
            {
                $(
                    let [< arg_ $U:snake >] = {
                        let field = {
                            let schema = self.base().array().schema()?;
                            schema.field([< field_ $U:snake >])?
                        };
                        let metadata = FieldMetadata::try_from(&field)?;
                        match [< scratch_ $U:snake >] {
                            ScratchStrategy::AttributeDefault => {
                                let alloc : Box<dyn ScratchAllocator<<T as $Callback>::$U> + 'data> = Box::new(field.query_scratch_allocator(None)?);
                                let managed = ManagedBuffer::from(alloc);
                                RawReadHandle::managed(metadata, managed)
                            },
                            ScratchStrategy::RawBuffers(qb) => {
                                RawReadHandle::new(metadata, qb)
                            },
                            ScratchStrategy::CustomAllocator(a) => {
                                let managed = ManagedBuffer::from(a);
                                RawReadHandle::managed(metadata, managed)
                            }
                        }
                    };
                )+

                let base = self;

                Ok($Builder {
                    callback,
                    base,
                    $(
                        [< arg_ $U:snake >],
                    )+
                })
            }
        }
    }
}

/// Trait for constructing a read query.
/// Provides methods for flexibly adapting requested attributes into raw results,
/// callbacks, or strongly-typed objects.
pub trait ReadQueryBuilder<'data>: QueryBuilder {
    /// Register a raw memory location to read query results into.
    fn register_raw<S, C>(
        self,
        field: S,
        scratch: &'data RefCell<QueryBuffersMut<'data, C>>,
    ) -> TileDBResult<RawReadBuilder<'data, Self>>
    where
        Self: Sized,
        S: AsRef<str>,
        RawReadHandle<'data, C>: Into<TypedReadHandle<'data>>,
    {
        let metadata = {
            let schema = self.base().array().schema()?;
            let field = schema.field(field.as_ref())?;
            FieldMetadata {
                name: field.name()?,
                datatype: field.datatype()?,
                cell_val_num: field.cell_val_num()?,
            }
        };
        Ok(RawReadBuilder {
            raw_read_output: RawReadHandle::new(metadata, scratch).into(),
            base: self,
        })
    }

    /// Register raw memory locations to read query results from multiple attributes into
    fn register_var_raw<I>(
        self,
        fields: I,
    ) -> TileDBResult<VarRawReadBuilder<'data, Self>>
    where
        I: IntoIterator<Item = TypedReadHandle<'data>>,
    {
        Ok(VarRawReadBuilder {
            raw_read_output: fields.into_iter().collect(),
            base: self,
        })
    }

    fn_register_callback!(
        register_callback,
        ReadCallback,
        CallbackReadBuilder,
        Unit
    );

    fn_register_callback!(
        register_callback2,
        ReadCallback2Arg,
        Callback2ArgReadBuilder,
        Unit1,
        Unit2
    );

    fn_register_callback!(
        register_callback3,
        ReadCallback3Arg,
        Callback3ArgReadBuilder,
        Unit1,
        Unit2,
        Unit3
    );

    fn_register_callback!(
        register_callback4,
        ReadCallback4Arg,
        Callback4ArgReadBuilder,
        Unit1,
        Unit2,
        Unit3,
        Unit4
    );

    fn register_callback_var<I, T>(
        self,
        fields: I,
        callback: T,
    ) -> TileDBResult<CallbackVarArgReadBuilder<'data, T, Self>>
    where
        I: IntoIterator<Item = TypedReadHandle<'data>>,
    {
        Ok(CallbackVarArgReadBuilder {
            callback,
            base: self.register_var_raw(fields)?,
        })
    }

    /// Register a typed result to be constructed from the query results.
    /// Intermediate raw results are written into the provided scratch space.
    fn register_constructor<S, T>(
        self,
        field: S,
        scratch: ScratchStrategy<
            'data,
            <<T as ReadResult>::Constructor as ReadCallback>::Unit,
        >,
    ) -> TileDBResult<TypedReadBuilder<'data, T, Self>>
    where
        Self: Sized,
        S: AsRef<str>,
        T: ReadResult,
        <T as ReadResult>::Constructor: Default,
    {
        let r = <T::Constructor as Default>::default();
        Ok(TypedReadBuilder {
            _marker: std::marker::PhantomData,
            base: self.register_callback((field.as_ref(), scratch), r)?,
        })
    }
}

pub struct AggregateBuilder<B, T> {
    base: B,
    agg_str: CString,
    attr_str: Option<CString>,
    attr_type: PhantomData<T>,
}

pub struct AggregateReader<Q, T> {
    base: Q,
    agg_str: CString,
    _attr_str: Option<CString>,
    attr_type: PhantomData<T>,
}

impl<B, T> QueryBuilder for AggregateBuilder<B, T> where B : QueryBuilder {
    type Query = AggregateReader<B::Query, T>;

    fn base(&self) -> &BuilderBase {
        &self.base.base()
    }

    fn build(self) -> Self::Query {
        AggregateReader::<B::Query, T> {
            base: self.base.build(),
            agg_str: self.agg_str,
            _attr_str: self.attr_str,
            attr_type: self.attr_type
        }
    }
}
    

impl<Q, T> Query for AggregateReader<Q, T> where Q : Query {
    fn base(&self) -> &QueryBase {
        &self.base.base()
    }

    fn finalize(self) -> TileDBResult<Array>
        where
            Self: Sized {
        self.base.finalize()
    }
}

impl<Q, T> ReadQuery for AggregateReader<Q, T> where Q: ReadQuery, {
    type Intermediate = ();
    type Final = T;

    fn step(
        &mut self,
    ) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>> {
        // Register the data buffer (set data buffer)
        let context = self.base().context();
        let cquery = **self.base().cquery();
        let mut location : T = out_ptr!();
        let mut size : u64 = mem::size_of::<T>().try_into().unwrap();

        let location_ptr = &mut location as *mut T;
        let c_bufptr = location_ptr as *mut std::ffi::c_void;
        let c_sizeptr = &mut size as *mut u64;
        let agg_str: &CString = &self.agg_str;
        let agg_c_ptr = agg_str.as_c_str().as_ptr() as *const i8;

        context.capi_call(|ctx| unsafe {
            ffi::tiledb_query_set_data_buffer(
                ctx,
                cquery,
                agg_c_ptr,
                c_bufptr,
                c_sizeptr,
            )
        })?;

        let base_result = self.base.step()?;
        
        // Run the query in a loop until you get the final result
        let return_val = match base_result {
            ReadStepOutput::Final(_) => location,
            ReadStepOutput::Intermediate(_) => unreachable!(),
            ReadStepOutput::NotEnoughSpace => unreachable!(),
        };

        Ok(ReadStepOutput::Final(return_val))

    }
}

#[derive(PartialEq)]
pub enum AggregateType {
    Count,
    Max,
    Mean,
    Min,
    NullCount,
    Sum
}

pub trait AggregateBuilderTrait : QueryBuilder {
    fn apply_aggregate<T>(self, agg_type : AggregateType, name: Option<String>) -> TileDBResult<AggregateBuilder<Self, T>>  {
        // Put aggregate C API functions here (channel initialization and setup)
        // So far only count
        let context = self.base().context();
        let cquery = **self.base().cquery();
        let mut default_channel : *mut tiledb_query_channel_t = out_ptr!();
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_query_get_default_channel(ctx, cquery, &mut default_channel)
        })?;

        let (agg_name, attr_name) = match agg_type {
            AggregateType::Count => {
                (cstring!("Count"), None)
            },
            AggregateType::NullCount => {
                if name.is_none() {
                    return Err(TileDBError::InvalidArgument(anyhow!("Sum aggregate should have an attribute to sum over.")))
                }
                (cstring!("NullCount"), Some(cstring!(name.unwrap().as_str())))
            },
            AggregateType::Sum => {
                if name.is_none() {
                    return Err(TileDBError::InvalidArgument(anyhow!("Sum aggregate should have an attribute to sum over.")))
                }
                (cstring!("Sum"), Some(cstring!(name.unwrap().as_str())))
            },
            AggregateType::Max => {
                if name.is_none() {
                    return Err(TileDBError::InvalidArgument(anyhow!("Max aggregate should have an attribute to max over.")))
                }
               ( cstring!("Max"), Some(cstring!(name.unwrap().as_str())))
            }, 
            AggregateType::Min => {
                if name.is_none() {
                    return Err(TileDBError::InvalidArgument(anyhow!("Min aggregate should have an attribute to min over.")))
                }
                (cstring!("Min"), Some(cstring!(name.unwrap().as_str())))
            },
            AggregateType::Mean => {
                if name.is_none() {
                    return Err(TileDBError::InvalidArgument(anyhow!("Mean aggregate should have an attribute to average over.")))
                }
                (cstring!("Mean"), Some(cstring!(name.unwrap().as_str())))
            }
        };

        // C API functionality
        let mut agg_operator : *const tiledb_channel_operator_t = out_ptr!();
        let mut agg_operation : *mut tiledb_channel_operation_t = out_ptr!();
        let c_agg_name = agg_name.as_c_str().as_ptr();

        if agg_type == AggregateType::Count {
            context.capi_call(|ctx| unsafe {
                ffi::tiledb_aggregate_count_get(ctx, core::ptr::addr_of_mut!(agg_operation) as *mut *const tiledb_channel_operation_t)
            })?;
        } else {
            let c_attr_name : *const i8 = attr_name.as_ref().unwrap().as_c_str().as_ptr();
            match agg_type {
                AggregateType::NullCount => {
                    context.capi_call(|ctx| unsafe {
                        ffi::tiledb_channel_operator_null_count_get(ctx, &mut agg_operator)
                    })?;
                }
                AggregateType::Sum => {
                    context.capi_call(|ctx| unsafe {
                        ffi::tiledb_channel_operator_sum_get(ctx, &mut agg_operator)
                    })?;
                },
                AggregateType::Max => {
                    context.capi_call(|ctx| unsafe {
                        ffi::tiledb_channel_operator_max_get(ctx, &mut agg_operator)
                    })?;
                }, 
                AggregateType::Min => {
                    context.capi_call(|ctx| unsafe {
                        ffi::tiledb_channel_operator_min_get(ctx, &mut agg_operator)
                    })?;
                },
                AggregateType::Mean => {
                    context.capi_call(|ctx| unsafe {
                        ffi::tiledb_channel_operator_mean_get(ctx, &mut agg_operator)
                    })?;
                },
                AggregateType::Count => unreachable!()
            };
            context.capi_call(|ctx| unsafe {
                ffi::tiledb_create_unary_aggregate(ctx, cquery, agg_operator, c_attr_name, &mut agg_operation)
            })?;
        }

        // Apply aggregate to the default channel.
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_channel_apply_aggregate(ctx, default_channel, c_agg_name, agg_operation)

        })?;

        Ok(AggregateBuilder::<Self, T> {
            base: self,
            agg_str: agg_name,
            attr_str: attr_name,
            attr_type: PhantomData,
        })

    }
}

pub struct ReadBuilder {
    base: BuilderBase,
}

impl ContextBound for ReadBuilder {
    fn context(&self) -> Context {
        self.base.context()
    }
}

impl ReadBuilder {
    pub fn new(array: Array) -> TileDBResult<Self> {
        let base = BuilderBase::new(array, QueryType::Read)?;

        /* configure the query to always use arrow-like output */
        {
            let mut config = Config::new()?;
            config.set("sm.var_offsets.bitsize", "64")?;
            config.set("sm.var_offsets.mode", "elements")?;
            config.set("sm.var_offsets.extra_element", "true")?;

            /*
             * TODO: make sure that users can't override this somehow,
             * else we will be very very sad
             */
            let c_query = **base.cquery();

            base.capi_call(|c_context| unsafe {
                ffi::tiledb_query_set_config(c_context, c_query, config.capi())
            })?;
        }

        Ok(ReadBuilder { base })
    }
}

impl QueryBuilder for ReadBuilder {
    type Query = QueryBase;

    fn base(&self) -> &BuilderBase {
        &self.base
    }

    fn build(self) -> Self::Query {
        self.base.build()
    }
}

impl<'data> ReadQueryBuilder<'data> for ReadBuilder {}

pub struct ReadQueryIterator<I, F> {
    query: Option<Box<dyn ReadQuery<Intermediate = I, Final = F>>>,
}

impl<I, F> Iterator for ReadQueryIterator<I, F> {
    type Item = TileDBResult<ReadStepOutput<I, F>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.query.take().map(|mut q| {
            q.step().inspect(|r| {
                if !r.is_final() {
                    self.query = Some(q);
                }
            })
        })
    }
}

impl<I, F> std::iter::FusedIterator for ReadQueryIterator<I, F> {}

impl AggregateBuilder for ReadBuilder {}
impl AggregateBuilderTrait for ReadBuilder {}
impl<B : QueryBuilder, T> AggregateBuilderTrait for AggregateBuilder<B, T> {}
