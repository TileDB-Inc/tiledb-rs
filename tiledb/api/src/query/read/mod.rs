use super::*;

use std::cell::RefCell;
use std::pin::Pin;

use ffi::{tiledb_channel_operation_t, tiledb_query_channel_t};
use paste::paste;

use crate::config::Config;
use crate::query::buffer::{BufferMut, QueryBuffersMut};
use crate::query::read::output::ScratchAllocator;
use crate::Result as TileDBResult;

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

pub struct CountReader {
    query : QueryBase
}

impl Query for CountReader {
    fn base(&self) -> &QueryBase {
        &self.query
    }

    fn finalize(self) -> TileDBResult<Array>
        where
            Self: Sized {
        self.query.finalize()
    }
}

impl ReadQuery for CountReader {
    type Intermediate = ();
    type Final = u64;

    fn step(
        &mut self,
    ) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>> {
        // Register the data buffer (set data buffer)
        let context = self.base().context();
        let cquery = **self.base().cquery();
        let location : *mut u64 = out_ptr!();
        let size : *mut u64 = out_ptr!();
        unsafe {
            *size = 8;
        }

        let c_bufptr = location as *mut std::ffi::c_void;
        let c_sizeptr = size as *mut u64;
        let count_str = String::from("Count");
        let count_c_ptr = count_str.as_ptr() as *const i8;

        context.capi_call(|ctx| unsafe {
            ffi::tiledb_query_set_data_buffer(
                ctx,
                cquery,
                count_c_ptr,
                c_bufptr,
                c_sizeptr,
            )
        })?;

        let base_result = self.base().step()?;
        
        // Run the query in a loop until you get the final result
        let return_val = unsafe {match base_result {
            ReadStepOutput::Final(_) => *location,
            ReadStepOutput::Intermediate(()) => unreachable!(),
            ReadStepOutput::NotEnoughSpace => unreachable!(),
        }};

        Ok(ReadStepOutput::Final(return_val))

    }
}

pub enum AggregateType {
    Count,
    Sum
    // agg_type : AggregateType , attr_name : Option<String>
}

pub trait AggregateBuilder : QueryBuilder {
    fn apply_aggregate(self) -> TileDBResult<CountReader> {
        // Put aggregate C API functions here (channel initialization and setup)
        // So far only count
        let context = self.base().context();
        let cquery = **self.base().cquery();
        let mut default_channel : *mut tiledb_query_channel_t = out_ptr!();
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_query_get_default_channel(ctx, cquery, &mut default_channel)
        })?;

        let mut count_agg : *const tiledb_channel_operation_t = out_ptr!();
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_aggregate_count_get(ctx, &mut count_agg)
        })?;

        let count = String::from("Count");
        let ccount = cstring!(count).as_c_str().as_ptr();

        context.capi_call(|ctx| unsafe {
            ffi::tiledb_channel_apply_aggregate(ctx, default_channel, ccount, count_agg)

        })?;

        Ok(CountReader{
            query: self.base().query
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
