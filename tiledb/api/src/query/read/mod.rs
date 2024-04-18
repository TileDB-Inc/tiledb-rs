use super::*;

use std::cell::RefCell;
use std::pin::Pin;

use paste::paste;

use crate::convert::CAPISameRepr;
use crate::query::buffer::{BufferMut, QueryBuffersMut};
use crate::query::read::output::{HasScratchSpaceStrategy, ScratchAllocator};
use crate::Result as TileDBResult;

mod callback;
mod managed;
pub mod output;
mod raw;
mod typed;

pub use callback::*;
pub use managed::*;
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

/// Trait for runnable read queries.
pub trait ReadQuery<'ctx>: Query<'ctx> {
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
                    (&str, &'data RefCell<QueryBuffersMut<'data, <T as $Callback>::$U>>),
                )+
                callback: T
            ) -> TileDBResult<$Builder<'data, T, Self>>
            where
                Self: Sized,
                T: $Callback
            {
                let base = self;
                $(
                    let [< arg_ $U:snake >] = RawReadHandle::new(
                        [< field_ $U:snake >], [< scratch_ $U:snake >]);
                )+

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
pub trait ReadQueryBuilder<'ctx, 'data>: QueryBuilder<'ctx> {
    type IntoRaw: ReadQueryBuilder<'ctx, 'data>;
    type IntoVarRaw: ReadQueryBuilder<'ctx, 'data>;

    /// Register a raw memory location to write query results into.
    fn register_raw<S, C>(
        self,
        field: S,
        scratch: &'data RefCell<QueryBuffersMut<'data, C>>,
    ) -> TileDBResult<Self::IntoRaw>
    where
        Self: Sized,
        S: AsRef<str>,
        RawReadHandle<'data, C>: Into<TypedReadHandle<'data>>;

    fn register_var_raw<I>(self, fields: I) -> TileDBResult<Self::IntoVarRaw>
    where
        I: IntoIterator<Item = TypedReadHandle<'data>>;

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

    /// Register a callback to be run on query results.
    /// Scratch space for raw results is managed by the callback.
    fn register_callback_managed<S, T, C, A>(
        self,
        field: S,
        callback: T,
        scratch_allocator: A,
    ) -> TileDBResult<
        ManagedReadBuilder<'data, C, A, CallbackReadBuilder<'data, T, Self>>,
    >
    where
        Self: Sized,
        S: AsRef<str>,
        T: ReadCallback<Unit = C> + HasScratchSpaceStrategy<C, Strategy = A>,
        A: ScratchAllocator<C>,
    {
        let scratch = scratch_allocator.alloc();

        let scratch = QueryBuffersMut {
            data: BufferMut::Owned(scratch.0),
            cell_offsets: scratch.1.map(BufferMut::Owned),
            validity: scratch.2.map(BufferMut::Owned),
        };

        let scratch = Box::pin(RefCell::new(scratch));

        let base = {
            let scratch = scratch.as_ref().get_ref()
                as *const RefCell<
                    QueryBuffersMut<'data, <T as ReadCallback>::Unit>,
                >;
            let scratch = unsafe {
                &*scratch
                    as &'data RefCell<
                        QueryBuffersMut<'data, <T as ReadCallback>::Unit>,
                    >
            };
            self.register_callback((field.as_ref(), scratch), callback)
        }?;

        Ok(ManagedReadBuilder {
            alloc: scratch_allocator,
            scratch,
            base,
        })
    }

    /// Register a typed result to be constructed from the query results.
    /// Intermediate raw results are written into the provided scratch space.
    fn register_constructor<S, T>(
        self,
        field: S,
        scratch: &'data RefCell<
            QueryBuffersMut<
                'data,
                <<T as ReadResult>::Constructor as ReadCallback>::Unit,
            >,
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

    /// Register a typed result to be constructed from the query results.
    /// Scratch space for raw results is managed by the callback.
    fn register_constructor_managed<S, T, R, C, A>(
        self,
        field: S,
        scratch_allocator: A,
    ) -> TileDBResult<
        ManagedReadBuilder<'data, C, A, TypedReadBuilder<'data, T, Self>>,
    >
    where
        Self: Sized,
        S: AsRef<str>,
        T: ReadResult<Constructor = R>
            + HasScratchSpaceStrategy<C, Strategy = A>,
        R: Default + ReadCallback<Unit = C>,
        A: ScratchAllocator<C>,
    {
        let scratch = scratch_allocator.alloc();

        let scratch = QueryBuffersMut {
            data: BufferMut::Owned(scratch.0),
            cell_offsets: scratch.1.map(BufferMut::Owned),
            validity: scratch.2.map(BufferMut::Owned),
        };

        let scratch = Box::pin(RefCell::new(scratch));

        let base = {
            let scratch = scratch.as_ref().get_ref()
                as *const RefCell<QueryBuffersMut<'data, C>>;
            let scratch = unsafe {
                &*scratch as &'data RefCell<QueryBuffersMut<'data, C>>
            };
            self.register_constructor::<S, T>(field, scratch)
        }?;

        Ok(ManagedReadBuilder {
            alloc: scratch_allocator,
            scratch,
            base,
        })
    }
}

#[derive(ContextBound)]
pub struct ReadBuilder<'ctx> {
    #[base(ContextBound)]
    base: BuilderBase<'ctx>,
}

impl<'ctx> ReadBuilder<'ctx> {
    pub fn new(array: Array<'ctx>) -> TileDBResult<Self> {
        Ok(ReadBuilder {
            base: BuilderBase::new(array, QueryType::Read)?,
        })
    }
}

impl<'ctx> QueryBuilder<'ctx> for ReadBuilder<'ctx> {
    type Query = QueryBase<'ctx>;

    fn base(&self) -> &BuilderBase<'ctx> {
        &self.base
    }

    fn build(self) -> Self::Query {
        self.base.build()
    }
}

impl<'ctx, 'data> ReadQueryBuilder<'ctx, 'data> for ReadBuilder<'ctx> {
    type IntoRaw = RawReadBuilder<'data, Self>;
    type IntoVarRaw = VarRawReadBuilder<'data, Self>;

    /// Register a raw memory location to write query results into.
    fn register_raw<S, C>(
        self,
        field: S,
        scratch: &'data RefCell<QueryBuffersMut<'data, C>>,
    ) -> TileDBResult<Self::IntoRaw>
    where
        Self: Sized,
        S: AsRef<str>,
        RawReadHandle<'data, C>: Into<TypedReadHandle<'data>>,
    {
        Ok(RawReadBuilder {
            raw_read_output: RawReadHandle::new(field.as_ref(), scratch).into(),
            base: self,
        })
    }

    fn register_var_raw<I>(self, fields: I) -> TileDBResult<Self::IntoVarRaw>
    where
        I: IntoIterator<Item = TypedReadHandle<'data>>,
    {
        Ok(VarRawReadBuilder {
            raw_read_output: fields.into_iter().collect(),
            base: self,
        })
    }
}
