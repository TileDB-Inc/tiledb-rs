use super::*;

use std::cell::{RefCell, RefMut};
use std::pin::Pin;

use crate::convert::{
    Buffer, BufferMut, CAPISameRepr, DataReceiver, HasScratchSpaceStrategy,
    InputData, OutputLocation, ReadResult, ScratchAllocator,
};
use crate::query::private::QueryCAPIInterface;
use crate::Result as TileDBResult;

pub enum ReadStepOutput<I, F> {
    NotEnoughSpace,
    Intermediate(I),
    Final(F),
}

impl<I, F> ReadStepOutput<I, F> {
    pub fn is_intermediate(&self) -> bool {
        matches!(self, ReadStepOutput::Intermediate(_))
    }

    pub fn is_final(&self) -> bool {
        matches!(self, ReadStepOutput::Final(_))
    }
}

impl<U> ReadStepOutput<U, U> {
    pub fn unwrap(self) -> Option<U> {
        match self {
            ReadStepOutput::NotEnoughSpace => None,
            ReadStepOutput::Intermediate(i) => Some(i),
            ReadStepOutput::Final(f) => Some(f),
        }
    }
}

pub trait ReadQuery<'ctx>:
    ContextBound<'ctx> + QueryCAPIInterface + Sized
{
    type Intermediate;
    type Final;

    /// Run the query to fill scratch space.
    // TODO: how should this indicate "not enough space to write any data"?
    fn step(
        &mut self,
    ) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>>;

    /// Run the query to completion.
    /// Operations may be interleaved between individual steps
    /// of the query.
    fn execute(&mut self) -> TileDBResult<Self::Final> {
        Ok(loop {
            if let ReadStepOutput::Final(result) = self.step()? {
                break result;
            }
        })
    }
}

struct RawReadOutput<'data, C> {
    data_size: Pin<Box<u64>>,
    offsets_size: Option<Pin<Box<u64>>>,
    location: &'data RefCell<OutputLocation<'data, C>>,
    raw_dataptr: *const C,
    raw_offsetptr: Option<*const u64>,
}

impl<'data, C> RawReadOutput<'data, C> {
    fn new(location: &'data RefCell<OutputLocation<'data, C>>) -> Self {
        let (data, cell_offsets) = {
            let mut scratch: RefMut<OutputLocation<'data, C>> =
                location.borrow_mut();

            let data = scratch.data.as_mut() as *mut [C];
            let data = unsafe { &mut *data as &mut [C] };

            let cell_offsets = scratch.cell_offsets.as_mut().map(|c| {
                let c = c.as_mut() as *mut [u64];
                unsafe { &mut *c as &mut [u64] }
            });

            (data, cell_offsets)
        };

        let (data_size, offsets_size) = {
            (
                Box::pin(std::mem::size_of_val(&*data) as u64),
                cell_offsets.as_ref().map(|off| {
                    let sz = std::mem::size_of_val::<[u64]>(&*off);
                    Box::pin(sz as u64)
                }),
            )
        };

        let (raw_dataptr, raw_offsetptr) = {
            let location = location.borrow();
            (
                location.data.as_ptr(),
                location.cell_offsets.as_ref().map(|c| c.as_ptr()),
            )
        };

        RawReadOutput {
            data_size,
            offsets_size,
            location,
            raw_dataptr,
            raw_offsetptr,
        }
    }

    fn attach_query<S>(
        &mut self,
        context: &Context,
        c_query: *mut ffi::tiledb_query_t,
        field: &S,
    ) -> TileDBResult<()>
    where
        S: AsRef<str>,
    {
        let c_context = context.capi();
        let c_name = cstring!(field.as_ref());

        let mut location = self.location.borrow_mut();

        *self.data_size.as_mut() =
            std::mem::size_of_val::<[C]>(&location.data) as u64;

        context.capi_return({
            let data = &mut location.data;
            let c_bufptr = data.as_mut().as_ptr() as *mut std::ffi::c_void;
            let c_sizeptr = self.data_size.as_mut().get_mut() as *mut u64;

            unsafe {
                ffi::tiledb_query_set_data_buffer(
                    c_context,
                    c_query,
                    c_name.as_ptr(),
                    c_bufptr,
                    c_sizeptr,
                )
            }
        })?;

        let cell_offsets = &mut location.cell_offsets;

        if let Some(ref mut offsets_size) = self.offsets_size.as_mut() {
            let cell_offsets = cell_offsets.as_mut().unwrap();

            *offsets_size.as_mut() =
                std::mem::size_of_val::<[u64]>(cell_offsets) as u64;

            let c_offptr = cell_offsets.as_mut_ptr() as *mut u64;
            let c_sizeptr = offsets_size.as_mut().get_mut() as *mut u64;

            context.capi_return(unsafe {
                ffi::tiledb_query_set_offsets_buffer(
                    c_context,
                    c_query,
                    c_name.as_ptr(),
                    c_offptr,
                    c_sizeptr,
                )
            })?;
        }

        Ok(())
    }
}

pub struct RawReadQuery<'data, C, Q> {
    field: String,
    raw_read_output: RawReadOutput<'data, C>,
    base: Q,
}

impl<'ctx, 'data, C, Q> ContextBound<'ctx> for RawReadQuery<'data, C, Q>
where
    Q: ContextBound<'ctx>,
{
    fn context(&self) -> &'ctx Context {
        self.base.context()
    }
}

impl<'data, C, Q> QueryCAPIInterface for RawReadQuery<'data, C, Q>
where
    Q: QueryCAPIInterface,
{
    fn raw(&self) -> &RawQuery {
        self.base.raw()
    }
}

impl<'ctx, 'data, C, Q> ReadQuery<'ctx> for RawReadQuery<'data, C, Q>
where
    Q: ReadQuery<'ctx>,
{
    type Intermediate = (usize, usize, Q::Intermediate);
    type Final = (usize, usize, Q::Final);

    fn step(
        &mut self,
    ) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>> {
        /* update the internal buffers */
        self.raw_read_output.attach_query(
            self.context(),
            **self.raw(),
            &self.field,
        )?;

        /* then execute */
        let base_result = {
            let _ = self.raw_read_output.location.borrow_mut();
            self.base.step()?
        };

        let records_written = match self.raw_read_output.offsets_size.as_ref() {
            Some(offsets_size) => {
                **offsets_size as usize / std::mem::size_of::<u64>()
            }
            None => {
                *self.raw_read_output.data_size as usize
                    / std::mem::size_of::<C>()
            }
        };
        let bytes_written = *self.raw_read_output.data_size as usize;

        Ok(match base_result {
            ReadStepOutput::NotEnoughSpace => {
                /* TODO: check that records/bytes are zero and produce an internal error if not */
                ReadStepOutput::NotEnoughSpace
            }
            ReadStepOutput::Intermediate(base_result) => {
                if records_written == 0 && bytes_written == 0 {
                    ReadStepOutput::NotEnoughSpace
                } else if records_written == 0 || bytes_written == 0 {
                    /* TODO: internal error */
                    return Err(unimplemented!());
                } else {
                    ReadStepOutput::Intermediate((
                        records_written,
                        bytes_written,
                        base_result,
                    ))
                }
            }
            ReadStepOutput::Final(base_result) => ReadStepOutput::Final((
                records_written,
                bytes_written,
                base_result,
            )),
        })
    }
}

pub struct ManagedReadQuery<'data, C, A, Q> {
    alloc: A,
    scratch: Pin<Box<RefCell<OutputLocation<'data, C>>>>,
    base: Q,
}

impl<'ctx, 'data, C, A, Q> ContextBound<'ctx>
    for ManagedReadQuery<'data, C, A, Q>
where
    Q: ContextBound<'ctx>,
{
    fn context(&self) -> &'ctx Context {
        self.base.context()
    }
}

impl<'data, C, A, Q> QueryCAPIInterface for ManagedReadQuery<'data, C, A, Q>
where
    Q: QueryCAPIInterface,
{
    fn raw(&self) -> &RawQuery {
        self.base.raw()
    }
}

impl<'ctx, 'data, C, A, Q> ReadQuery<'ctx> for ManagedReadQuery<'data, C, A, Q>
where
    Q: ReadQuery<'ctx>,
{
    type Intermediate = Q::Intermediate;
    type Final = Q::Final;

    /// Run the query until it fills the scratch space.
    /// Invokes the callback on all data in the scratch space when the query returns.
    fn step(
        &mut self,
    ) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>> {
        self.base.step()
    }
}

pub struct CallbackReadQuery<'data, T, Q>
where
    T: DataReceiver,
{
    receiver: T,
    base: RawReadQuery<'data, T::Unit, Q>,
}

impl<'ctx, 'data, T, Q> ContextBound<'ctx> for CallbackReadQuery<'data, T, Q>
where
    Q: ContextBound<'ctx>,
    T: DataReceiver,
{
    fn context(&self) -> &'ctx Context {
        self.base.context()
    }
}

impl<'data, T, Q> QueryCAPIInterface for CallbackReadQuery<'data, T, Q>
where
    Q: QueryCAPIInterface,
    T: DataReceiver,
{
    fn raw(&self) -> &RawQuery {
        self.base.raw()
    }
}

impl<'ctx, 'data, T, Q> ReadQuery<'ctx> for CallbackReadQuery<'data, T, Q>
where
    T: DataReceiver,
    Q: ReadQuery<'ctx>,
{
    type Intermediate = Q::Intermediate;
    type Final = Q::Final;

    /// Run the query until it fills the scratch space.
    /// Invokes the callback on all data in the scratch space when the query returns.
    fn step(
        &mut self,
    ) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>> {
        let base_result = self.base.step()?;

        let records_written =
            match self.base.raw_read_output.offsets_size.as_ref() {
                Some(offsets_size) => {
                    **offsets_size as usize / std::mem::size_of::<u64>()
                }
                None => {
                    *self.base.raw_read_output.data_size as usize
                        / std::mem::size_of::<<T as DataReceiver>::Unit>()
                }
            };
        let bytes_written = *self.base.raw_read_output.data_size as usize;

        let location = self.base.raw_read_output.location.borrow();

        /* TODO: check status and invoke callback with either borrowed or owned buffer */
        let input_data = InputData {
            data: Buffer::Borrowed(&*location.data),
            cell_offsets: location
                .cell_offsets
                .as_ref()
                .map(|c| Buffer::Borrowed(&*c)),
        };

        self.receiver
            .receive(records_written, bytes_written, input_data)?;

        Ok(match base_result {
            ReadStepOutput::NotEnoughSpace => ReadStepOutput::NotEnoughSpace,
            ReadStepOutput::Intermediate((_, _, base_result)) => {
                ReadStepOutput::Intermediate(base_result)
            }
            ReadStepOutput::Final((_, _, base_result)) => {
                ReadStepOutput::Final(base_result)
            }
        })
    }
}

pub struct TypedReadQuery<'data, T, Q>
where
    T: ReadResult,
{
    _marker: std::marker::PhantomData<T>,
    base: CallbackReadQuery<'data, <T as ReadResult>::Receiver, Q>,
}

impl<'ctx, 'data, T, Q> ContextBound<'ctx> for TypedReadQuery<'data, T, Q>
where
    Q: ContextBound<'ctx>,
    T: ReadResult,
{
    fn context(&self) -> &'ctx Context {
        self.base.context()
    }
}

impl<'data, T, Q> QueryCAPIInterface for TypedReadQuery<'data, T, Q>
where
    Q: QueryCAPIInterface,
    T: ReadResult,
{
    fn raw(&self) -> &RawQuery {
        self.base.raw()
    }
}

impl<'ctx, 'data, T, Q> ReadQuery<'ctx> for TypedReadQuery<'data, T, Q>
where
    T: ReadResult,
    Q: ReadQuery<'ctx>,
{
    type Intermediate = Q::Intermediate;
    type Final = (T, Q::Final);

    fn step(
        &mut self,
    ) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>> {
        let base_result = self.base.step()?;
        Ok(match base_result {
            ReadStepOutput::NotEnoughSpace => ReadStepOutput::NotEnoughSpace,
            ReadStepOutput::Intermediate(i) => ReadStepOutput::Intermediate(i),
            ReadStepOutput::Final(f) => {
                let my_result = std::mem::replace(
                    &mut self.base.receiver,
                    T::new_receiver(),
                )
                .into();
                ReadStepOutput::Final((my_result, f))
            }
        })
    }
}

pub trait ReadQueryBuilder<'ctx>: Sized + QueryBuilder<'ctx> {
    fn register_raw<'data, S, C>(
        self,
        field: S,
        scratch: &'data RefCell<OutputLocation<'data, C>>,
    ) -> TileDBResult<RawReadBuilder<C, Self>>
    where
        S: AsRef<str>,
        C: CAPISameRepr,
    {
        Ok(RawReadBuilder {
            field: field.as_ref().to_string(),
            raw_read_output: RawReadOutput::new(scratch),
            base: self,
        })
    }

    fn register_callback<'data, S, T>(
        self,
        field: S,
        callback: T,
        scratch: &'data RefCell<
            OutputLocation<'data, <T as DataReceiver>::Unit>,
        >,
    ) -> TileDBResult<CallbackReadBuilder<T, Self>>
    where
        S: AsRef<str>,
        T: DataReceiver,
    {
        let base = self.register_raw(field, scratch)?;

        Ok(CallbackReadBuilder { callback, base })
    }

    fn register_callback_managed<'data, S, T, C>(
        self,
        field: S,
        callback: T,
        params: <<T as HasScratchSpaceStrategy<C>>::Strategy as ScratchAllocator<C>>::Parameters,
    ) -> TileDBResult<
        ManagedReadBuilder<
            'data,
            C,
            <T as HasScratchSpaceStrategy<C>>::Strategy,
            CallbackReadBuilder<'data, T, Self>,
        >,
    >
    where
        S: AsRef<str>,
        T: DataReceiver<Unit = C> + HasScratchSpaceStrategy<C>,
    {
        let a =
            <<T as HasScratchSpaceStrategy<C>>::Strategy as ScratchAllocator<
                C,
            >>::construct(params);
        let scratch = a.scratch_space();

        let scratch = OutputLocation {
            data: BufferMut::Owned(scratch.0),
            cell_offsets: scratch.1.map(|c| BufferMut::Owned(c)),
        };

        let scratch = Box::pin(RefCell::new(scratch));

        let base = {
            let scratch = scratch.as_ref().get_ref()
                as *const RefCell<
                    OutputLocation<'data, <T as DataReceiver>::Unit>,
                >;
            let scratch = unsafe {
                &*scratch
                    as &'data RefCell<
                        OutputLocation<'data, <T as DataReceiver>::Unit>,
                    >
            };
            self.register_callback(field, callback, scratch)
        }?;

        Ok(ManagedReadBuilder {
            alloc: a,
            scratch,
            base,
        })
    }

    fn add_result<'data, S, T>(
        self,
        field: S,
        scratch: &'data RefCell<
            OutputLocation<
                'data,
                <<T as ReadResult>::Receiver as DataReceiver>::Unit,
            >,
        >,
    ) -> TileDBResult<TypedReadBuilder<'data, T, Self>>
    where
        S: AsRef<str>,
        T: ReadResult,
    {
        let r = T::new_receiver();
        Ok(TypedReadBuilder {
            _marker: std::marker::PhantomData,
            base: self.register_callback(field, r, scratch)?,
        })
    }

    fn add_result_managed<'data, S, T, R, C>(
        self,
        field: S,
        params: <<T as HasScratchSpaceStrategy<C>>::Strategy as ScratchAllocator<C>>::Parameters,
    ) -> TileDBResult<
        ManagedReadBuilder<
            'data,
            C,
            <T as HasScratchSpaceStrategy<C>>::Strategy,
            TypedReadBuilder<'data, T, Self>,
        >,
    >
    where
        S: AsRef<str>,
        T: ReadResult<Receiver = R> + HasScratchSpaceStrategy<C>,
        R: DataReceiver<Unit = C>,
    {
        let a =
            <<T as HasScratchSpaceStrategy<C>>::Strategy as ScratchAllocator<
                C,
            >>::construct(params);
        let scratch = a.scratch_space();

        let scratch = OutputLocation {
            data: BufferMut::Owned(scratch.0),
            cell_offsets: scratch.1.map(|c| BufferMut::Owned(c)),
        };

        let scratch = Box::pin(RefCell::new(scratch));

        let base = {
            let scratch = scratch.as_ref().get_ref()
                as *const RefCell<OutputLocation<'data, C>>;
            let scratch = unsafe {
                &*scratch as &'data RefCell<OutputLocation<'data, C>>
            };
            self.add_result::<S, T>(field, scratch)
        }?;

        Ok(ManagedReadBuilder {
            alloc: a,
            scratch,
            base,
        })
    }
}

pub struct RawReadBuilder<'data, C, B> {
    field: String,
    raw_read_output: RawReadOutput<'data, C>,
    base: B,
}

impl<'ctx, 'data, C, B> ContextBound<'ctx> for RawReadBuilder<'data, C, B>
where
    B: ContextBound<'ctx>,
{
    fn context(&self) -> &'ctx Context {
        self.base.context()
    }
}

impl<'data, C, B> crate::query::private::QueryCAPIInterface
    for RawReadBuilder<'data, C, B>
where
    B: crate::query::private::QueryCAPIInterface,
{
    fn raw(&self) -> &RawQuery {
        self.base.raw()
    }
}

impl<'ctx, 'data, C, B> QueryBuilder<'ctx> for RawReadBuilder<'data, C, B>
where
    B: QueryBuilder<'ctx>,
{
    type Query = RawReadQuery<'data, C, B::Query>;

    fn array(&self) -> &Array {
        self.base.array()
    }

    fn build(self) -> Self::Query {
        RawReadQuery {
            field: self.field,
            raw_read_output: self.raw_read_output,
            base: self.base.build(),
        }
    }
}

impl<'ctx, 'data, C, B> ReadQueryBuilder<'ctx> for RawReadBuilder<'data, C, B> where
    B: ReadQueryBuilder<'ctx>
{
}

pub struct ManagedReadBuilder<'data, C, A, B> {
    alloc: A,
    scratch: Pin<Box<RefCell<OutputLocation<'data, C>>>>,
    base: B,
}

impl<'ctx, 'data, C, A, B> ContextBound<'ctx>
    for ManagedReadBuilder<'data, C, A, B>
where
    B: ContextBound<'ctx>,
{
    fn context(&self) -> &'ctx Context {
        self.base.context()
    }
}

impl<'data, C, A, B> crate::query::private::QueryCAPIInterface
    for ManagedReadBuilder<'data, C, A, B>
where
    B: crate::query::private::QueryCAPIInterface,
{
    fn raw(&self) -> &RawQuery {
        self.base.raw()
    }
}

impl<'ctx, 'data, C, A, B> QueryBuilder<'ctx>
    for ManagedReadBuilder<'data, C, A, B>
where
    B: QueryBuilder<'ctx>,
{
    type Query = ManagedReadQuery<'data, C, A, B::Query>;

    fn array(&self) -> &Array {
        self.base.array()
    }

    fn build(self) -> Self::Query {
        ManagedReadQuery {
            alloc: self.alloc,
            scratch: self.scratch,
            base: self.base.build(),
        }
    }
}

impl<'ctx, 'data, C, A, B> ReadQueryBuilder<'ctx>
    for ManagedReadBuilder<'data, C, A, B>
where
    B: ReadQueryBuilder<'ctx>,
{
}

pub struct CallbackReadBuilder<'data, T, B>
where
    T: DataReceiver,
{
    callback: T,
    base: RawReadBuilder<'data, <T as DataReceiver>::Unit, B>,
}

impl<'ctx, 'data, T, B> ContextBound<'ctx> for CallbackReadBuilder<'data, T, B>
where
    T: DataReceiver,
    B: ContextBound<'ctx>,
{
    fn context(&self) -> &'ctx Context {
        self.base.context()
    }
}

impl<'data, T, B> crate::query::private::QueryCAPIInterface
    for CallbackReadBuilder<'data, T, B>
where
    T: DataReceiver,
    B: crate::query::private::QueryCAPIInterface,
{
    fn raw(&self) -> &RawQuery {
        self.base.raw()
    }
}

impl<'ctx, 'data, T, B> QueryBuilder<'ctx> for CallbackReadBuilder<'data, T, B>
where
    T: DataReceiver,
    B: QueryBuilder<'ctx>,
{
    type Query = CallbackReadQuery<'data, T, B::Query>;

    fn array(&self) -> &Array {
        self.base.array()
    }

    fn build(self) -> Self::Query {
        CallbackReadQuery {
            receiver: self.callback,
            base: self.base.build(),
        }
    }
}

impl<'ctx, 'data, T, B> ReadQueryBuilder<'ctx>
    for CallbackReadBuilder<'data, T, B>
where
    T: DataReceiver,
    B: ReadQueryBuilder<'ctx>,
{
}

pub struct TypedReadBuilder<'data, T, B>
where
    T: ReadResult,
{
    _marker: std::marker::PhantomData<T>,
    base: CallbackReadBuilder<'data, <T as ReadResult>::Receiver, B>,
}

impl<'ctx, 'data, T, B> ContextBound<'ctx> for TypedReadBuilder<'data, T, B>
where
    T: ReadResult,
    B: ContextBound<'ctx>,
{
    fn context(&self) -> &'ctx Context {
        self.base.context()
    }
}

impl<'data, T, B> crate::query::private::QueryCAPIInterface
    for TypedReadBuilder<'data, T, B>
where
    T: ReadResult,
    B: crate::query::private::QueryCAPIInterface,
{
    fn raw(&self) -> &RawQuery {
        self.base.raw()
    }
}

impl<'ctx, 'data, T, B> QueryBuilder<'ctx> for TypedReadBuilder<'data, T, B>
where
    T: ReadResult,
    B: QueryBuilder<'ctx>,
{
    type Query = TypedReadQuery<'data, T, B::Query>;

    fn array(&self) -> &Array {
        self.base.array()
    }

    fn build(self) -> Self::Query {
        TypedReadQuery {
            _marker: self._marker,
            base: self.base.build(),
        }
    }
}

impl<'ctx, 'data, T, B> ReadQueryBuilder<'ctx> for TypedReadBuilder<'data, T, B>
where
    T: ReadResult,
    B: ReadQueryBuilder<'ctx>,
{
}

pub struct ReadBuilder<'ctx> {
    base: BuilderBase<'ctx>,
}

impl<'ctx> ReadBuilder<'ctx> {
    pub fn new(
        context: &'ctx Context,
        array: Array<'ctx>,
    ) -> TileDBResult<Self> {
        Ok(ReadBuilder {
            base: BuilderBase::new(context, array, QueryType::Read)?,
        })
    }
}

impl<'ctx> ContextBound<'ctx> for ReadBuilder<'ctx> {
    fn context(&self) -> &'ctx Context {
        self.base.context()
    }
}

impl<'ctx> private::QueryCAPIInterface for ReadBuilder<'ctx> {
    fn raw(&self) -> &RawQuery {
        &self.base.raw()
    }
}

impl<'ctx> QueryBuilder<'ctx> for ReadBuilder<'ctx> {
    type Query = Query<'ctx>;

    fn array(&self) -> &Array {
        &self.base.array()
    }

    fn build(self) -> Self::Query {
        self.base.build()
    }
}

impl<'ctx> ReadQueryBuilder<'ctx> for ReadBuilder<'ctx> {}
