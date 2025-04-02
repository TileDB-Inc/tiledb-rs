use super::*;

use std::cell::RefMut;

use crate::Datatype;
use crate::array::CellValNum;
use crate::array::schema::Field;
use crate::error::Error;
use crate::query::Query;
use crate::query::buffer::{
    CellStructureMut, QueryBuffersMut, RefTypedQueryBuffersMut,
};
use crate::query::read::output::ScratchSpace;

pub struct ManagedBuffer<'data, C> {
    pub buffers: Pin<Box<RefCell<QueryBuffersMut<'data, C>>>>,
    pub allocator: Box<dyn ScratchAllocator<C> + 'data>,
}

impl<'data, C> ManagedBuffer<'data, C> {
    pub fn new<A>(allocator: A) -> Self
    where
        A: ScratchAllocator<C> + 'data,
    {
        let allocator: Box<dyn ScratchAllocator<C> + 'data> =
            Box::new(allocator);
        ManagedBuffer::from(allocator)
    }

    pub fn realloc(&self) {
        let old_scratch = {
            let tmp = QueryBuffersMut {
                data: BufferMut::Empty,
                cell_structure: CellStructureMut::Var(BufferMut::Empty),
                validity: None,
            };
            ScratchSpace::<C>::try_from(self.buffers.replace(tmp))
                .expect("ManagedBuffer cannot have a borrowed output location")
        };

        let new_scratch = self.allocator.realloc(old_scratch);
        let _ = self.buffers.replace(QueryBuffersMut::from(new_scratch));
    }
}

impl<'data, C> From<Box<dyn ScratchAllocator<C> + 'data>>
    for ManagedBuffer<'data, C>
{
    fn from(allocator: Box<dyn ScratchAllocator<C> + 'data>) -> Self {
        let buffers = Box::pin(RefCell::new(allocator.alloc().into()));
        ManagedBuffer { buffers, allocator }
    }
}

/// Metadata providing additional context for a field in a read query
pub struct FieldMetadata {
    pub name: String,
    pub datatype: Datatype,
    pub cell_val_num: CellValNum,
}

impl TryFrom<&Field> for FieldMetadata {
    type Error = Error;
    fn try_from(value: &Field) -> TileDBResult<Self> {
        Ok(FieldMetadata {
            name: value.name()?,
            datatype: value.datatype()?,
            cell_val_num: value.cell_val_num()?,
        })
    }
}

/// Encapsulates data for writing intermediate query results for a data field.
pub struct RawReadHandle<'data, C> {
    /// Metadata describing the field which this handle receives data from
    pub field: FieldMetadata,

    /// As input to the C API, the size of the data buffer.
    /// As output from the C API, the size in bytes of an intermediate result.
    pub data_size: Pin<Box<u64>>,

    /// As input to the C API, the size of the cell offsets buffer.
    /// As output from the C API, the size in bytes of intermediate offset results.
    pub offsets_size: Option<Pin<Box<u64>>>,

    /// As input to the C API, the size of the validity buffer.
    /// As output from the C API, the size in bytes of validity results.
    pub validity_size: Option<Pin<Box<u64>>>,

    /// Buffers for writing data and cell offsets.
    /// These are re-registered with the query at each step.
    /// The application which owns the query may own these buffers,
    /// or defer their management to the reader.
    // In the case of the former, the application can do whatever it wants with the
    // buffers between steps of a query.
    // RefCell is used so that the query can write to the buffers when it is executing
    // but the application can do whatever with the buffers between steps.
    pub location: &'data RefCell<QueryBuffersMut<'data, C>>,

    /// Optional allocator for query buffers wrapped by this handle.
    pub managed_buffer: Option<ManagedBuffer<'data, C>>,
}

impl<'data, C> RawReadHandle<'data, C> {
    pub fn new(
        field: FieldMetadata,
        location: &'data RefCell<QueryBuffersMut<'data, C>>,
    ) -> Self {
        let (data, cell_offsets, validity) = {
            let mut scratch: RefMut<QueryBuffersMut<'data, C>> =
                location.borrow_mut();

            let data = scratch.data.as_mut() as *mut [C];
            let data = unsafe { &mut *data as &mut [C] };

            let cell_offsets = scratch.cell_structure.offsets_mut().map(|c| {
                let c = c.as_mut() as *mut [u64];
                unsafe { &mut *c as &mut [u64] }
            });

            let validity = scratch.validity.as_mut().map(|v| {
                let v = v.as_mut() as *mut [u8];
                unsafe { &mut *v as &mut [u8] }
            });

            (data, cell_offsets, validity)
        };

        let data_size = Box::pin(std::mem::size_of_val(&*data) as u64);

        let offsets_size = cell_offsets.as_ref().map(|off| {
            let sz = std::mem::size_of_val::<[u64]>(*off);
            Box::pin(sz as u64)
        });

        let validity_size = validity.as_ref().map(|val| {
            let sz = std::mem::size_of_val::<[u8]>(*val);
            Box::pin(sz as u64)
        });

        RawReadHandle {
            field,
            data_size,
            offsets_size,
            validity_size,
            location,
            managed_buffer: None,
        }
    }

    pub fn managed(
        field: FieldMetadata,
        managed: ManagedBuffer<'data, C>,
    ) -> Self {
        let qb = {
            let qb: Pin<&RefCell<QueryBuffersMut<'data, C>>> =
                managed.buffers.as_ref();
            let qb: &RefCell<QueryBuffersMut<'data, C>> = qb.get_ref();
            let qb = qb as *const RefCell<QueryBuffersMut<'data, C>>;

            /*
             * RawReadHandle keeps a reference to the RefCell which owns the buffers.
             * If the user owns that RefCell, then this is fine.
             * But when the user wants the query to manage the buffer instead,
             * then the read handle shall also own the RefCell - this means that
             * the RawReadHandle is self-referential, which is not sound in the
             * general case because moving `self` would invalidate a reference.
             * In this case, it is safe, because we Pin<Box> the RefCell,
             * which prevents the pointee from moving around in memory.
             */
            unsafe { &*qb as &'data RefCell<QueryBuffersMut<'data, C>> }
        };

        let r = RawReadHandle::new(field, qb);
        RawReadHandle {
            managed_buffer: Some(managed),
            ..r
        }
    }

    pub(crate) fn attach_query(
        &mut self,
        context: &Context,
        c_query: *mut ffi::tiledb_query_t,
    ) -> TileDBResult<()> {
        let c_name = cstring!(&*self.field.name);

        let mut location = self.location.borrow_mut();

        *self.data_size.as_mut() =
            std::mem::size_of_val::<[C]>(&location.data) as u64;

        let data = &mut location.data;
        let c_bufptr = data.as_mut().as_ptr() as *mut std::ffi::c_void;
        let c_sizeptr = self.data_size.as_mut().get_mut() as *mut u64;

        context.capi_call(|ctx| unsafe {
            ffi::tiledb_query_set_data_buffer(
                ctx,
                c_query,
                c_name.as_ptr(),
                c_bufptr,
                c_sizeptr,
            )
        })?;

        if let Some(ref mut offsets_size) = self.offsets_size.as_mut() {
            let cell_offsets = location.cell_structure.offsets_mut();
            let cell_offsets = cell_offsets.unwrap();

            *offsets_size.as_mut() =
                std::mem::size_of_val::<[u64]>(cell_offsets) as u64;

            let c_offptr = cell_offsets.as_mut_ptr();
            let c_sizeptr = offsets_size.as_mut().get_mut() as *mut u64;

            context.capi_call(|ctx| unsafe {
                ffi::tiledb_query_set_offsets_buffer(
                    ctx,
                    c_query,
                    c_name.as_ptr(),
                    c_offptr,
                    c_sizeptr,
                )
            })?;
        }

        let validity = &mut location.validity;

        if let Some(ref mut validity_size) = self.validity_size.as_mut() {
            let validity = validity.as_mut().unwrap();

            *validity_size.as_mut() =
                std::mem::size_of_val::<[u8]>(validity) as u64;

            let c_validityptr = validity.as_mut_ptr();
            let c_sizeptr = validity_size.as_mut().get_mut() as *mut u64;

            context.capi_call(|ctx| unsafe {
                ffi::tiledb_query_set_validity_buffer(
                    ctx,
                    c_query,
                    c_name.as_ptr(),
                    c_validityptr,
                    c_sizeptr,
                )
            })?;
        }

        Ok(())
    }

    /// Returns the number of cells produced by the last read,
    /// or the capacity of the destination buffers if no read has occurred.
    pub fn last_read_ncells(&self) -> usize {
        match self.field.cell_val_num {
            CellValNum::Fixed(nz) => {
                let nz = nz.get() as usize;

                assert!(self.offsets_size.is_none());
                let data_size = *self.data_size as usize;
                let nvalues = data_size / nz / std::mem::size_of::<C>();

                // assumption: core gives us an integral number of cells
                assert_eq!(data_size, nvalues * nz * std::mem::size_of::<C>());
                nvalues
            }
            CellValNum::Var => {
                let offsets_size =
                    **self.offsets_size.as_ref().unwrap() as usize;
                let noffsets = offsets_size / std::mem::size_of::<u64>();

                // assumption: core isn't lying about giving us u64 offsets
                assert_eq!(offsets_size, noffsets * std::mem::size_of::<u64>());

                /*
                 * We use the "extra_offsets" mode.
                 * Note that the core does not add a zero offset if
                 * the result set is empty.
                 */
                if noffsets == 0 { 0 } else { noffsets - 1 }
            }
        }
    }

    pub fn realloc_if_managed(&mut self) {
        if let Some(managed_buffer) = self.managed_buffer.as_ref() {
            managed_buffer.realloc();
        }
    }
}

pub enum TypedReadHandle<'data> {
    UInt8(RawReadHandle<'data, u8>),
    UInt16(RawReadHandle<'data, u16>),
    UInt32(RawReadHandle<'data, u32>),
    UInt64(RawReadHandle<'data, u64>),
    Int8(RawReadHandle<'data, i8>),
    Int16(RawReadHandle<'data, i16>),
    Int32(RawReadHandle<'data, i32>),
    Int64(RawReadHandle<'data, i64>),
    Float32(RawReadHandle<'data, f32>),
    Float64(RawReadHandle<'data, f64>),
}
macro_rules! typed_read_handle_go {
    ($expr:expr, $DT:ident, $inner:pat, $then:expr) => {
        match $expr {
            TypedReadHandle::UInt8($inner) => {
                type $DT = u8;
                $then
            }
            TypedReadHandle::UInt16($inner) => {
                type $DT = u16;
                $then
            }
            TypedReadHandle::UInt32($inner) => {
                type $DT = u32;
                $then
            }
            TypedReadHandle::UInt64($inner) => {
                type $DT = u64;
                $then
            }
            TypedReadHandle::Int8($inner) => {
                type $DT = i8;
                $then
            }
            TypedReadHandle::Int16($inner) => {
                type $DT = i16;
                $then
            }
            TypedReadHandle::Int32($inner) => {
                type $DT = i32;
                $then
            }
            TypedReadHandle::Int64($inner) => {
                type $DT = i64;
                $then
            }
            TypedReadHandle::Float32($inner) => {
                type $DT = f32;
                $then
            }
            TypedReadHandle::Float64($inner) => {
                type $DT = f64;
                $then
            }
        }
    };
}

impl<'data> TypedReadHandle<'data> {
    pub fn field(&self) -> &FieldMetadata {
        typed_read_handle_go!(self, _DT, handle, &handle.field)
    }

    pub fn attach_query(
        &mut self,
        context: &Context,
        query: *mut ffi::tiledb_query_t,
    ) -> TileDBResult<()> {
        typed_read_handle_go!(
            self,
            _DT,
            handle,
            handle.attach_query(context, query)
        )
    }

    pub fn last_read_ncells(&self) -> usize {
        typed_read_handle_go!(self, _DT, handle, handle.last_read_ncells())
    }

    pub fn borrow_mut<'this>(
        &'this self,
    ) -> RefTypedQueryBuffersMut<'this, 'data> {
        typed_read_handle_go!(self, _DT, handle, {
            RefTypedQueryBuffersMut::from(handle.location.borrow())
        })
    }

    pub fn realloc_if_managed(&mut self) {
        typed_read_handle_go!(self, _DT, handle, handle.realloc_if_managed());
    }
}

macro_rules! typed_read_handle {
    ($($V:ident : $U:ty),+) => {
        $(
            impl<'data> From<RawReadHandle<'data, $U>> for TypedReadHandle<'data> {
                fn from(value: RawReadHandle<'data, $U>) -> Self {
                    TypedReadHandle::$V(value)
                }
            }

            impl<'data> TryFrom<TypedReadHandle<'data>> for RawReadHandle<'data, $U> {
                type Error = ();
                fn try_from(value: TypedReadHandle<'data>) -> std::result::Result<Self, Self::Error> {
                    if let TypedReadHandle::$V(d) = value {
                        Ok(d)
                    } else {
                        Err(())
                    }
                }
            }

            impl<'data, 'this> TryFrom<&'this TypedReadHandle<'data>> for &'this RawReadHandle<'data, $U> {
                type Error = ();
                fn try_from(value: &'this TypedReadHandle<'data>) -> std::result::Result<Self, Self::Error> {
                    if let TypedReadHandle::$V(d) = value {
                        Ok(d)
                    } else {
                        Err(())
                    }
                }
            }
        )+
    }
}

typed_read_handle!(UInt8: u8, UInt16: u16, UInt32: u32, UInt64: u64);
typed_read_handle!(Int8: i8, Int16: i16, Int32: i32, Int64: i64);
typed_read_handle!(Float32: f32, Float64: f64);

/// Reads query results into a raw buffer.
///
/// This is the most flexible way to read data but also the most cumbersome.
/// Recommended usage is to run the query one step at a time, and borrow
/// the buffers between each step to process intermediate results.
pub struct RawReadQuery<'data, Q> {
    pub(crate) raw_read_output: TypedReadHandle<'data>,
    pub(crate) base: Q,
}

impl<Q> ContextBound for RawReadQuery<'_, Q>
where
    Q: ContextBound,
{
    fn context(&self) -> Context {
        self.base.context()
    }
}

impl<Q> Query for RawReadQuery<'_, Q>
where
    Q: Query,
{
    fn base(&self) -> &QueryBase {
        self.base.base()
    }

    fn finalize(self) -> TileDBResult<Array> {
        self.base.finalize()
    }
}

impl<Q> ReadQuery for RawReadQuery<'_, Q>
where
    Q: ReadQuery + ContextBound,
{
    type Intermediate = (usize, Q::Intermediate);
    type Final = (usize, Q::Final);

    fn step(
        &mut self,
    ) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>> {
        /* update the internal buffers */
        self.raw_read_output
            .attach_query(&self.base().context(), **self.base().cquery())?;

        /* then execute */
        let base_result = {
            let _ = self.raw_read_output.borrow_mut();
            self.base.step()?
        };

        let ncells = self.raw_read_output.last_read_ncells();

        Ok(match base_result {
            ReadStepOutput::NotEnoughSpace => {
                /* realloc any self-managed buffers */
                self.raw_read_output.realloc_if_managed();

                /* TODO: check that records/bytes are zero and produce an internal error if not */
                ReadStepOutput::NotEnoughSpace
            }
            ReadStepOutput::Intermediate(base_result) => {
                if ncells == 0 {
                    /*
                     * The input produced no data.
                     * The returned status itself is not enough to distinguish between
                     * "no results, allocate more space plz" and "there are more results after you consume these".
                     * The API tiledb_query_get_status_details exists but is experimental,
                     * so we will worry about it later.  For now, assume this is the first
                     * raw read and it is our responsibility to signal NotEnoughSpace.
                     */
                    ReadStepOutput::NotEnoughSpace
                } else {
                    ReadStepOutput::Intermediate((ncells, base_result))
                }
            }
            ReadStepOutput::Final(base_result) => {
                ReadStepOutput::Final((ncells, base_result))
            }
        })
    }
}

pub struct RawReadBuilder<'data, B> {
    pub(crate) raw_read_output: TypedReadHandle<'data>,
    pub(crate) base: B,
}

impl<B> ContextBound for RawReadBuilder<'_, B>
where
    B: QueryBuilder,
{
    fn context(&self) -> Context {
        self.base.base().context()
    }
}

impl<'data, B> QueryBuilder for RawReadBuilder<'data, B>
where
    B: QueryBuilder,
    <B as QueryBuilder>::Query: ContextBound,
{
    type Query = RawReadQuery<'data, B::Query>;

    fn base(&self) -> &BuilderBase {
        self.base.base()
    }

    fn build(self) -> Self::Query {
        RawReadQuery {
            raw_read_output: self.raw_read_output,
            base: self.base.build(),
        }
    }
}

impl<'data, B> ReadQueryBuilder<'data> for RawReadBuilder<'data, B>
where
    B: ReadQueryBuilder<'data>,
    <B as QueryBuilder>::Query: ContextBound,
{
}

/// Reads query results into raw buffers.
///
/// This is the most flexible way to read data but also the most cumbersome.
/// Recommended usage is to run the query one step at a time, and borrow
/// the buffers between each step to process intermediate results.
pub struct VarRawReadQuery<'data, Q> {
    pub(crate) raw_read_output: Vec<TypedReadHandle<'data>>,
    pub(crate) base: Q,
}

impl<Q> ContextBound for VarRawReadQuery<'_, Q>
where
    Q: ContextBound,
{
    fn context(&self) -> Context {
        self.base.context()
    }
}

impl<Q> Query for VarRawReadQuery<'_, Q>
where
    Q: Query,
{
    fn base(&self) -> &QueryBase {
        self.base.base()
    }

    fn finalize(self) -> TileDBResult<Array> {
        self.base.finalize()
    }
}

impl<Q> ReadQuery for VarRawReadQuery<'_, Q>
where
    Q: ReadQuery,
{
    type Intermediate = (Vec<usize>, Q::Intermediate);
    type Final = (Vec<usize>, Q::Final);

    fn step(
        &mut self,
    ) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>> {
        /* update the internal buffers */
        {
            let context = self.base().context();
            let cquery = **self.base().cquery();
            for handle in self.raw_read_output.iter_mut() {
                handle.attach_query(&context, cquery)?;
            }
        }

        /* then execute */
        let base_result = {
            let _ = self
                .raw_read_output
                .iter_mut()
                .map(|r| r.borrow_mut())
                .collect::<Vec<_>>();
            self.base.step()?
        };

        let read_sizes = self
            .raw_read_output
            .iter()
            .map(|r| r.last_read_ncells())
            .collect::<Vec<usize>>();

        Ok(match base_result {
            ReadStepOutput::NotEnoughSpace => {
                /* realloc any self-managed buffers */
                for handle in self.raw_read_output.iter_mut() {
                    handle.realloc_if_managed();
                }

                /* TODO: check that records/bytes are zero and produce an internal error if not */
                ReadStepOutput::NotEnoughSpace
            }
            ReadStepOutput::Intermediate(base_result) => {
                for ncells in read_sizes.iter() {
                    if *ncells == 0 {
                        /*
                         * The input produced no data.
                         * The returned status itself is not enough to distinguish between
                         * "no results, allocate more space plz" and "there are more results after you consume these".
                         * The API tiledb_query_get_status_details exists but is experimental,
                         * so we will worry about it later.  For now, assume this is the first
                         * raw read and it is our responsibility to signal NotEnoughSpace.
                         */
                        return Ok(ReadStepOutput::NotEnoughSpace);
                    }
                }
                ReadStepOutput::Intermediate((read_sizes, base_result))
            }
            ReadStepOutput::Final(base_result) => {
                ReadStepOutput::Final((read_sizes, base_result))
            }
        })
    }
}

pub struct VarRawReadBuilder<'data, B> {
    pub(crate) raw_read_output: Vec<TypedReadHandle<'data>>,
    pub(crate) base: B,
}

impl<B> ContextBound for VarRawReadBuilder<'_, B>
where
    B: ContextBound,
{
    fn context(&self) -> Context {
        self.base.context()
    }
}

impl<'data, B> QueryBuilder for VarRawReadBuilder<'data, B>
where
    B: QueryBuilder,
{
    type Query = VarRawReadQuery<'data, B::Query>;

    fn base(&self) -> &BuilderBase {
        self.base.base()
    }

    fn build(self) -> Self::Query {
        VarRawReadQuery {
            raw_read_output: self.raw_read_output,
            base: self.base.build(),
        }
    }
}

impl<'data, B> ReadQueryBuilder<'data> for VarRawReadBuilder<'data, B> where
    B: ReadQueryBuilder<'data>
{
}
