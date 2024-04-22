use super::output::ScratchSpace;
use super::*;

pub enum ManagedScratch<'data> {
    UInt8(
        Box<dyn ScratchAllocator<u8> + 'data>,
        Pin<Box<RefCell<QueryBuffersMut<'data, u8>>>>,
    ),
    UInt16(
        Box<dyn ScratchAllocator<u16> + 'data>,
        Pin<Box<RefCell<QueryBuffersMut<'data, u16>>>>,
    ),
    UInt32(
        Box<dyn ScratchAllocator<u32> + 'data>,
        Pin<Box<RefCell<QueryBuffersMut<'data, u32>>>>,
    ),
    UInt64(
        Box<dyn ScratchAllocator<u64> + 'data>,
        Pin<Box<RefCell<QueryBuffersMut<'data, u64>>>>,
    ),
    Int8(
        Box<dyn ScratchAllocator<i8> + 'data>,
        Pin<Box<RefCell<QueryBuffersMut<'data, i8>>>>,
    ),
    Int16(
        Box<dyn ScratchAllocator<i16> + 'data>,
        Pin<Box<RefCell<QueryBuffersMut<'data, i16>>>>,
    ),
    Int32(
        Box<dyn ScratchAllocator<i32> + 'data>,
        Pin<Box<RefCell<QueryBuffersMut<'data, i32>>>>,
    ),
    Int64(
        Box<dyn ScratchAllocator<i64> + 'data>,
        Pin<Box<RefCell<QueryBuffersMut<'data, i64>>>>,
    ),
    Float32(
        Box<dyn ScratchAllocator<f32> + 'data>,
        Pin<Box<RefCell<QueryBuffersMut<'data, f32>>>>,
    ),
    Float64(
        Box<dyn ScratchAllocator<f64> + 'data>,
        Pin<Box<RefCell<QueryBuffersMut<'data, f64>>>>,
    ),
}

macro_rules! managed_output_location {
    ($($V:ident : $U:ty),+) => {
        $(
            impl<'data> From<(Box<dyn ScratchAllocator<$U> + 'data>, Pin<Box<RefCell<QueryBuffersMut<'data, $U>>>>)> for ManagedScratch<'data> {
                fn from(value: (Box<dyn ScratchAllocator<$U> + 'data>, Pin<Box<RefCell<QueryBuffersMut<'data, $U>>>>)) -> Self {
                    ManagedScratch::$V(value.0, value.1)
                }
            }
        )+
    }
}

managed_output_location!(UInt8: u8, UInt16: u16, UInt32: u32, UInt64: u64);
managed_output_location!(Int8: i8, Int16: i16, Int32: i32, Int64: i64);
managed_output_location!(Float32: f32, Float64: f64);

macro_rules! managed_output_location_go {
    ($managed:expr, $C:ident, ($alloc:pat, $scratch:pat), $go:expr) => {
        match $managed {
            ManagedScratch::UInt8($alloc, $scratch) => {
                type $C = u8;
                $go
            }
            ManagedScratch::UInt16($alloc, $scratch) => {
                type $C = u16;
                $go
            }
            ManagedScratch::UInt32($alloc, $scratch) => {
                type $C = u32;
                $go
            }
            ManagedScratch::UInt64($alloc, $scratch) => {
                type $C = u64;
                $go
            }
            ManagedScratch::Int8($alloc, $scratch) => {
                type $C = i8;
                $go
            }
            ManagedScratch::Int16($alloc, $scratch) => {
                type $C = i16;
                $go
            }
            ManagedScratch::Int32($alloc, $scratch) => {
                type $C = i32;
                $go
            }
            ManagedScratch::Int64($alloc, $scratch) => {
                type $C = i64;
                $go
            }
            ManagedScratch::Float32($alloc, $scratch) => {
                type $C = f32;
                $go
            }
            ManagedScratch::Float64($alloc, $scratch) => {
                type $C = f64;
                $go
            }
        }
    };
}

pub enum ScratchStrategy<'data, C> {
    AttributeDefault,
    RawBuffers(&'data RefCell<QueryBuffersMut<'data, C>>),
    CustomAllocator(Box<dyn ScratchAllocator<C> + 'data>),
}

impl<'data, C> Default for ScratchStrategy<'data, C> {
    fn default() -> Self {
        ScratchStrategy::AttributeDefault
    }
}

impl<'data, C> From<&'data RefCell<QueryBuffersMut<'data, C>>>
    for ScratchStrategy<'data, C>
{
    fn from(value: &'data RefCell<QueryBuffersMut<'data, C>>) -> Self {
        ScratchStrategy::RawBuffers(value)
    }
}

/// Adapter for a read result which allocates and manages scratch space opaquely.
#[derive(ContextBound, Query)]
pub struct ManagedReadQuery<'data, Q> {
    pub(crate) scratch: ManagedScratch<'data>,
    #[base(ContextBound, Query)]
    pub(crate) base: Q,
}

impl<'data, Q> ManagedReadQuery<'data, Q> {
    fn realloc(&self) {
        managed_output_location_go!(
            self.scratch,
            C,
            (ref alloc, ref scratch),
            {
                let tmp = QueryBuffersMut {
                    data: BufferMut::Empty,
                    cell_offsets: None,
                    validity: None,
                };
                let old_scratch = ScratchSpace::<C>::try_from(
                    scratch.replace(tmp),
                )
                .expect(
                    "ManagedReadQuery cannot have a borrowed output location",
                );
                let new_scratch = alloc.realloc(old_scratch);
                let _ = scratch.replace(QueryBuffersMut::from(new_scratch));
            }
        );
    }
}

impl<'ctx, 'data, Q> ReadQuery<'ctx> for ManagedReadQuery<'data, Q>
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
        let base_output = self.base.step()?;
        if matches!(base_output, ReadStepOutput::NotEnoughSpace) {
            /*
             * Arguably this should happen prior to `self.base.step()` if the *previous*
             * step result was NotEnoughSpace, as this will do unnecessary allocations
             * if the user chooses to abort prior to submitting the next step.
             */
            self.realloc();
        }
        Ok(base_output)
    }
}

#[derive(ContextBound)]
pub struct ManagedReadBuilder<'data, B> {
    pub(crate) scratch: ManagedScratch<'data>,
    #[base(ContextBound)]
    pub(crate) base: B,
}

impl<'ctx, 'data, B> QueryBuilder<'ctx> for ManagedReadBuilder<'data, B>
where
    B: QueryBuilder<'ctx>,
{
    type Query = ManagedReadQuery<'data, B::Query>;

    fn base(&self) -> &BuilderBase<'ctx> {
        self.base.base()
    }

    fn build(self) -> Self::Query {
        ManagedReadQuery {
            scratch: self.scratch,
            base: self.base.build(),
        }
    }
}

impl<'ctx, 'data, B> ReadQueryBuilder<'ctx, 'data>
    for ManagedReadBuilder<'data, B>
where
    B: ReadQueryBuilder<'ctx, 'data>,
{
}
