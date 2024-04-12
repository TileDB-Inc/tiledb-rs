use super::output::ScratchSpace;
use super::*;

/// Adapter for a read result which allocates and manages scratch space opaquely.
#[derive(ContextBound, QueryCAPIInterface)]
pub struct ManagedReadQuery<'data, C, A, Q> {
    pub(crate) alloc: A,
    pub(crate) scratch: Pin<Box<RefCell<OutputLocation<'data, C>>>>,
    #[base(ContextBound, QueryCAPIInterface)]
    pub(crate) base: Q,
}

impl<'data, C, A, Q> ManagedReadQuery<'data, C, A, Q>
where
    A: ScratchAllocator<C>,
{
    fn realloc(&self) {
        let tmp = OutputLocation {
            data: BufferMut::Empty,
            cell_offsets: None,
        };
        let old_scratch = ScratchSpace::<C>::try_from(
            self.scratch.replace(tmp),
        )
        .expect("ManagedReadQuery cannot have a borrowed output location");

        let new_scratch = self.alloc.realloc(old_scratch);
        let _ = self.scratch.replace(OutputLocation::from(new_scratch));
    }
}

impl<'ctx, 'data, C, A, Q> ReadQuery<'ctx> for ManagedReadQuery<'data, C, A, Q>
where
    Q: ReadQuery<'ctx>,
    A: ScratchAllocator<C>,
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

#[derive(ContextBound, QueryCAPIInterface)]
pub struct ManagedReadBuilder<'data, C, A, B> {
    pub(crate) alloc: A,
    pub(crate) scratch: Pin<Box<RefCell<OutputLocation<'data, C>>>>,
    #[base(ContextBound, QueryCAPIInterface)]
    pub(crate) base: B,
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
