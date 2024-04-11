use super::*;

pub struct ManagedReadQuery<'data, C, A, Q> {
    pub(crate) alloc: A,
    pub(crate) scratch: Pin<Box<RefCell<OutputLocation<'data, C>>>>,
    pub(crate) base: Q,
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

pub struct ManagedReadBuilder<'data, C, A, B> {
    pub(crate) alloc: A,
    pub(crate) scratch: Pin<Box<RefCell<OutputLocation<'data, C>>>>,
    pub(crate) base: B,
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

impl<'data, C, A, B> QueryCAPIInterface for ManagedReadBuilder<'data, C, A, B>
where
    B: QueryCAPIInterface,
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
