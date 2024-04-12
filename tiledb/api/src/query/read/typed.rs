use super::*;

/// Query result handler which constructs an object from query results.
#[derive(ContextBound, QueryCAPIInterface)]
pub struct TypedReadQuery<'data, T, Q>
where
    T: ReadResult,
{
    pub(crate) _marker: std::marker::PhantomData<T>,
    #[base(ContextBound, QueryCAPIInterface)]
    pub(crate) base: CallbackReadQuery<'data, <T as ReadResult>::Receiver, Q>,
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

#[derive(ContextBound, QueryCAPIInterface)]
pub struct TypedReadBuilder<'data, T, B>
where
    T: ReadResult,
{
    pub(crate) _marker: std::marker::PhantomData<T>,
    #[base(ContextBound, QueryCAPIInterface)]
    pub(crate) base: CallbackReadBuilder<'data, <T as ReadResult>::Receiver, B>,
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
