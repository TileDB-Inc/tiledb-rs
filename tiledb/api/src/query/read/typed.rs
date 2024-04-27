use super::*;

pub trait ReadResult: Sized {
    type Constructor: ReadCallback<Intermediate = (), Final = Self>;
}

/// Query result handler which constructs an object from query results.
#[derive(Query)]
pub struct TypedReadQuery<'data, T, Q>
where
    T: ReadResult,
{
    pub(crate) _marker: std::marker::PhantomData<T>,
    #[base(ContextBound, Query)]
    pub(crate) base:
        CallbackReadQuery<'data, <T as ReadResult>::Constructor, Q>,
}

impl<'data, T, Q> ReadQuery for TypedReadQuery<'data, T, Q>
where
    T: ReadResult,
    Q: ReadQuery,
{
    type Intermediate = Q::Intermediate;
    type Final = (T, Q::Final);

    fn step(
        &mut self,
    ) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>> {
        Ok(match self.base.step()? {
            ReadStepOutput::NotEnoughSpace => ReadStepOutput::NotEnoughSpace,
            ReadStepOutput::Intermediate((_, base_result)) => {
                ReadStepOutput::Intermediate(base_result)
            }
            ReadStepOutput::Final((f, base_result)) => {
                ReadStepOutput::Final((f, base_result))
            }
        })
    }
}

pub struct TypedReadBuilder<'data, T, B>
where
    T: ReadResult,
{
    pub(crate) _marker: std::marker::PhantomData<T>,
    pub(crate) base:
        CallbackReadBuilder<'data, <T as ReadResult>::Constructor, B>,
}

impl<'data, T, B> QueryBuilder for TypedReadBuilder<'data, T, B>
where
    T: ReadResult,
    B: QueryBuilder,
{
    type Query = TypedReadQuery<'data, T, B::Query>;

    fn base(&self) -> &BuilderBase {
        self.base.base()
    }

    fn build(self) -> Self::Query {
        TypedReadQuery {
            _marker: self._marker,
            base: self.base.build(),
        }
    }
}

impl<'data, T, B> ReadQueryBuilder<'data> for TypedReadBuilder<'data, T, B>
where
    T: ReadResult,
    B: ReadQueryBuilder<'data>,
{
}

mod impls {
    use super::*;

    impl<C> ReadResult for Vec<C>
    where
        C: CAPISameRepr,
    {
        type Constructor = Self;
    }

    impl<C> ReadResult for (Vec<C>, Vec<u8>)
    where
        C: CAPISameRepr,
    {
        type Constructor = Self;
    }

    impl ReadResult for Vec<String> {
        type Constructor = Self;
    }

    impl ReadResult for (Vec<String>, Vec<u8>) {
        type Constructor = Self;
    }
}
