use super::*;

use crate::datatype::PhysicalType;

pub trait ReadResult: Sized {
    type Constructor: ReadCallback<Intermediate = (), Final = Self>;
}

/// Query result handler which constructs an object from query results.
pub struct TypedReadQuery<'data, T, Q>
where
    T: ReadResult,
{
    pub(crate) _marker: std::marker::PhantomData<T>,
    pub(crate) base:
        CallbackReadQuery<'data, <T as ReadResult>::Constructor, Q>,
}

impl<'data, T, Q> ContextBound for TypedReadQuery<'data, T, Q>
where
    T: ReadResult,
    CallbackReadQuery<'data, <T as ReadResult>::Constructor, Q>: ContextBound,
{
    fn context(&self) -> Context {
        self.base.context()
    }
}

impl<'array, 'data, T, Q> Query<'array> for TypedReadQuery<'data, T, Q>
where
    T: ReadResult,
    CallbackReadQuery<'data, <T as ReadResult>::Constructor, Q>: Query<'array>,
{
    fn base(&self) -> &QueryBase<'array> {
        self.base.base()
    }

    fn finalize(self) -> TileDBResult<()> {
        self.base.finalize()
    }
}

impl<'array, 'data, T, Q> ReadQuery<'array> for TypedReadQuery<'data, T, Q>
where
    T: ReadResult,
    Q: ReadQuery<'array>,
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

impl<'data, T, B> ContextBound for TypedReadBuilder<'data, T, B>
where
    T: ReadResult,
    CallbackReadBuilder<'data, <T as ReadResult>::Constructor, B>: ContextBound,
{
    fn context(&self) -> Context {
        self.base.context()
    }
}

impl<'array, 'data, T, B> QueryBuilder<'array> for TypedReadBuilder<'data, T, B>
where
    T: ReadResult,
    B: QueryBuilder<'array>,
{
    type Query = TypedReadQuery<'data, T, B::Query>;

    fn base(&self) -> &BuilderBase<'array> {
        self.base.base()
    }

    fn build(self) -> Self::Query {
        TypedReadQuery {
            _marker: self._marker,
            base: self.base.build(),
        }
    }
}

impl<'array, 'data, T, B> ReadQueryBuilder<'array, 'data>
    for TypedReadBuilder<'data, T, B>
where
    T: ReadResult,
    B: ReadQueryBuilder<'array, 'data>,
{
}

mod impls {
    use super::*;

    impl<C> ReadResult for Vec<C>
    where
        C: PhysicalType,
    {
        type Constructor = Self;
    }

    impl<C> ReadResult for (Vec<C>, Vec<u8>)
    where
        C: PhysicalType,
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
