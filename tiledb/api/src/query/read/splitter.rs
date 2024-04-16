use super::*;

use std::cell::RefCell;
use std::rc::Rc;

/// Read query node which functions as a leaf of a tree.
/// A node with multiple children can use this to feed
/// the same step result into multiple nodes (presumably RawReadQuery nodes).
#[derive(Clone, ContextBound, QueryCAPIInterface)]
pub struct ReadSplitterQuery<'ctx> {
    pub(crate) previous_step: Rc<RefCell<Option<ReadStepOutput<(), ()>>>>,
    #[base(ContextBound, QueryCAPIInterface)]
    pub(crate) base: Rc<QueryRaw<'ctx>>,
}

impl<'ctx> ReadQuery for ReadSplitterQuery<'ctx> {
    type Intermediate = ();
    type Final = ();

    fn step(
        &mut self,
    ) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>> {
        let mut step = self.previous_step.borrow_mut();
        if step.is_none() {
            *step = Some(self.base.do_submit_read()?);
        }

        Ok(step.as_ref().cloned().unwrap())
    }
}

#[derive(ContextBound, QueryCAPIInterface)]
pub struct QueryRaw<'ctx> {
    #[context]
    context: &'ctx Context,
    #[raw_array]
    array: RawArray,
    #[raw_query]
    query: RawQuery,
}

#[derive(Clone, ContextBound, QueryCAPIInterface)]
pub struct ReadSplitterBuilder<'ctx> {
    #[base(ContextBound, QueryCAPIInterface)]
    query: ReadSplitterQuery<'ctx>,
}

impl<'ctx> ReadSplitterBuilder<'ctx> {
    pub fn new<B>(b: &B) -> Self
    where
        B: ContextBound<'ctx> + QueryCAPIInterface,
    {
        ReadSplitterBuilder {
            query: ReadSplitterQuery {
                previous_step: Rc::new(RefCell::new(None)),
                base: Rc::new(QueryRaw {
                    context: b.context(),
                    array: b.carray().borrow(),
                    query: b.cquery().borrow(),
                }),
            },
        }
    }
}

impl<'ctx> QueryBuilder<'ctx> for ReadSplitterBuilder<'ctx> {
    type Query = ReadSplitterQuery<'ctx>;

    fn build(self) -> Self::Query {
        self.query.clone()
    }
}

impl<'ctx> ReadQueryBuilder<'ctx> for ReadSplitterBuilder<'ctx> {}
