use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::rc::Rc;

use proptest::prelude::*;
use proptest::strategy::{NewTree, ValueTree};
use proptest::test_runner::{Config as ProptestConfig, TestRunner};

use crate::sequence::SequenceValueTree;

/// Strategy to create [ValueTree] objects for a wrapped [Strategy].
///
/// See `StrategyExt::prop_indirect`.
///
/// ```
/// # use proptest::prelude::*;
/// # use proptest::strategy::ValueTree;
/// # use strategy_ext::meta::ValueTreeStrategy;
/// use strategy_ext::StrategyExt;
///
/// proptest! {
///     fn value_tree_test(mut value_tree in any::<u64>().prop_indirect()) {
///         // binary search should always simplify
///         assert!(value_tree.simplify());
///     }
/// }
///
/// value_tree_test();
/// ```
/// This can be used to write tests about how a [ValueTree] created by
/// a custom [Strategy] responds to shrinking.
#[derive(Debug)]
pub struct ValueTreeStrategy<S>(pub(super) S);

impl<S> Strategy for ValueTreeStrategy<S>
where
    S: Strategy,
    <S as Strategy>::Tree: Clone + Debug,
{
    type Tree = ValueTreeWrapper<S::Tree>;
    type Value = S::Tree;

    fn new_tree(&self, runner: &mut TestRunner) -> NewTree<Self> {
        Ok(ValueTreeWrapper(self.0.new_tree(runner)?))
    }
}

/// [ValueTree] corresponding to [ValueTreeStrategy].
///
/// The values of this `[ValueTree]` are [ValueTree]s created by
/// some other strategy. The shrinking process shrinks the wrapped
/// [ValueTree].
pub struct ValueTreeWrapper<VT>(VT);

impl<VT> ValueTree for ValueTreeWrapper<VT>
where
    VT: Clone + Debug + ValueTree,
{
    type Value = VT;

    fn current(&self) -> Self::Value {
        self.0.clone()
    }

    fn simplify(&mut self) -> bool {
        self.0.simplify()
    }

    fn complicate(&mut self) -> bool {
        self.0.complicate()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShrinkAction {
    Simplify,
    Complicate,
}

impl ShrinkAction {
    pub fn apply<VT>(&self, vt: &mut VT) -> bool
    where
        VT: ValueTree,
    {
        match self {
            Self::Simplify => vt.simplify(),
            Self::Complicate => vt.complicate(),
        }
    }
}

/// Strategy to create sequences of shrinking steps.
///
/// This is useful with [ValueTreeStrategy] to write
/// tests which assert properties about how a [ValueTree]
/// responds to shrinking.
///
/// Sequences produced by this [Strategy] ensure that the
/// number of complications does not exceed the number
/// of simplifications up to that point in the sequence.
#[derive(Debug)]
pub struct ShrinkSequenceStrategy {
    pub max_length: usize,
}

impl Default for ShrinkSequenceStrategy {
    fn default() -> Self {
        ShrinkSequenceStrategy {
            max_length: std::cmp::min(
                1024,
                ProptestConfig::default().max_shrink_iters as usize,
            ),
        }
    }
}

impl Strategy for ShrinkSequenceStrategy {
    type Tree = ShrinkSequenceValueTree;
    type Value = <<Self as Strategy>::Tree as ValueTree>::Value;

    fn new_tree(&self, runner: &mut TestRunner) -> NewTree<Self> {
        let desired_length =
            proptest::num::sample_uniform_incl(runner, 0, self.max_length);
        if desired_length == 0 {
            return Ok(SequenceValueTree::new(Vec::new()));
        }

        let mut steps = vec![];

        let mut num_shrinks = 0;
        while steps.len() < desired_length {
            if num_shrinks == 0 {
                num_shrinks += 1;
                steps.push(ShrinkAction::Simplify);
            } else {
                // choose randomly whether to continue simplifying or to complicate.
                // avoid early thrashing by making complication more likely
                // as the number of Simplify actions grows.
                let value = proptest::num::sample_uniform_incl(
                    runner,
                    0,
                    self.max_length - num_shrinks,
                );
                steps.push(if value == 0 {
                    num_shrinks -= 1;
                    ShrinkAction::Complicate
                } else {
                    num_shrinks += 1;
                    ShrinkAction::Simplify
                });
            }
        }
        Ok(SequenceValueTree::new(steps))
    }
}

pub type ShrinkSequenceValueTree = SequenceValueTree<ShrinkAction>;

/// Strategy adapter to transform [ValueTree]s.
///
/// Where [prop_map] transforms the [Strategy], this adapter transforms
/// the [ValueTree]s produced by the source [Strategy].
///
/// One way to use this would be to implement custom shrinking strategies
/// for strategies built using existing adapters, without changing
/// the way that the strategy is constructed.
#[derive(Clone)]
pub struct MapValueTree<S, VT>
where
    S: Strategy,
    VT: ValueTree<Value = S::Value>,
{
    source: S,
    transform: Rc<dyn Fn(S::Tree) -> VT>,
}

impl<S, VT> MapValueTree<S, VT>
where
    S: Strategy,
    VT: ValueTree<Value = S::Value>,
{
    pub(super) fn new<F>(source: S, transform: F) -> Self
    where
        F: Fn(S::Tree) -> VT + 'static,
    {
        MapValueTree {
            source,
            transform: Rc::new(transform),
        }
    }
}

impl<S, VT> Debug for MapValueTree<S, VT>
where
    S: Debug + Strategy,
    VT: ValueTree<Value = S::Value>,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        f.debug_struct("MapValueTree")
            .field("source", &self.source)
            .finish()
    }
}

impl<S, VT> Strategy for MapValueTree<S, VT>
where
    S: Strategy,
    VT: Debug + ValueTree<Value = S::Value>,
{
    type Tree = VT;
    type Value = VT::Value;

    fn new_tree(&self, runner: &mut TestRunner) -> NewTree<Self> {
        Ok((self.transform)(self.source.new_tree(runner)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    proptest! {
        #[test]
        fn valid_shrink_sequence(sequence in ShrinkSequenceStrategy::default()) {
            assert_valid_shrink_sequence(sequence)
        }
    }

    fn assert_valid_shrink_sequence(sequence: Vec<ShrinkAction>) {
        let mut simplify_run_length: isize = 0;

        for action in sequence {
            match action {
                ShrinkAction::Simplify => {
                    simplify_run_length += 1;
                }
                ShrinkAction::Complicate => {
                    assert!(simplify_run_length > 0);
                    simplify_run_length -= 1;
                }
            }
        }
    }
}
