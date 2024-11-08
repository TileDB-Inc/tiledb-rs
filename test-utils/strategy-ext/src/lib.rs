pub mod meta;
pub mod records;
pub mod sequence;
pub mod strategy;

use std::fmt::Debug;

use proptest::strategy::{Strategy, ValueTree};

pub trait StrategyExt: Strategy {
    /// Returns a strategy which produces the [ValueTree]s returned by [self].
    ///
    /// This additional indirection can be used to test the [ValueTree]
    /// associated with this [Strategy].
    fn prop_indirect(self) -> meta::ValueTreeStrategy<Self>
    where
        Self: Sized,
        Self::Tree: Clone + Debug,
    {
        meta::ValueTreeStrategy(self)
    }

    /// Returns a strategy which produces values transformed by
    /// the [ValueTree] mapping function `transform`.
    ///
    /// This is similar to [prop_map] but also enables changing the way
    /// that values produced by [self] are shrunk.
    fn value_tree_map<F, VT>(self, transform: F) -> meta::MapValueTree<Self, VT>
    where
        Self: Sized,
        F: Fn(<Self as Strategy>::Tree) -> VT + 'static,
        VT: ValueTree<Value = Self::Value>,
    {
        meta::MapValueTree::new(self, transform)
    }
}

impl<S> StrategyExt for S where S: Strategy {}
