use std::cell::RefCell;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;

use proptest::strategy::{NewTree, Strategy, ValueTree};
use proptest::test_runner::TestRunner;

#[derive(Debug)]
pub struct WithoutReplacement<T: Strategy> {
    source: T,
    values: RefCell<HashSet<T::Value>>,
}

impl<T: Strategy> WithoutReplacement<T> {
    pub fn new(source: T) -> Self {
        Self {
            source,
            values: RefCell::new(HashSet::new()),
        }
    }
}

impl<T: Strategy> Strategy for WithoutReplacement<T>
where
    T::Value: Eq + Hash,
{
    type Tree = T::Tree;
    type Value = T::Value;

    fn new_tree(&self, runner: &mut TestRunner) -> NewTree<Self> {
        loop {
            let tree = self.source.new_tree(runner)?;
            if self.values.borrow_mut().insert(tree.current()) {
                break Ok(tree);
            }
            runner.reject_local(format!(
                "Strategy generated value already: {:?}",
                tree.current()
            ))?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    use crate::StrategyExt;

    #[test]
    fn without_replacement() {
        let previous = RefCell::new(HashSet::new());

        proptest!(|(s in any::<String>().prop_without_replacement())| {
            let first_insert = previous.borrow_mut().insert(s);
            assert!(first_insert);
        });
    }
}
