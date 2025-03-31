use std::fmt::Debug;

use proptest::strategy::ValueTree;

/// [ValueTree] representing a logical sequence of ordered steps.
///
/// Shrinking truncates or re-grows values at the end of the sequence
/// so that the order of steps is always preserved.
#[derive(Clone, Debug)]
pub struct SequenceValueTree<Element> {
    initial_sequence: Vec<Element>,
    bound_length: usize,
    current_length: usize,
}

impl<Element> SequenceValueTree<Element> {
    pub fn new(initial_sequence: impl IntoIterator<Item = Element>) -> Self {
        let collected = initial_sequence.into_iter().collect::<Vec<Element>>();
        let init_len = collected.len();
        SequenceValueTree {
            initial_sequence: collected,
            bound_length: init_len,
            current_length: init_len,
        }
    }
}

impl<Element> ValueTree for SequenceValueTree<Element>
where
    Element: Clone + Debug,
{
    type Value = Vec<Element>;

    fn current(&self) -> Self::Value {
        self.initial_sequence[0..self.current_length].to_vec()
    }

    fn simplify(&mut self) -> bool {
        self.bound_length = self.current_length;
        if self.current_length > 0 {
            self.current_length /= 2;
            true
        } else {
            false
        }
    }

    fn complicate(&mut self) -> bool {
        if self.current_length < self.bound_length {
            self.current_length += 1;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::collection::vec as strat_vec;
    use proptest::prelude::*;

    use super::*;
    use crate::StrategyExt;
    use crate::meta::{ShrinkAction, ShrinkSequenceStrategy};

    proptest! {
        /// Ensure that the sequence strategy always returns a subsequence
        /// which starts from the beginning of the initial input
        #[test]
        fn sequence_shrinking(
            vt in strat_vec(any::<u64>(), 0..=1024)
                .value_tree_map(|vt| SequenceValueTree::new(vt.current()))
                .prop_indirect(),
            shrinks in ShrinkSequenceStrategy::default())
        {
            let init = vt.current();
            let mut vt = vt;

            let mut converged = false;
            let mut high_bound_len = init.len();

            for action in shrinks {
                let prev = vt.current();

                let step = action.apply(&mut vt);

                let current = vt.current();

                assert_eq!(&init[0.. vt.current_length], current);
                assert!(current.len() <= high_bound_len);

                if step {
                    match action {
                        ShrinkAction::Simplify => {
                            assert!(current.len() < prev.len());
                            high_bound_len = prev.len();
                        },
                        ShrinkAction::Complicate => {
                            assert!(current.len() > prev.len());
                        }
                    }
                } else {
                    assert_eq!(prev.len(), current.len());
                }

                if converged {
                    assert!(!step);
                } else {
                    converged = !step;
                }
            }
        }
    }
}
