use std::fmt::Debug;

use proptest::strategy::{NewTree, Strategy, ValueTree};
use proptest::test_runner::TestRunner;

#[derive(Clone, Debug)]
pub struct Maybe<T>(pub T);

impl<T> Strategy for Maybe<T>
where
    T: Debug + Strategy,
    <T as Strategy>::Tree: Debug,
{
    type Tree = MaybeValueTree<<T as Strategy>::Tree>;
    type Value = Option<T::Value>;

    fn new_tree(&self, runner: &mut TestRunner) -> NewTree<Self> {
        Ok(MaybeValueTree::new(self.0.new_tree(runner)?))
    }
}

#[derive(Clone, Debug)]
pub struct MaybeValueTree<T> {
    inner: T,
    state: MaybeState,
}

impl<T> MaybeValueTree<T> {
    pub fn new(value: T) -> Self {
        MaybeValueTree {
            inner: value,
            state: MaybeState::New,
        }
    }
}

#[derive(Clone, Debug)]
enum MaybeState {
    New,
    TryNone,
    NoneFailed,
    NonePassed,
}

impl<T> ValueTree for MaybeValueTree<T>
where
    T: Debug + ValueTree,
{
    type Value = Option<<T as ValueTree>::Value>;

    fn current(&self) -> Self::Value {
        match self.state {
            MaybeState::TryNone => None,
            _ => Some(self.inner.current()),
        }
    }

    fn simplify(&mut self) -> bool {
        match self.state {
            MaybeState::New => {
                // the initial output was Some of the initial
                self.state = MaybeState::TryNone;
                true
            }
            MaybeState::TryNone => {
                // `None` failed, no way to get simpler
                self.state = MaybeState::NoneFailed;
                false
            }
            MaybeState::NoneFailed => {
                // we have already found the simplest failing input
                false
            }
            MaybeState::NonePassed => {
                // `None` is not the simplest so defer to inner
                self.inner.simplify()
            }
        }
    }

    fn complicate(&mut self) -> bool {
        match self.state {
            MaybeState::New => {
                // initial input passed, this likely is just `true`
                self.inner.complicate()
            }
            MaybeState::TryNone => {
                // `None` passed, begin searching the inner value tree
                self.state = MaybeState::NonePassed;
                true
            }
            MaybeState::NoneFailed => {
                // search is over, None is the simplest failing input
                false
            }
            MaybeState::NonePassed => {
                // search the inner value
                self.inner.complicate()
            }
        }
    }
}
