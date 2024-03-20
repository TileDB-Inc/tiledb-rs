use proptest::prelude::*;
use proptest::strategy::*;
use proptest::test_runner::TestRunner;
use std::sync::Arc;

type LifetimeValueTree<'ctx, T> = Box<dyn ValueTree<Value = T> + 'ctx>;

/// Similar to BoxedStrategy, but with a narrower lifetime than 'static.
/// Use when there are conflicts with multiple Strategy implementing types that
/// have the same Value output - this erases the implementing type and just leaves the trait.
#[derive(Debug)]
pub struct LifetimeStrategy<'ctx, T>(
    Arc<dyn Strategy<Value = T, Tree = LifetimeValueTree<'ctx, T>> + 'ctx>,
);

impl<'ctx, T: std::fmt::Debug> Strategy for LifetimeStrategy<'ctx, T> {
    type Tree = LifetimeValueTree<'ctx, T>;
    type Value = T;

    fn new_tree(&self, runner: &mut TestRunner) -> NewTree<Self> {
        self.0.new_tree(runner)
    }
}

pub trait LifetimeBoundStrategy<'ctx>: Strategy + 'ctx {
    fn bind(self) -> LifetimeStrategy<'ctx, <Self as Strategy>::Value>;
}

impl<'ctx, S> LifetimeBoundStrategy<'ctx> for S
where
    S: Sized + Strategy + 'ctx,
{
    fn bind(self) -> LifetimeStrategy<'ctx, <Self as Strategy>::Value> {
        LifetimeStrategy(Arc::new(LifetimeStrategyWrapper::new(self)))
    }
}

#[derive(Debug)]
struct LifetimeStrategyWrapper<'ctx, T> {
    strategy: T,
    _lifetime: std::marker::PhantomData<&'ctx T>,
}

impl<'ctx, S: Strategy> LifetimeStrategyWrapper<'ctx, S>
where
    S::Tree: 'ctx,
{
    pub fn new(strategy: S) -> Self {
        LifetimeStrategyWrapper {
            strategy,
            _lifetime: std::marker::PhantomData,
        }
    }
}

impl<'ctx, T: Strategy> Strategy for LifetimeStrategyWrapper<'ctx, T>
where
    T::Tree: 'ctx,
{
    type Tree = Box<dyn ValueTree<Value = T::Value> + 'ctx>;
    type Value = T::Value;

    fn new_tree(&self, runner: &mut TestRunner) -> NewTree<Self> {
        Ok(Box::new(self.strategy.new_tree(runner)?))
    }
}
