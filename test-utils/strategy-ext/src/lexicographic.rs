use std::fmt::Debug;

use num_traits::{Bounded, FromPrimitive, NumOps, One, ToPrimitive};
use proptest::prelude::*;
use proptest::strategy::{NewTree, ValueTree};
use proptest::test_runner::TestRunner;

/// Strategy which produces values which are lexicographically between a lower and upper bound
/// (inclusive).
#[derive(Debug)]
pub struct Between<T> {
    lb: Vec<T>,
    ub: Vec<T>,
}

impl<T> Between<T> {
    pub fn new(lb: &[T], ub: &[T]) -> Self
    where
        T: Clone,
    {
        Self {
            lb: lb.to_vec(),
            ub: ub.to_vec(),
        }
    }
}

impl<T> Strategy for Between<T>
where
    T: Bounded
        + Copy
        + Debug
        + Default
        + FromPrimitive
        + NumOps
        + One
        + Ord
        + ToPrimitive,
{
    type Tree = BetweenValueTree<T>;
    type Value = Vec<T>;

    fn new_tree(&self, runner: &mut TestRunner) -> NewTree<Self> {
        if self.lb == self.ub {
            return Ok(BetweenValueTree::new(
                self.lb.clone(),
                self.ub.clone(),
                self.lb.clone(),
            ));
        }

        let prefix = self
            .lb
            .iter()
            .zip(self.ub.iter())
            .take_while(|(l, r)| l == r)
            .map(|(l, _)| l)
            .copied()
            .collect::<Vec<T>>();

        let lb_suffix = &self.lb[prefix.len()..];
        let ub_suffix = &self.ub[prefix.len()..];
        assert!(!ub_suffix.is_empty());

        let value_suffix_len = (runner.rng().next_u64() as usize)
            % std::cmp::max(lb_suffix.len(), ub_suffix.len());

        if value_suffix_len == 0 {
            return Ok(BetweenValueTree::new(
                self.lb.clone(),
                self.ub.clone(),
                self.lb.clone(),
            ));
        }

        let domain_lower = <T as Bounded>::min_value();
        let domain_upper = <T as Bounded>::max_value();

        let mut next_value = |min: T, max: T| {
            let min = min.to_i128().unwrap();
            let max = max.to_i128().unwrap();

            let rng =
                min + i128::from(runner.rng().next_u64()) % (max - min + 1);
            T::from_i128(rng).unwrap()
        };

        let mut values = vec![if lb_suffix.is_empty() {
            next_value(domain_lower, ub_suffix[0])
        } else {
            next_value(lb_suffix[0], ub_suffix[0])
        }];

        if !lb_suffix.is_empty() && values[0] == lb_suffix[0] {
            assert!(values.as_slice() < ub_suffix);

            while values.len() < value_suffix_len
                || values.as_slice() < lb_suffix
            {
                let i = values.len();
                values.push(
                    if i < lb_suffix.len() && values == lb_suffix[0..i] {
                        next_value(lb_suffix[i], domain_upper)
                    } else {
                        next_value(domain_lower, domain_upper)
                    },
                );
            }
        } else {
            assert!(lb_suffix < values.as_slice());

            for i in 1..value_suffix_len {
                if values == ub_suffix {
                    break;
                }
                values.push(
                    if i < ub_suffix.len() && values == ub_suffix[0..i] {
                        next_value(domain_lower, ub_suffix[i])
                    } else {
                        next_value(domain_lower, domain_upper)
                    },
                );
            }
        }

        Ok(BetweenValueTree::new(
            self.lb.clone(),
            self.ub.clone(),
            [prefix, values].concat(),
        ))
    }
}

/// Shrinks to find the minimum failing value between a lower and upper bound.
///
/// FIXME: shrinking is not currently implemented.
pub struct BetweenValueTree<T> {
    _lb: Vec<T>,
    _ub: Vec<T>,
    init: Vec<T>,
}

impl<T> BetweenValueTree<T> {
    pub fn new(lb: Vec<T>, ub: Vec<T>, init: Vec<T>) -> Self {
        Self {
            _lb: lb,
            _ub: ub,
            init,
        }
    }
}

impl<T> ValueTree for BetweenValueTree<T>
where
    T: Clone + Debug,
{
    type Value = Vec<T>;

    fn current(&self) -> Self::Value {
        self.init.clone()
    }

    fn simplify(&mut self) -> bool {
        // FIXME
        false
    }

    fn complicate(&mut self) -> bool {
        // FIXME
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strat_between<T>() -> impl Strategy<Value = (Vec<T>, Vec<T>, Vec<T>)>
    where
        T: Arbitrary
            + Bounded
            + Copy
            + Debug
            + Default
            + FromPrimitive
            + NumOps
            + One
            + Ord
            + ToPrimitive
            + 'static,
    {
        let strat_bound = proptest::collection::vec(any::<T>(), 0..=16).boxed();
        (strat_bound.clone(), strat_bound.clone()).prop_flat_map(|(b1, b2)| {
            let (lb, ub) = if b1 < b2 { (b1, b2) } else { (b2, b1) };
            let strat_value = Between::new(&lb, &ub);
            (Just(lb), Just(ub), strat_value)
        })
    }

    proptest! {
        #[test]
        fn between_u8((lb, ub, value) in strat_between::<u8>()) {
            assert!(lb <= value);
            assert!(value <= ub);
        }

        #[test]
        fn between_i8((lb, ub, value) in strat_between::<i8>()) {
            assert!(lb <= value);
            assert!(value <= ub);
        }

        #[test]
        fn between_u64((lb, ub, value) in strat_between::<u64>()) {
            assert!(lb <= value);
            assert!(value <= ub);
        }

        #[test]
        fn between_i64((lb, ub, value) in strat_between::<i64>()) {
            assert!(lb <= value);
            assert!(value <= ub);
        }
    }
}
