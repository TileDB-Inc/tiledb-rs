use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

use proptest::bits::{BitSetLike, VarBitSet};
use proptest::collection::SizeRange;
use proptest::num::sample_uniform_incl;
use proptest::prelude::*;
use proptest::strategy::{NewTree, ValueTree};
use proptest::test_runner::TestRunner;

/// Create a strategy to generate `Vec`s containing elements drawn from `element` and with a size
/// range given by `size`.
///
/// In contrast to `proptest::collection::vec`, value trees produced by
/// this strategy do not attempt to shrink any of the elements of the vector, instead shrinking
/// by more rapidly searching for the minimum set of elements needed to produce a failure.
pub fn vec_records_strategy<T>(
    element: T,
    size: impl Into<SizeRange>,
) -> VecRecordsStrategy<T>
where
    T: Strategy,
{
    VecRecordsStrategy {
        element,
        size: size.into(),
    }
}

/// A value which represents a collection of logical records.
pub trait Records: Clone + Debug {
    /// Returns the number of records in the collection.
    /// Implementations should panic if `self` is somehow invalid.
    fn len(&self) -> usize;

    /// Returns a filtered set of `self`'s records, with `subset` indicating
    /// the set of records to include.
    /// Implementations should panic if `self` is somehow invalid.
    fn filter(&self, subset: &VarBitSet) -> Self;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> Records for Vec<T>
where
    T: Clone + Debug,
{
    fn len(&self) -> usize {
        self.len()
    }

    fn filter(&self, subset: &VarBitSet) -> Self {
        self.iter()
            .enumerate()
            .filter(|&(ix, _)| subset.test(ix))
            .map(|v| v.1.clone())
            .collect::<Self>()
    }
}

impl<K, V> Records for HashMap<K, V>
where
    K: Clone + Debug + Eq + Hash,
    V: Records,
{
    fn len(&self) -> usize {
        let mut nrecords = None;
        for f in self.values() {
            if let Some(nrecords) = nrecords {
                assert_eq!(nrecords, f.len())
            } else {
                nrecords = Some(f.len())
            }
        }
        nrecords.unwrap()
    }

    fn filter(&self, subset: &VarBitSet) -> Self {
        self.iter()
            .map(|(k, v)| (k.clone(), v.filter(subset)))
            .collect::<Self>()
    }
}

macro_rules! tuple_impls_record {
    ($T:ident) => {
        tuple_impls_record!(@impl $T);
    };
    ($T:ident, $( $U:ident ),+) => {
        tuple_impls_record!($($U),+);
        tuple_impls_record!(@impl $T, $($U),+);
    };

    // internal implementation
    (@impl $($T:ident),+) => {
        impl<$($T: Records),+> Records for ($($T,)+) {
            fn len(&self) -> usize {
                #[allow(non_snake_case)]
                let ($($T,)+) = self;

                let mut nrecords = None;
                $(
                    if let Some(nrecords) = nrecords {
                        assert_eq!(nrecords, $T.len());
                    } else {
                        nrecords = Some($T.len());
                    }
                )+

                nrecords.unwrap()
            }

            fn filter(&self, subset: &VarBitSet) -> Self {
                #[allow(non_snake_case)]
                let ($($T,)+) = self;
                (
                    $(
                        $T.filter(subset),
                    )+
                )
            }
        }
    };
}

// this is what std does for identifiers, who knows why
tuple_impls_record!(E, D, C, B, A, Z, Y, X, W, V, U, T);

/// Strategy to create `Vec`s in a size range, using a dependent strategy to create elements.
/// See `vec_records_strategy`.
#[derive(Debug)]
pub struct VecRecordsStrategy<T> {
    element: T,
    size: SizeRange,
}

impl<T> Strategy for VecRecordsStrategy<T>
where
    T: Strategy,
    T::Value: Clone + Debug,
{
    type Value = <<Self as Strategy>::Tree as ValueTree>::Value;
    type Tree = RecordsValueTree<Vec<T::Value>>;

    fn new_tree(&self, runner: &mut TestRunner) -> NewTree<Self> {
        let (min_size, max_size) = self.size.start_end_incl();
        let init_size = sample_uniform_incl(runner, min_size, max_size);
        let mut elements = Vec::with_capacity(init_size);
        while elements.len() < init_size {
            elements.push(self.element.new_tree(runner)?.current());
        }

        Ok(RecordsValueTree::new(min_size, elements))
    }
}

/// A `ValueTree` implementation which searches over any `Records` to find the minimum
/// set of elements needed to produce a failure.
///
/// For any set of N records, there are `2^N` candidate subsets which could be the minimum failing
/// set. Rather than search for that, we choose to shrink the space quickly and find
/// a reasonably small input after a hopefully small number of iterations.
///
/// The algorithm used is to partition the records into a fixed number of pieces.
/// For each piece P, run the test with all of the records excluding P.
/// A failure means that piece P is not necessary for the test to fail.
/// After enumerating each of the P pieces, shrink the record set down to only the
/// pieces which passed when they were not included in the test input.
///
/// This algorithm does not precisely identify the minimum record set which fails,
/// since it reasons about contiguous chunks of records.  In the best case,
/// where exactly one record is responsible for a test failure, we will find it
/// after O(P * log_P N) iterations.  In the worst case, where there is exactly
/// one record in each of the initial P chunks, we will be very sad and not
/// shrink at all.
#[derive(Clone, Debug)]
pub struct RecordsValueTree<R> {
    min_records: usize,
    init: R,
    last_records_included: Option<Vec<usize>>,
    records_included: Vec<usize>,
    explore_results: Box<[Option<bool>]>,
    search: Option<ShrinkStep>,
}

impl<R> RecordsValueTree<R>
where
    R: Records,
{
    const CONTAINER_VALUE_TREE_DEFAULT_EXPLORE_CHUNKS: usize = 8;

    /// Initializes a value tree to choosing records from `init`.
    /// The number of records produced for each iteration is bounded by `min_records`.
    pub fn new(min_records: usize, init: R) -> Self {
        let nchunks = std::cmp::min(
            init.len(),
            Self::CONTAINER_VALUE_TREE_DEFAULT_EXPLORE_CHUNKS,
        );
        let records_included = (0..init.len()).collect::<Vec<usize>>();

        RecordsValueTree {
            min_records,
            init,
            last_records_included: None,
            records_included,
            explore_results: vec![None; nchunks].into_boxed_slice(),
            search: None,
        }
    }

    pub fn num_chunks(&self) -> usize {
        std::cmp::min(self.records_included.len(), self.explore_results.len())
    }

    /// Computes the bit mask for which records to include in the next iteration
    fn record_mask(&self) -> VarBitSet {
        let mut by_search = match self.search {
            None => VarBitSet::saturated(self.init.len()),
            Some(ShrinkStep::Explore(c)) => {
                let nchunks = std::cmp::max(1, self.num_chunks());

                let approx_chunk_len = self.records_included.len() / nchunks;

                if approx_chunk_len == 0 {
                    /* no records are included, we have shrunk down to empty */
                    VarBitSet::new_bitset(self.init.len())
                } else {
                    let mut record_mask =
                        VarBitSet::new_bitset(self.init.len());

                    let exclude_min = c * approx_chunk_len;
                    let exclude_max = if c + 1 == nchunks {
                        self.records_included.len()
                    } else {
                        (c + 1) * approx_chunk_len
                    };

                    for r in self.records_included[0..exclude_min]
                        .iter()
                        .chain(self.records_included[exclude_max..].iter())
                    {
                        record_mask.set(*r)
                    }

                    record_mask
                }
            }
            Some(ShrinkStep::Recur) | Some(ShrinkStep::Done) => {
                let mut record_mask = VarBitSet::new_bitset(self.init.len());
                for r in self.records_included.iter() {
                    record_mask.set(*r);
                }
                record_mask
            }
        };

        if by_search.count() < self.min_records {
            // Buffer with some extras because the strategy requires it.
            // We could probably do something smarter, but just choose the initial
            // records. Most likely the minimum is 1 anyway.
            let mut remaining = self.min_records - by_search.count();

            for i in 0..self.init.len() {
                if by_search.test(i) {
                    continue;
                }
                remaining -= 1;
                by_search.set(i);
                if remaining == 0 {
                    break;
                }
            }
        }
        by_search
    }

    /// Receive the latest test result and return whether further steps can be taken.
    fn explore_step(&mut self, failed: bool) -> bool {
        match self.search {
            None => {
                if failed {
                    /* failed on the initial input */
                    if self.init.is_empty() {
                        /* no way to shrink */
                        false
                    } else {
                        /* begin search */
                        self.search = Some(ShrinkStep::Explore(0));
                        true
                    }
                } else {
                    /* passed on the initial input, nothing to do */
                    false
                }
            }
            Some(ShrinkStep::Explore(c)) => {
                let nchunks = self.num_chunks();

                self.explore_results[c] = Some(failed);

                match (c + 1).cmp(&nchunks) {
                    Ordering::Less => {
                        /* advance to the next */
                        self.search = Some(ShrinkStep::Explore(c + 1));
                        true
                    }
                    Ordering::Greater => {
                        assert_eq!(nchunks, 0);
                        false
                    }
                    Ordering::Equal => {
                        /* finished exploring at this level, either recur or finish */
                        self.explore_level_finished()
                    }
                }
            }
            Some(ShrinkStep::Recur) => {
                if failed {
                    self.search = Some(ShrinkStep::Explore(0));
                } else {
                    /*
                     * This means that removing more than one chunk causes the
                     * test to no longer fail.
                     * Try again with a larger chunk size if possible
                     */
                    if self.explore_results.len() == 1 {
                        unreachable!()
                    }
                    let Some(last_records_included) =
                        self.last_records_included.take()
                    else {
                        unreachable!()
                    };
                    self.last_records_included = None;
                    self.records_included = last_records_included;
                    self.explore_results =
                        vec![None; self.explore_results.len() / 2]
                            .into_boxed_slice();
                }
                self.search = Some(ShrinkStep::Explore(0));
                true
            }
            Some(ShrinkStep::Done) => false,
        }
    }

    /// Called after exploring each of P chunks to choose the set
    /// of records for the next level of exploration.
    fn explore_level_finished(&mut self) -> bool {
        let nchunks = self.num_chunks();
        let approx_chunk_len = self.records_included.len() / nchunks;

        let new_records_included = {
            let mut new_records_included = vec![];
            for i in 0..nchunks {
                let chunk_min = i * approx_chunk_len;
                let chunk_max = if i + 1 == nchunks {
                    self.records_included.len()
                } else {
                    (i + 1) * approx_chunk_len
                };

                if !self.explore_results[i].take().unwrap() {
                    /* the test passed when chunk `i` was not included; keep it */
                    new_records_included.extend_from_slice(
                        &self.records_included[chunk_min..chunk_max],
                    );
                }
            }

            new_records_included
        };

        if new_records_included == self.records_included {
            /* everything was needed to pass */
            self.search = Some(ShrinkStep::Done);
        } else {
            self.last_records_included = Some(std::mem::replace(
                &mut self.records_included,
                new_records_included,
            ));
            self.search = Some(ShrinkStep::Recur);
        }
        /* run another round on the updated input */
        true
    }
}

impl<R> ValueTree for RecordsValueTree<R>
where
    R: Records,
{
    type Value = R;

    fn current(&self) -> Self::Value {
        let record_mask = self.record_mask();
        self.init.filter(&record_mask)
    }

    fn simplify(&mut self) -> bool {
        self.explore_step(true)
    }

    fn complicate(&mut self) -> bool {
        self.explore_step(false)
    }
}

/// Tracks the last step taken for the container shrinking.
#[derive(Clone, Debug)]
enum ShrinkStep {
    Explore(usize),
    Recur,
    Done,
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::StrategyExt;
    use crate::meta::*;

    #[test]
    fn shrink_convergence_u64() {
        let strat = vec_records_strategy(any::<u64>(), 0..=1024);

        let mut runner = TestRunner::new(Default::default());

        for _ in 0..runner.config().cases {
            let mut tree = strat.new_tree(&mut runner).unwrap();

            while tree.simplify() {}

            let convergence = tree.current();
            assert!(
                convergence.is_empty(),
                "Value tree converged to: {convergence:?}"
            );
        }
    }

    #[test]
    fn shrink_convergence_u32_u32() {
        let strat =
            vec_records_strategy((any::<u32>(), any::<u32>()), 0..=1024)
                .prop_map(|v| v.into_iter().unzip::<_, _, Vec<_>, Vec<_>>());

        let mut runner = TestRunner::new(Default::default());

        for _ in 0..runner.config().cases {
            let mut tree: RecordsValueTree<(Vec<u32>, Vec<u32>)> =
                RecordsValueTree::new(
                    0,
                    strat.new_tree(&mut runner).unwrap().current(),
                );

            while tree.simplify() {}

            let convergence = tree.current();
            assert!(
                convergence.is_empty(),
                "Value tree converged to: {convergence:?}"
            );
        }
    }

    fn do_arbitrary_shrink_search(
        rvt: &mut RecordsValueTree<Vec<u64>>,
        search: Vec<ShrinkAction>,
    ) {
        for step in search {
            step.apply(rvt);

            let records = rvt.current();
            assert!(records.len() >= rvt.min_records);
        }
    }

    proptest! {
        #[test]
        fn arbitrary_shrink_search(
            mut rvt in vec_records_strategy(any::<u64>().no_shrink(), 0..=1024).prop_indirect(),
            sequence in ShrinkSequenceStrategy {
                max_length: 1024
            },
        ) {
            do_arbitrary_shrink_search(&mut rvt, sequence)
        }

        #[test]
        fn arbitrary_shrink_search_min_size(
            mut rvt in (1..=32usize).prop_flat_map(|min| vec_records_strategy(any::<u64>(), min..=1024).prop_indirect()),
            sequence in ShrinkSequenceStrategy {
                max_length: 1024
            },
        ) {
            do_arbitrary_shrink_search(&mut rvt, sequence)
        }
    }
}
