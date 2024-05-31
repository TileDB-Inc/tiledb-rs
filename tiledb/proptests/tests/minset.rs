use std::collections::HashSet;

use proptest::collection::vec;
use proptest::prelude::*;
use proptest::sample::Index;
use proptest::strategy::ValueTree;

use tiledb_proptests::minset::MinSet;

#[test]
fn minset_correctness() {
    let cfg = ProptestConfig::with_cases(25);

    let size = 1usize..4096;
    let size_and_indices =
        size.prop_flat_map(|size| (Just(size), vec(any::<Index>(), 0..size)));

    proptest!(cfg, move |((num_values, required) in size_and_indices)| {
        // A vector where each value is the corresponding index. I.e.,
        // [0, 1, 2, 3, ...]
        let values = (0usize..num_values).collect::<Vec<_>>();
        let required = required
            .iter()
            .map(|idx| idx.index(num_values))
            .collect::<HashSet<_>>();

        let mut minset = MinSet::new(values.clone());

        if minset.simplify() {
            loop {
                let found = minset.current().into_iter().collect::<HashSet<_>>();

                // The logic on whether a "test" would pass with the returned
                // set elements if fairly subtle. Though breaking it down, there
                // are three main bits of logic we have to consider.

                // First, the the required elements set is empty, that means
                // that the test will always fail.
                let always_fail = required.is_empty();

                // Second, if any of the elements in required are missing, then
                // the test would pass. We can do this by checking that the
                // intersection of the two sets matches the required set.
                let intersection = found.intersection(&required).cloned().collect::<HashSet<_>>();
                let meets_requirements = intersection == required;

                // Combining all of those into one happy little boolean
                let test_failed = always_fail || meets_requirements;

                // And invert it so that our simplify/complicate logic matches
                // what happens in the proptest TestRunner's shrink method.
                let test_passed = !test_failed;

                if test_passed {
                    if !minset.complicate() {
                        break;
                    }
                } else if !minset.simplify() {
                    break;
                }
            }
        }

        // The actual test is whether minset found exactly the required
        // set of elements.
        let mut required = required.into_iter().collect::<Vec<_>>();
        required.sort();

        assert_eq!(minset.current(), required);
    });
}
