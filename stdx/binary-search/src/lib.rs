use std::ops::Range;

use num_traits::{FromPrimitive, ToPrimitive};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Bisect<T> {
    NeverTrue,
    UpperBound(T),
    AlwaysTrue,
}

/// A type which represents a searchable space of values.
pub trait Search {
    type Item;

    /// Performs an efficient search over the items of `self` to find the upper bound
    /// where `property` is true.
    ///
    /// `property` is some function which bisects the search space,
    /// returning `true` on each value in the first segment and `false` on each value in the second.
    fn upper_bound<F>(&self, property: F) -> Bisect<Self::Item>
    where
        F: Fn(&Self::Item) -> bool;
}

macro_rules! binary_search_impl {
    ($($ITYPE:ty),+) => {
        $(
        impl Search for Range<$ITYPE> {
            type Item = $ITYPE;

            fn upper_bound<F>(&self, property: F) -> Bisect<Self::Item>
            where
                F: Fn(&Self::Item) -> bool,
            {
                if self.is_empty() {
                    return Bisect::AlwaysTrue
                } else if self.start + 1 == self.end {
                    return if property(&self.start) {
                        Bisect::AlwaysTrue
                    } else {
                        Bisect::NeverTrue
                    }
                }
                let mut search = self.clone();
                while search.start + 1 < search.end {
                    let midpoint = midpoint(&search);
                    if property(&midpoint) {
                        search.start = midpoint;
                    } else {
                        search.end = midpoint;
                    }
                }
                if search.end == self.end {
                    Bisect::AlwaysTrue
                } else if property(&search.start) {
                    Bisect::UpperBound(search.start)
                } else {
                    Bisect::NeverTrue
                }
            }
        }
        )+
    };
}

fn midpoint<T>(range: &Range<T>) -> T
where
    T: Copy + FromPrimitive + ToPrimitive,
{
    T::from_i128(
        (range.start.to_i128().unwrap() + range.end.to_i128().unwrap()) / 2,
    )
    .unwrap()
}

binary_search_impl!(
    u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize
);

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    /// Performs a linear search to return the maximum value in the range
    /// for which a `property` is true
    fn linear_search<T, F>(range: Range<T>, property: F) -> Bisect<T>
    where
        Range<T>: Iterator<Item = T>,
        F: Fn(&T) -> bool,
    {
        let mut prev = None;
        for i in range {
            if property(&i) {
                prev = Some(i);
            } else if let Some(prev) = prev {
                return Bisect::UpperBound(prev);
            } else {
                return Bisect::NeverTrue;
            }
        }
        Bisect::AlwaysTrue
    }

    fn search_results<T, F>(
        range: Range<T>,
        property: F,
    ) -> (Bisect<T>, Bisect<T>)
    where
        Range<T>: Iterator<Item = T> + Search<Item = T>,
        T: Clone + PartialEq,
        F: Clone + Fn(&T) -> bool,
    {
        let linear_search_result =
            linear_search(range.clone(), property.clone());
        let binary_search_result = range.upper_bound(property.clone());

        (linear_search_result, binary_search_result)
    }

    #[test]
    fn example_simple_less_than() {
        let cmp = |value: &usize| *value < 5;

        for i in 0..10 {
            for j in i..10 {
                let (linear, binary) = search_results(i..j, &cmp);
                assert_eq!(linear, binary, "i..j = {:?}", i..j)
            }
        }
    }

    proptest! {
        #[test]
        fn proptest_simple_less_than(target in any::<usize>(), range in any::<Range<usize>>()) {
            match range.upper_bound(|value: &usize| *value < target) {
                Bisect::AlwaysTrue => assert!(range.end <= target),
                Bisect::NeverTrue => assert!(target < range.start),
                Bisect::UpperBound(bound) => {
                    assert_eq!(target - 1, bound);
                }
            }
        }

        #[test]
        fn proptest_search_compare(target in any::<u8>(), range in any::<Range<u8>>()) {
            let (linear, binary) = search_results(range, |value: &u8| *value < target);
            assert_eq!(linear, binary);
        }
    }
}
