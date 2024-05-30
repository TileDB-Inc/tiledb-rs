use std::fmt;

use proptest::strategy::ValueTree;

pub trait BinSearch {
    type Value: fmt::Debug;

    fn current(&self) -> Self::Value;
    fn simplify(&mut self) -> bool;
    fn complicate(&mut self) -> bool;
    fn update_range(
        &self,
        simplify: bool,
        range: [Self::Value; 2],
    ) -> [Self::Value; 2];
    fn to_string(&self) -> String;
}

impl<V: fmt::Debug> fmt::Debug for dyn BinSearch<Value = V> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

/// This struct is used to search for the maximum required value in a set.
#[derive(Debug)]
pub struct MaxSearch {
    min: usize,
    mid: usize,
    max: usize,
}

impl MaxSearch {
    pub fn new(min: usize, max: usize) -> Self {
        assert!(min <= max);
        Self { min, mid: max, max }
    }
}

impl BinSearch for MaxSearch {
    type Value = usize;

    fn current(&self) -> Self::Value {
        self.mid
    }

    fn simplify(&mut self) -> bool {
        assert!(self.min <= self.mid && self.mid <= self.max);

        let prev_mid = self.mid;
        self.max = self.mid;
        self.mid =
            (self.min + (self.max - self.min) / 2).clamp(self.min, self.max);

        assert!(self.min <= self.mid && self.mid <= self.max);
        prev_mid != self.mid
    }

    fn complicate(&mut self) -> bool {
        assert!(self.min <= self.mid && self.mid <= self.max);

        let prev_mid = self.mid;
        self.min = self.mid;
        self.mid =
            (self.min + (self.max - self.min) / 2).clamp(self.min, self.max);

        assert!(self.min <= self.mid && self.mid <= self.max);
        prev_mid != self.mid
    }

    fn update_range(
        &self,
        simplify: bool,
        range: [Self::Value; 2],
    ) -> [Self::Value; 2] {
        if simplify {
            [range[0], self.mid]
        } else {
            [range[0], self.max]
        }
    }

    fn to_string(&self) -> String {
        format!("{:#?}", self)
    }
}

/// This struct is used to search for the minimum required value in a set.
#[derive(Debug)]
pub struct MinSearch {
    min: usize,
    mid: usize,
    max: usize,
}

impl MinSearch {
    pub fn new(min: usize, max: usize) -> Self {
        assert!(min <= max);
        Self { min, mid: min, max }
    }
}

impl BinSearch for MinSearch {
    type Value = usize;

    fn current(&self) -> Self::Value {
        self.mid
    }

    fn simplify(&mut self) -> bool {
        assert!(self.min <= self.mid && self.mid <= self.max);

        let prev_mid = self.mid;
        self.min = self.mid;
        self.mid =
            (self.min + (self.max - self.min) / 2).clamp(self.min, self.max);

        assert!(self.min <= self.mid && self.mid <= self.max);
        prev_mid != self.mid
    }

    fn complicate(&mut self) -> bool {
        assert!(self.min <= self.mid && self.mid <= self.max);

        let prev_mid = self.mid;
        self.max = self.mid;
        self.mid =
            (self.min + (self.max - self.min) / 2).clamp(self.min, self.max);

        assert!(self.min <= self.mid && self.mid <= self.max);
        prev_mid != self.mid
    }

    fn update_range(
        &self,
        simplify: bool,
        range: [Self::Value; 2],
    ) -> [Self::Value; 2] {
        if simplify {
            [self.mid, range[1]]
        } else {
            [self.min, range[1]]
        }
    }

    fn to_string(&self) -> String {
        format!("{:#?}", self)
    }
}

#[derive(Debug)]
pub struct MinSet<T: Clone + fmt::Debug> {
    elements: Vec<T>,
    lows: Vec<usize>,
    highs: Vec<usize>,
    range: [usize; 2],
    search: Box<dyn BinSearch<Value = usize>>,
    find_max: bool,
    complicated: bool,
    none_required: bool,
}

impl<T: Clone + fmt::Debug> MinSet<T> {
    pub fn new(elements: Vec<T>) -> Self {
        assert!(
            !elements.is_empty(),
            "MinSet requires at least one element."
        );
        let max_idx = elements.len() - 1;
        Self {
            elements,
            lows: Vec::new(),
            highs: Vec::new(),
            range: [0, max_idx],
            search: Box::new(MaxSearch::new(0, max_idx)),
            find_max: true,
            complicated: false,
            none_required: false,
        }
    }

    pub fn update(&mut self, simplify: bool) -> bool {
        // This first check is actually one of the terminating conditions. Its
        // marked below when we would get here.
        if self.none_required && simplify {
            // If we tried the empty set and the test still failed, we're the
            // empty set and done searching.
            println!("Result is empty set");
            return false;
        } else if self.none_required && !simplify {
            // We tried the empty set and the test passed. This means we're
            // a single element set and done searching.
            self.none_required = false;
            println!("Result is first set element.");
            return false;
        }

        let range = self.search.update_range(simplify, self.range);
        println!("RANGE: {:?} {:?}", self.range, range);

        // If our range still contains elements between the bounds, it means
        // we either have to start the minimum bound search or add both
        // bounds to our lists and resume search for the next maximum bound.
        if self.find_max && range[1] - range[0] > 1 {
            // Start the search for the minimum bound
            self.range = range;
            self.search =
                Box::new(MinSearch::new(self.range[0], self.range[1]));
            self.find_max = false;
            println!("Start min range bound search");
            return true;
        } else if range[1] - range[0] > 1 {
            // We have found the minimum bound. Shove the existing bounds into
            // our lists and continue searching for the next maximum.
            self.lows.push(self.range[0]);
            self.highs.push(self.range[1]);

            // Update our range to be inside the previous range.
            let min = (self.range[0] + 1).clamp(self.range[0], self.range[1]);
            let max = (self.range[1] - 1).clamp(self.range[0], self.range[1]);
            self.range = [min, max];
            self.search =
                Box::new(MaxSearch::new(self.range[0], self.range[1]));
            self.find_max = true;
            println!("Found bounds, starting next search phase.");
            return true;
        }

        // Next up to check. If we started with `[N, N+1]` once we get to this
        // step we switch gears and try probing either of the two bounds. First
        // we'll return a range of 1 for the upper bound
        if self.range[1] - self.range[0] == 1 {
            self.range = [self.range[1], self.range[1]];
            self.search =
                Box::new(MaxSearch::new(self.range[0], self.range[1]));
            self.find_max = true;
            println!("Checking [N+1, N+1]");
            return true;
        }

        // At this point we have two possibilities. We've either just tried
        // the [N+1, N+1] range and the test passed which means [N] is not
        // a set requirement, or we're also possibly checking a single element
        // set. Either way, the next step is to check if the empty set passes
        // or not. The result of the next probe is handled in the first check
        // in this method.
        if simplify && range[1] - range[0] == 0 {
            // There are two main cases here, either we've found an input that
            // has an empty set of requirements, or we had an even number of
            // required elements and have no exhaustively checked all of the
            // others in between the last two bounds. We just check if the
            // bound lists are empty or not.
            if self.lows.is_empty() {
                assert!(self.highs.is_empty());
            } else {
                assert_eq!(self.lows.len(), self.highs.len());
            }

            // TODO: Rename this to something like "ignore_range"
            self.none_required = true;

            // If we have any bounds, we're done searching.
            if !self.lows.is_empty() {
                return false;
            }

            println!("Checking empty set");
            return true;
        } else if !simplify && self.find_max && range[1] - range[0] == 0 {
            // If we have a single element set that's been asked to be
            // complicated, it means that we've just tried the [N+1, N+1] set
            // so now we should try the [N, N].

            // We should only be able to get to this state if we previously
            // encounter a [N, N+1] range.
            assert!(self.range[0] > 0);

            self.range = [self.range[0] - 1, self.range[0] - 1];
            self.search =
                Box::new(MinSearch::new(self.range[0], self.range[1]));
            self.find_max = false;
            println!("Next is [N] set");
            return true;
        } else if !simplify && !self.find_max && range[1] - range[0] == 0 {
            // The [N] set just failed, which means we have identified that
            // both N, and N+1 are required.
            self.range = [self.range[0], self.range[0] + 1];
            return false;
        }

        // None of our special case conditions have applied. If the range has
        // changed, apply it and update.
        if self.range != range {
            self.range = range;
            self.update(simplify)
        } else {
            false
        }
    }
}

impl<T: Clone + fmt::Debug> ValueTree for MinSet<T> {
    type Value = Vec<T>;

    fn current(&self) -> Self::Value {
        println!("Previous range: {:#?}", self.range);
        let curr_range = self.search.update_range(true, self.range);
        println!("Current range: {:#?}", curr_range);

        let mut ret = Vec::new();
        for idx in self.lows.iter() {
            ret.push(self.elements[*idx].clone());
        }

        // I really need to change this name.
        if !self.none_required {
            for idx in curr_range[0]..=curr_range[1] {
                ret.push(self.elements[idx].clone());
            }
        }

        for idx in self.highs.iter().rev() {
            ret.push(self.elements[*idx].clone());
        }

        println!("Return: {:#?}", ret);

        ret
    }

    fn simplify(&mut self) -> bool {
        assert!(self.range[0] <= self.range[1]);

        if self.search.simplify() {
            println!("Simplified!");
            true
        } else {
            println!("Simply updated!");
            self.update(true)
        }
    }

    fn complicate(&mut self) -> bool {
        assert!(self.range[0] <= self.range[1]);

        // Mark whether we have ever needed to complicate our current value.
        // This means that at least one element is required for the test
        // to fail.
        self.complicated = true;

        if self.search.complicate() {
            println!("Complicated");
            true
        } else {
            println!("Complicated update");
            self.update(false)
        }
    }
}
