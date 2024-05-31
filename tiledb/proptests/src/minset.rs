use std::fmt;

use proptest::strategy::ValueTree;

pub trait BinSearch {
    fn has_next(&self) -> bool;
    fn current(&self) -> Vec<usize>;

    fn simplify(&mut self) -> bool;
    fn simplify_next(&self) -> Box<dyn BinSearch>;

    fn complicate(&mut self) -> bool;
    fn complicate_next(&self) -> Box<dyn BinSearch>;

    fn to_string(&self) -> String;
}

impl fmt::Debug for dyn BinSearch {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

/// This struct is used to search a two element range.
#[derive(Debug)]
pub struct SingleSearch {
    lows: Vec<usize>,
    highs: Vec<usize>,
    range: [usize; 2],
    include: bool,
}

impl SingleSearch {
    pub fn new(lows: Vec<usize>, highs: Vec<usize>, range: [usize; 2]) -> Self {
        assert!(range[0] == range[1]);
        Self {
            lows,
            highs,
            range,
            include: false,
        }
    }
}

impl BinSearch for SingleSearch {
    fn has_next(&self) -> bool {
        false
    }

    fn current(&self) -> Vec<usize> {
        let mut ret = Vec::new();

        for idx in self.lows.iter() {
            ret.push(*idx);
        }

        if self.include {
            ret.push(self.range[0]);
        }

        for idx in self.highs.iter().rev() {
            ret.push(*idx);
        }

        ret
    }

    fn simplify(&mut self) -> bool {
        false
    }

    fn simplify_next(&self) -> Box<dyn BinSearch> {
        panic!("Invalid simplify next.");
    }

    fn complicate(&mut self) -> bool {
        assert!(self.range[0] == self.range[1]);
        assert!(!self.include);
        self.include = true;
        false
    }

    fn complicate_next(&self) -> Box<dyn BinSearch> {
        panic!("Invalid complicate next.");
    }

    fn to_string(&self) -> String {
        format!("{:#?}", self)
    }
}

/// This struct is used to search a two element range.
#[derive(Debug)]
pub struct PairSearch {
    lows: Vec<usize>,
    highs: Vec<usize>,
    range: [usize; 2],
    required: [Option<bool>; 2],
}

impl PairSearch {
    pub fn new(lows: Vec<usize>, highs: Vec<usize>, range: [usize; 2]) -> Self {
        assert!(range[1] - range[0] == 1);
        Self {
            lows,
            highs,
            range,
            required: [None, None],
        }
    }
}

impl BinSearch for PairSearch {
    fn has_next(&self) -> bool {
        false
    }

    fn current(&self) -> Vec<usize> {
        let mut ret = Vec::new();

        for idx in self.lows.iter() {
            ret.push(*idx);
        }

        if self.required[0] != Some(false) {
            ret.push(self.range[0]);
        }

        if self.required[1] != Some(false) {
            ret.push(self.range[1]);
        }

        for idx in self.highs.iter().rev() {
            ret.push(*idx);
        }

        ret
    }

    fn simplify(&mut self) -> bool {
        assert!(self.range[1] - self.range[0] == 1);
        match self.required {
            [None, None] => {
                self.required = [Some(false), None];
                true
            }
            [Some(false), None] => {
                self.required = [Some(false), Some(false)];
                true
            }
            [Some(true), None] => {
                self.required = [Some(true), Some(false)];
                true
            }
            [Some(_), Some(_)] => false,
            _ => panic!("Whoops!"),
        }
    }

    fn simplify_next(&self) -> Box<dyn BinSearch> {
        panic!("Invalid simplify next.");
    }

    fn complicate(&mut self) -> bool {
        assert!(self.range[1] - self.range[0] == 1);
        match self.required {
            [None, None] => {
                self.required = [Some(true), None];
                true
            }
            [Some(false), None] => {
                self.required = [Some(true), None];
                true
            }
            [Some(true), None] => {
                self.required = [Some(true), Some(true)];
                true
            }
            [Some(false), Some(false)] => {
                self.required = [Some(false), Some(true)];
                false
            }
            [Some(true), Some(false)] => {
                self.required = [Some(true), Some(true)];
                false
            }
            _ => panic!("Oh no!"),
        }
    }

    fn complicate_next(&self) -> Box<dyn BinSearch> {
        panic!("Invalid complicate next.");
    }

    fn to_string(&self) -> String {
        format!("{:#?}", self)
    }
}

/// This struct is used to search for the maximum required value in a set.
#[derive(Debug)]
pub struct MaxSearch {
    lows: Vec<usize>,
    highs: Vec<usize>,
    range: [usize; 2],
    min: usize,
    mid: usize,
    max: usize,
}

impl MaxSearch {
    pub fn new(lows: Vec<usize>, highs: Vec<usize>, range: [usize; 2]) -> Self {
        assert!(range[1] - range[0] > 1);
        Self {
            lows,
            highs,
            range,
            min: range[0],
            mid: range[1],
            max: range[1],
        }
    }
}

impl BinSearch for MaxSearch {
    fn has_next(&self) -> bool {
        true
    }

    fn current(&self) -> Vec<usize> {
        let mut ret = Vec::new();
        for idx in self.lows.iter() {
            ret.push(*idx);
        }

        for idx in self.range[0]..=self.mid {
            ret.push(idx);
        }

        for idx in self.highs.iter().rev() {
            ret.push(*idx);
        }

        ret
    }

    fn simplify(&mut self) -> bool {
        assert!(self.min <= self.mid && self.mid <= self.max);

        let prev = self.mid;
        self.max = self.mid;
        self.mid =
            (self.min + (self.max - self.min) / 2).clamp(self.min, self.max);

        assert!(self.min <= self.mid && self.mid <= self.max);

        self.mid != prev
    }

    fn simplify_next(&self) -> Box<dyn BinSearch> {
        let lows = self.lows.clone();
        let highs = self.highs.clone();
        let range = [self.range[0], self.mid];
        if range[1] - range[0] == 0 {
            Box::new(SingleSearch::new(lows, highs, range))
        } else if range[1] - range[0] == 1 {
            Box::new(PairSearch::new(lows, highs, range))
        } else {
            Box::new(MinSearch::new(lows, highs, range))
        }
    }

    fn complicate(&mut self) -> bool {
        assert!(self.min <= self.mid && self.mid <= self.max);

        let prev = self.mid;
        self.min = self.mid;
        self.mid =
            (self.min + (self.max - self.min) / 2).clamp(self.min, self.max);

        assert!(self.min <= self.mid && self.mid <= self.max);

        self.mid != prev
    }

    fn complicate_next(&self) -> Box<dyn BinSearch> {
        let lows = self.lows.clone();
        let highs = self.highs.clone();
        let range = [self.range[0], self.max];
        if range[1] - range[0] == 0 {
            Box::new(SingleSearch::new(lows, highs, range))
        } else if range[1] - range[0] == 1 {
            Box::new(PairSearch::new(lows, highs, range))
        } else {
            Box::new(MinSearch::new(lows, highs, range))
        }
    }

    fn to_string(&self) -> String {
        format!("{:#?}", self)
    }
}

/// This struct is used to search for the minimum required value in a set.
#[derive(Debug)]
pub struct MinSearch {
    lows: Vec<usize>,
    highs: Vec<usize>,
    range: [usize; 2],
    min: usize,
    mid: usize,
    max: usize,
}

impl MinSearch {
    pub fn new(lows: Vec<usize>, highs: Vec<usize>, range: [usize; 2]) -> Self {
        assert!(range[1] - range[0] > 1);
        Self {
            lows,
            highs,
            range,
            min: range[0],
            mid: range[0],
            max: range[1],
        }
    }
}

impl BinSearch for MinSearch {
    fn has_next(&self) -> bool {
        true
    }

    fn current(&self) -> Vec<usize> {
        let mut ret = Vec::new();

        for idx in self.lows.iter() {
            ret.push(*idx);
        }

        for idx in self.mid..=self.range[1] {
            ret.push(idx);
        }

        for idx in self.highs.iter().rev() {
            ret.push(*idx);
        }

        ret
    }

    fn simplify(&mut self) -> bool {
        assert!(self.min <= self.mid && self.mid <= self.max);

        let prev = self.mid;
        self.min = self.mid;
        self.mid =
            (self.min + (self.max - self.min) / 2).clamp(self.min, self.max);

        assert!(self.min <= self.mid && self.mid <= self.max);

        self.mid != prev
    }

    fn simplify_next(&self) -> Box<dyn BinSearch> {
        let mut lows = self.lows.clone();
        let mut highs = self.highs.clone();
        let range = [self.mid, self.range[1]];
        if range[1] - range[0] == 0 {
            Box::new(SingleSearch::new(lows, highs, range))
        } else if range[1] - range[0] == 1 {
            Box::new(PairSearch::new(lows, highs, range))
        } else {
            // We've found two required elements with one or more elements
            // between them. Push the min and max bounds into our lists and
            // then continue searching between them.
            assert!(range[1] - range[0] > 1);

            lows.push(range[0]);
            highs.push(range[1]);

            let range = [range[0] + 1, range[1] - 1];
            assert!(range[0] <= range[1]);

            if range[1] - range[0] == 0 {
                Box::new(SingleSearch::new(lows, highs, range))
            } else if range[1] - range[0] == 1 {
                Box::new(PairSearch::new(lows, highs, range))
            } else {
                Box::new(MaxSearch::new(lows, highs, range))
            }
        }
    }

    fn complicate(&mut self) -> bool {
        assert!(self.min <= self.mid && self.mid <= self.max);

        let prev = self.mid;
        self.max = self.mid;
        self.mid =
            (self.min + (self.max - self.min) / 2).clamp(self.min, self.max);

        assert!(self.min <= self.mid && self.mid <= self.max);

        self.mid != prev
    }

    fn complicate_next(&self) -> Box<dyn BinSearch> {
        let lows = self.lows.clone();
        let highs = self.highs.clone();
        let range = [self.min, self.range[1]];
        if range[1] - range[0] == 0 {
            Box::new(SingleSearch::new(lows, highs, range))
        } else if range[1] - range[0] == 1 {
            Box::new(PairSearch::new(lows, highs, range))
        } else {
            Box::new(MaxSearch::new(lows, highs, range))
        }
    }

    fn to_string(&self) -> String {
        format!("{:#?}", self)
    }
}

#[derive(Debug)]
pub struct MinSet<T: Clone + fmt::Debug> {
    elements: Vec<T>,
    search: Box<dyn BinSearch>,
    complicated: bool,
}

impl<T: Clone + fmt::Debug> MinSet<T> {
    pub fn new(elements: Vec<T>) -> Self {
        assert!(
            !elements.is_empty(),
            "MinSet requires at least one element."
        );
        let max_idx = elements.len() - 1;

        let lows = Vec::new();
        let highs = Vec::new();
        let range = [0, max_idx];
        let search: Box<dyn BinSearch> = if range[1] - range[0] == 0 {
            Box::new(SingleSearch::new(lows, highs, range))
        } else if range[1] - range[0] == 1 {
            Box::new(PairSearch::new(lows, highs, range))
        } else {
            Box::new(MaxSearch::new(lows, highs, range))
        };

        Self {
            elements,
            search,
            complicated: false,
        }
    }
}

impl<T: Clone + fmt::Debug> ValueTree for MinSet<T> {
    type Value = Vec<T>;

    fn current(&self) -> Self::Value {
        let indices = self.search.current();

        let mut ret = Vec::with_capacity(indices.len());
        for idx in indices.iter() {
            ret.push(self.elements[*idx].clone())
        }

        ret
    }

    fn simplify(&mut self) -> bool {
        if self.search.simplify() {
            return true;
        }

        if self.search.has_next() {
            self.search = self.search.simplify_next();
            return true;
        }

        false
    }

    fn complicate(&mut self) -> bool {
        // We may not need this in the future, but for now we track if we've
        // ever encountered a single complicate call as we can use that to
        // make some specific assertions because it means at least one element
        // is required for the test to fail.
        self.complicated = true;

        if self.search.complicate() {
            return true;
        }

        if self.search.has_next() {
            self.search = self.search.complicate_next();
            return true;
        }

        false
    }
}
