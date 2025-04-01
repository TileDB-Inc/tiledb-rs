pub mod field;
pub mod write;

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;

use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::ops::Range;

use proptest::bits::{BitSetLike, VarBitSet};
use tiledb_common::datatype::physical::{BitsEq, BitsKeyAdapter, BitsOrd};
use tiledb_common::query::condition::*;

pub use self::field::FieldData;

/// Applies an action to the typed data within a query condition enum instance and a [FieldData] instance.
macro_rules! qc_thing_zip {
    ($qcenum:ident, $qcthing:expr, $qcbind:pat, $fieldthing:expr, $fieldbind:pat, $action:expr, $actionstr:expr) => {{
        match ($qcthing, $fieldthing) {
            ($qcenum::UInt8($qcbind), FieldData::UInt8($fieldbind)) => $action,
            ($qcenum::UInt16($qcbind), FieldData::UInt16($fieldbind)) => {
                $action
            }
            ($qcenum::UInt32($qcbind), FieldData::UInt32($fieldbind)) => {
                $action
            }
            ($qcenum::UInt64($qcbind), FieldData::UInt64($fieldbind)) => {
                $action
            }
            ($qcenum::Int8($qcbind), FieldData::Int8($fieldbind)) => $action,
            ($qcenum::Int16($qcbind), FieldData::Int16($fieldbind)) => $action,
            ($qcenum::Int32($qcbind), FieldData::Int32($fieldbind)) => $action,
            ($qcenum::Int64($qcbind), FieldData::Int64($fieldbind)) => $action,
            ($qcenum::Float32($qcbind), FieldData::Float32($fieldbind)) => {
                $action
            }
            ($qcenum::Float64($qcbind), FieldData::Float64($fieldbind)) => {
                $action
            }
            ($qcenum::String($qcbind), FieldData::VecUInt8($fieldbind)) => {
                $actionstr
            }
            _ => unreachable!(
                "Type mismatch: {:?} vs. {:?}",
                $qcthing, $fieldthing
            ),
        }
    }};
}

#[derive(Clone, Debug, PartialEq)]
pub struct Cells {
    enumeration_values: HashMap<String, FieldData>,
    fields: HashMap<String, FieldData>,
}

impl Cells {
    /// # Panics
    ///
    /// Panics if the fields do not all have the same number of cells.
    pub fn new(fields: HashMap<String, FieldData>) -> Self {
        let mut expect_len: Option<usize> = None;
        for (_, d) in fields.iter() {
            if let Some(expect_len) = expect_len {
                assert_eq!(d.len(), expect_len);
            } else {
                expect_len = Some(d.len())
            }
        }

        Cells {
            enumeration_values: Default::default(),
            fields,
        }
    }

    pub fn with_enumerations(
        self,
        enumeration_values: HashMap<String, FieldData>,
    ) -> Self {
        Cells {
            enumeration_values,
            ..self
        }
    }

    pub fn is_empty(&self) -> bool {
        self.fields.values().next().unwrap().is_empty()
    }

    pub fn len(&self) -> usize {
        self.fields.values().next().unwrap().len()
    }

    pub fn fields(&self) -> &HashMap<String, FieldData> {
        &self.fields
    }

    /// Joins `self.fields().get(name)` together with the enumeration for `name`,
    /// returning a column containing the indexed enumeration variants and a bit set indicating
    /// validity.
    ///
    /// The returned bit set is set at index `i` if the key was a valid index into the enumeration.
    pub fn field_resolve_enumeration(
        &self,
        name: &str,
    ) -> Option<(FieldData, VarBitSet)> {
        self.enumeration_values.get(name).map(|e| {
            let idx = typed_field_data_go!(
                self.fields.get(name).unwrap(),
                _DT,
                _keys,
                _keys.iter().map(|k| *k as usize).collect::<Vec<_>>(),
                unreachable!(),
                unreachable!(),
                unreachable!()
            );

            typed_field_data_go!(
                e,
                variants,
                #[allow(clippy::clone_on_copy)]
                {
                    let data = FieldData::from(
                        idx.iter()
                            .map(|i| {
                                variants
                                    .get(*i)
                                    .cloned()
                                    .unwrap_or_default()
                                    .clone()
                            })
                            .collect::<Vec<_>>(),
                    );
                    let valid = {
                        let mut valid = VarBitSet::saturated(idx.len());
                        idx.iter()
                            .enumerate()
                            .filter(|(_, i)| variants.get(**i).is_none())
                            .for_each(|(row, _)| valid.clear(row));
                        valid
                    };
                    (data, valid)
                }
            )
        })
    }

    /// Returns the domain (if any) of each field.
    pub fn domain(&self) -> Vec<(String, Option<tiledb_common::range::Range>)> {
        self.fields
            .iter()
            .map(|(k, v)| (k.to_owned(), v.domain()))
            .collect::<Vec<_>>()
    }

    /// Copies data from the argument.
    /// Overwrites data at common indices and extends `self` where necessary.
    pub fn copy_from(&mut self, cells: Self) {
        for (field, data) in cells.fields.into_iter() {
            match self.fields.entry(field) {
                Entry::Vacant(v) => {
                    v.insert(data);
                }
                Entry::Occupied(mut o) => {
                    let prev_write_data = o.get_mut();
                    typed_field_data_cmp!(
                        prev_write_data,
                        data,
                        _DT,
                        mine,
                        theirs,
                        {
                            if mine.len() <= theirs.len() {
                                *mine = theirs;
                            } else {
                                mine[0..theirs.len()]
                                    .clone_from_slice(theirs.as_slice());
                            }
                        },
                        unreachable!()
                    );
                }
            }
        }
    }

    /// Shortens the cells, keeping the first `len` records and dropping the rest.
    pub fn truncate(&mut self, len: usize) {
        for data in self.fields.values_mut() {
            data.truncate(len)
        }
    }

    /// Extends this cell data with the contents of another.
    ///
    /// # Panics
    ///
    /// Panics if the set of fields in `self` and `other` do not match.
    ///
    /// Panics if any field in `self` and `other` has a different type.
    pub fn extend(&mut self, other: Self) {
        let mut other = other;
        for (field, data) in self.fields.iter_mut() {
            let other_data = other.fields.remove(field).unwrap();
            data.extend(other_data);
        }
        assert_eq!(other.fields.len(), 0);
    }

    /// Returns a view over a slice of the cells,
    /// with a subset of the fields viewed as indicated by `keys`.
    /// This is useful for comparing a section of `self` to another `Cells` instance.
    pub fn view<'a>(
        &'a self,
        keys: &'a [String],
        slice: Range<usize>,
    ) -> CellsView<'a> {
        for k in keys.iter() {
            if !self.fields.contains_key(k) {
                panic!(
                    "Cannot construct view: key '{}' not found (fields are {:?})",
                    k,
                    self.fields.keys()
                )
            }
        }

        CellsView {
            cells: self,
            keys,
            slice,
        }
    }

    /// Returns a comparator for ordering indices into the cells.
    fn index_comparator<'a>(
        &'a self,
        keys: &'a [String],
    ) -> impl Fn(&usize, &usize) -> Ordering + 'a {
        move |l: &usize, r: &usize| -> Ordering {
            for key in keys.iter() {
                typed_field_data_go!(self.fields[key], ref data, {
                    match BitsOrd::bits_cmp(&data[*l], &data[*r]) {
                        Ordering::Less => return Ordering::Less,
                        Ordering::Greater => return Ordering::Greater,
                        Ordering::Equal => continue,
                    }
                })
            }
            Ordering::Equal
        }
    }

    /// Returns whether the cells are sorted according to `keys`. See `Self::sort`.
    pub fn is_sorted(&self, keys: &[String]) -> bool {
        let index_comparator = self.index_comparator(keys);
        for i in 1..self.len() {
            if index_comparator(&(i - 1), &i) == Ordering::Greater {
                return false;
            }
        }
        true
    }

    /// Sorts the cells using `keys`. If two elements are equal on the first item in `keys`,
    /// then they will be ordered using the second; and so on.
    /// May not preserve the order of elements which are equal for all fields in `keys`.
    pub fn sort(&mut self, keys: &[String]) {
        let mut idx = std::iter::repeat_n((), self.len())
            .enumerate()
            .map(|(i, _)| i)
            .collect::<Vec<usize>>();

        let idx_comparator = self.index_comparator(keys);
        idx.sort_by(idx_comparator);

        for data in self.fields.values_mut() {
            typed_field_data_go!(data, data, {
                let mut unsorted = std::mem::replace(
                    data,
                    vec![Default::default(); data.len()],
                );
                for i in 0..unsorted.len() {
                    data[i] = std::mem::take(&mut unsorted[idx[i]]);
                }
            });
        }
    }

    /// Returns a copy of the cells, sorted as if by `self.sort()`.
    pub fn sorted(&self, keys: &[String]) -> Self {
        let mut sorted = self.clone();
        sorted.sort(keys);
        sorted
    }

    /// Returns the list of offsets beginning each group, i.e. run of contiguous values on `keys`.
    ///
    /// This is best used with sorted cells, but that is not required.
    /// For each pair of offsets in the output, all cells in that index range are equal;
    /// and the adjacent cells outside of the range are not equal.
    pub fn identify_groups(&self, keys: &[String]) -> Option<Vec<usize>> {
        if self.is_empty() {
            return None;
        }
        let mut groups = vec![0];
        let mut icmp = 0;
        for i in 1..self.len() {
            let distinct = keys.iter().any(|k| {
                let v = self.fields().get(k).unwrap();
                typed_field_data_go!(v, cells, cells[i].bits_ne(&cells[icmp]))
            });
            if distinct {
                groups.push(i);
                icmp = i;
            }
        }
        groups.push(self.len());
        Some(groups)
    }

    /// Returns the number of distinct values grouped on `keys`
    pub fn count_distinct(&self, keys: &[String]) -> usize {
        if self.len() <= 1 {
            return self.len();
        }

        let key_cells = {
            let key_fields = self
                .fields
                .iter()
                .filter(|(k, _)| keys.contains(k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<HashMap<_, _>>();
            Cells::new(key_fields).sorted(keys)
        };

        let mut icmp = 0;
        let mut count = 1;

        for i in 1..key_cells.len() {
            let distinct = keys.iter().any(|k| {
                let v = key_cells.fields().get(k).unwrap();
                typed_field_data_go!(v, cells, cells[i].bits_ne(&cells[icmp]))
            });
            if distinct {
                icmp = i;
                count += 1;
            }
        }

        count
    }

    /// Returns a subset of the records using the bitmap to determine which are included
    pub fn filter(&self, set: &VarBitSet) -> Cells {
        Self::new(
            self.fields()
                .iter()
                .map(|(k, v)| (k.clone(), v.filter(set)))
                .collect::<HashMap<String, FieldData>>(),
        )
    }

    /// Returns a bitmap indicating for each record indicating whether that record passes
    /// `query_condition`.
    fn query_condition_bitmap(
        &self,
        query_condition: &QueryConditionExpr,
    ) -> VarBitSet {
        fn collect_bitmap(
            num_records: usize,
            idx: impl Iterator<Item = usize>,
        ) -> VarBitSet {
            let b = VarBitSet::new_bitset(num_records);
            collect_into_bitmap(b, idx)
        }

        fn collect_into_bitmap(
            mut b: VarBitSet,
            idx: impl Iterator<Item = usize>,
        ) -> VarBitSet {
            for i in idx {
                b.set(i);
            }
            b
        }

        match query_condition {
            QueryConditionExpr::Cond(predicate) => {
                match predicate {
                    Predicate::Equality(eq) => {
                        fn compare<T>(
                            cell: T,
                            op: EqualityOp,
                            literal: T,
                        ) -> bool
                        where
                            T: PartialOrd,
                        {
                            match op {
                                EqualityOp::Less => cell < literal,
                                EqualityOp::LessEqual => cell <= literal,
                                EqualityOp::Equal => cell == literal,
                                EqualityOp::GreaterEqual => cell >= literal,
                                EqualityOp::Greater => cell > literal,
                                EqualityOp::NotEqual => cell != literal,
                            }
                        }

                        let fdata = self.fields.get(eq.field()).unwrap();

                        // if there is an enumeration then the operation
                        // is applied on the index
                        let cmpvalue = if let Some(variants) =
                            self.enumeration_values.get(eq.field())
                        {
                            let maybe_index = qc_thing_zip!(
                                Literal,
                                eq.value(),
                                value,
                                variants,
                                variants,
                                {
                                    variants
                                        .iter()
                                        .enumerate()
                                        .filter(|(_, variant)| {
                                            *variant == value
                                        })
                                        .take(1)
                                        .map(|(i, _)| i)
                                        .next()
                                },
                                {
                                    variants
                                        .iter()
                                        .enumerate()
                                        .filter(|(_, variant)| {
                                            variant.as_slice()
                                                == value.as_bytes()
                                        })
                                        .take(1)
                                        .map(|(i, _)| i)
                                        .next()
                                }
                            );
                            if maybe_index.is_none() {
                                return match eq.operation() {
                                    EqualityOp::NotEqual => {
                                        VarBitSet::saturated(self.len())
                                    }
                                    _ => VarBitSet::new_bitset(self.len()),
                                };
                            }
                            typed_field_data_go!(
                                fdata,
                                _DT,
                                _,
                                Literal::from(maybe_index.unwrap() as _DT),
                                unreachable!(),
                                unreachable!(),
                                unreachable!()
                            )
                        } else {
                            eq.value().clone()
                        };

                        qc_thing_zip!(
                            Literal,
                            &cmpvalue,
                            literal,
                            fdata,
                            cells,
                            {
                                collect_bitmap(
                                    self.len(),
                                    cells
                                        .iter()
                                        .enumerate()
                                        .filter(|(_, c)| {
                                            compare(*c, eq.operation(), literal)
                                        })
                                        .map(|(i, _)| i),
                                )
                            },
                            {
                                collect_bitmap(
                                    self.len(),
                                    cells
                                        .iter()
                                        .enumerate()
                                        .filter(|(_, c)| {
                                            compare(
                                                c.as_slice(),
                                                eq.operation(),
                                                literal.as_bytes(),
                                            )
                                        })
                                        .map(|(i, _)| i),
                                )
                            }
                        )
                    }
                    Predicate::SetMembership(set) => {
                        let fdata = self.field_resolve_enumeration(set.field());
                        let (fdata, validity) =
                            if let Some((ref fdata, validity)) = fdata {
                                (fdata, validity)
                            } else {
                                (
                                    self.fields.get(set.field()).unwrap(),
                                    VarBitSet::saturated(self.len()),
                                )
                            };
                        let pred = qc_thing_zip!(
                            SetMembers,
                            set.members(),
                            members,
                            fdata,
                            cells,
                            {
                                let members =
                                    HashSet::<BitsKeyAdapter<_>>::from_iter(
                                        members
                                            .iter()
                                            .map(|m| BitsKeyAdapter(*m)),
                                    );
                                collect_bitmap(
                                    self.len(),
                                    cells
                                        .iter()
                                        .enumerate()
                                        .filter(|(_, c)| {
                                            match set.operation() {
                                                SetMembershipOp::In => members
                                                    .contains(&BitsKeyAdapter(
                                                        **c,
                                                    )),
                                                SetMembershipOp::NotIn => {
                                                    !members.contains(
                                                        &BitsKeyAdapter(**c),
                                                    )
                                                }
                                            }
                                        })
                                        .map(|(i, _)| i),
                                )
                            },
                            {
                                let members = HashSet::<Vec<u8>>::from_iter(
                                    members
                                        .iter()
                                        .map(|s| s.as_bytes().to_vec()),
                                );
                                collect_bitmap(
                                    self.len(),
                                    cells
                                        .iter()
                                        .enumerate()
                                        .filter(|(_, c)| {
                                            match set.operation() {
                                                SetMembershipOp::In => members
                                                    .contains(c.as_slice()),
                                                SetMembershipOp::NotIn => {
                                                    !members
                                                        .contains(c.as_slice())
                                                }
                                            }
                                        })
                                        .map(|(i, _)| i),
                                )
                            }
                        );
                        match set.operation() {
                            SetMembershipOp::In => intersect(validity, pred),
                            SetMembershipOp::NotIn => {
                                union(negate(validity), pred)
                            }
                        }
                    }
                    Predicate::Nullness(n) => {
                        match n.operation() {
                            NullnessOp::IsNull => {
                                // right now there are no nulls, so these are all false
                                VarBitSet::new_bitset(self.len())
                            }
                            NullnessOp::NotNull => {
                                // right now there are no nulls, so these are all true
                                VarBitSet::saturated(self.len())
                            }
                        }
                    }
                }
            }
            QueryConditionExpr::Comb { lhs, rhs, op } => {
                let lhs_passing = self.query_condition_bitmap(lhs);
                let rhs_passing = self.query_condition_bitmap(rhs);
                assert_eq!(lhs_passing.len(), rhs_passing.len());

                match op {
                    CombinationOp::And => intersect(lhs_passing, rhs_passing),
                    CombinationOp::Or => union(lhs_passing, rhs_passing),
                }
            }
            QueryConditionExpr::Negate(predicate) => {
                // core does not embed Negate in its expression trees,
                // instead it builds an expression tree of the negated expression,
                // so we shall evaluate the negated expression here to match
                // (Is this *correct* to do? In a binary logic system... probably.
                // In a ternary logic system... perhaps not)
                let negated_pred = predicate.negate();
                self.query_condition_bitmap(&negated_pred)
            }
        }
    }

    /// Returns a subset of the records which pass the provided query condition.
    pub fn query_condition(
        &self,
        query_condition: &QueryConditionExpr,
    ) -> Cells {
        self.filter(&self.query_condition_bitmap(query_condition))
    }

    /// Returns a subset of `self` containing only cells which have distinct values in `keys`
    /// such that `self.dedup(keys).count_distinct(keys) == self.len()`.
    /// The order of cells in the input is preserved and the
    /// first cell for each value of `keys` is preserved in the output.
    pub fn dedup(&self, keys: &[String]) -> Cells {
        if self.is_empty() {
            return self.clone();
        }

        let mut idx = (0..self.len()).collect::<Vec<usize>>();

        let idx_comparator = self.index_comparator(keys);
        idx.sort_by(idx_comparator);

        let mut icmp = 0;
        let mut preserve = VarBitSet::new_bitset(idx.len());
        preserve.set(idx[0]);

        for i in 1..idx.len() {
            let distinct = keys.iter().any(|k| {
                let v = self.fields.get(k).unwrap();
                typed_field_data_go!(
                    v,
                    field_cells,
                    field_cells[idx[i]].bits_ne(&field_cells[idx[icmp]])
                )
            });
            if distinct {
                icmp = i;
                preserve.set(idx[i]);
            }
        }

        self.filter(&preserve)
    }

    /// Returns a copy of `self` with only the fields in `fields`,
    /// or `None` if not all the requested fields are present.
    pub fn projection(&self, fields: &[&str]) -> Option<Cells> {
        let projection = fields
            .iter()
            .map(|f| {
                self.fields
                    .get(*f)
                    .map(|data| (f.to_string(), data.clone()))
            })
            .collect::<Option<HashMap<String, FieldData>>>()?;
        Some(Cells::new(projection))
    }

    /// Adds an additional field to `self`. Returns `true` if successful,
    /// i.e. the field data is valid for the current set of cells
    /// and there is not already a field for the key.
    pub fn add_field(&mut self, key: &str, values: FieldData) -> bool {
        if self.len() != values.len() {
            return false;
        }

        if self.fields.contains_key(key) {
            false
        } else {
            self.fields.insert(key.to_owned(), values);
            true
        }
    }
}

impl BitsEq for Cells {
    fn bits_eq(&self, other: &Self) -> bool {
        for (key, mine) in self.fields().iter() {
            if let Some(theirs) = other.fields().get(key) {
                if !mine.bits_eq(theirs) {
                    return false;
                }
            } else {
                return false;
            }
        }
        self.fields().keys().len() == other.fields().keys().len()
    }
}

pub struct StructuredCells {
    dimensions: Vec<usize>,
    cells: Cells,
}

impl StructuredCells {
    pub fn new(dimensions: Vec<usize>, cells: Cells) -> Self {
        let expected_cells: usize = dimensions.iter().cloned().product();
        assert_eq!(expected_cells, cells.len(), "Dimensions: {:?}", dimensions);

        StructuredCells { dimensions, cells }
    }

    pub fn num_dimensions(&self) -> usize {
        self.dimensions.len()
    }

    /// Returns the span of dimension `d`
    pub fn dimension_len(&self, d: usize) -> usize {
        self.dimensions[d]
    }

    pub fn into_inner(self) -> Cells {
        self.cells
    }

    pub fn slice(&self, slices: Vec<Range<usize>>) -> Self {
        assert_eq!(slices.len(), self.dimensions.len()); // this is doable but unimportant

        struct NextIndex<'a> {
            dimensions: &'a [usize],
            ranges: &'a [Range<usize>],
            cursors: Option<Vec<usize>>,
        }

        impl<'a> NextIndex<'a> {
            fn new(
                dimensions: &'a [usize],
                ranges: &'a [Range<usize>],
            ) -> Self {
                for r in ranges {
                    if r.is_empty() {
                        return NextIndex {
                            dimensions,
                            ranges,
                            cursors: None,
                        };
                    }
                }

                NextIndex {
                    dimensions,
                    ranges,
                    cursors: Some(
                        ranges.iter().map(|r| r.start).collect::<Vec<usize>>(),
                    ),
                }
            }

            fn compute(&self) -> usize {
                let Some(cursors) = self.cursors.as_ref() else {
                    unreachable!()
                };
                let mut index = 0;
                let mut scale = 1;
                for i in 0..self.dimensions.len() {
                    let i = self.dimensions.len() - i - 1;
                    index += cursors[i] * scale;
                    scale *= self.dimensions[i];
                }
                index
            }

            fn advance(&mut self) {
                let Some(cursors) = self.cursors.as_mut() else {
                    return;
                };
                for d in 0..self.dimensions.len() {
                    let d = self.dimensions.len() - d - 1;
                    if cursors[d] + 1 < self.ranges[d].end {
                        cursors[d] += 1;
                        return;
                    } else {
                        cursors[d] = self.ranges[d].start;
                    }
                }

                // this means that we reset the final dimension
                self.cursors = None;
            }
        }

        impl Iterator for NextIndex<'_> {
            type Item = usize;
            fn next(&mut self) -> Option<Self::Item> {
                if self.cursors.is_some() {
                    let index = self.compute();
                    self.advance();
                    Some(index)
                } else {
                    None
                }
            }
        }

        let mut v = VarBitSet::new_bitset(self.cells.len());

        NextIndex::new(self.dimensions.as_slice(), slices.as_slice())
            .for_each(|idx| v.set(idx));

        StructuredCells {
            dimensions: self.dimensions.clone(),
            cells: self.cells.filter(&v),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CellsView<'a> {
    cells: &'a Cells,
    keys: &'a [String],
    slice: Range<usize>,
}

impl<'b> PartialEq<CellsView<'b>> for CellsView<'_> {
    fn eq(&self, other: &CellsView<'b>) -> bool {
        // must have same number of values
        if self.slice.len() != other.slice.len() {
            return false;
        }

        for key in self.keys.iter() {
            let Some(mine) = self.cells.fields.get(key) else {
                // validated on construction
                unreachable!()
            };
            let Some(theirs) = other.cells.fields.get(key) else {
                return false;
            };

            typed_field_data_cmp!(
                mine,
                theirs,
                _DT,
                mine,
                theirs,
                if mine[self.slice.clone()] != theirs[other.slice.clone()] {
                    return false;
                },
                return false
            );
        }

        self.keys.len() == other.keys.len()
    }
}

fn intersect(mut b1: VarBitSet, b2: VarBitSet) -> VarBitSet {
    for bi in 0..b1.len() {
        if b1.test(bi) && b2.test(bi) {
            b1.set(bi);
        } else {
            b1.clear(bi);
        }
    }
    b1
}

fn union(mut b1: VarBitSet, b2: VarBitSet) -> VarBitSet {
    for bi in 0..b1.len() {
        if b1.test(bi) || b2.test(bi) {
            b1.set(bi);
        }
    }
    b1
}

fn negate(mut b1: VarBitSet) -> VarBitSet {
    for bi in 0..b1.len() {
        if b1.test(bi) {
            b1.clear(bi)
        } else {
            b1.set(bi)
        }
    }
    b1
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::rc::Rc;

    use proptest::prelude::*;
    use tiledb_common::Datatype;
    use tiledb_common::array::CellValNum;
    use tiledb_common::query::condition::strategy::Parameters as QueryConditionParameters;
    use tiledb_common::query::condition::strategy::{
        QueryConditionField, QueryConditionSchema,
    };
    use tiledb_common::query::condition::{EqualityOp, SetMembers};
    use tiledb_pod::array::schema::SchemaData;

    use super::*;
    use crate::strategy::{CellsParameters, CellsStrategySchema};

    struct CellsAsQueryConditionSchema {
        fields: Vec<CellsQueryConditionField>,
    }

    struct CellsQueryConditionField {
        cells: Rc<Cells>,
        name: String,
    }

    impl CellsAsQueryConditionSchema {
        pub fn new(cells: Rc<Cells>) -> CellsAsQueryConditionSchema {
            Self {
                fields: cells
                    .fields
                    .keys()
                    .map(|name| CellsQueryConditionField {
                        cells: Rc::clone(&cells),
                        name: name.to_owned(),
                    })
                    .collect(),
            }
        }
    }

    impl QueryConditionSchema for CellsAsQueryConditionSchema {
        /// Returns a list of fields which can have query conditions applied to them.
        fn fields(&self) -> Vec<&dyn QueryConditionField> {
            self.fields
                .iter()
                .map(|f| f as &dyn QueryConditionField)
                .collect::<Vec<&dyn QueryConditionField>>()
        }
    }

    impl QueryConditionField for CellsQueryConditionField {
        fn name(&self) -> &str {
            &self.name
        }

        fn equality_ops(&self) -> Option<Vec<EqualityOp>> {
            // all ops are supported
            None
        }

        fn domain(&self) -> Option<tiledb_common::range::Range> {
            self.cells.fields.get(&self.name).unwrap().domain()
        }

        fn set_members(&self) -> Option<SetMembers> {
            self.cells.enumeration_values.get(&self.name).and_then(|e| {
                typed_field_data_go!(
                    e,
                    _DT,
                    _members,
                    Some(SetMembers::from(_members.clone())),
                    {
                        // NB: this is Var, we could do String but it's just to test the test code
                        // so we will skip
                        None
                    }
                )
            })
        }
    }

    fn do_cells_extend(dst: Cells, src: Cells) {
        let orig_dst = dst.clone();
        let orig_src = src.clone();

        let mut dst = dst;
        dst.extend(src);

        for (fname, data) in dst.fields().iter() {
            let orig_dst = orig_dst.fields().get(fname).unwrap();
            let orig_src = orig_src.fields().get(fname).unwrap();

            typed_field_data_go!(data, dst, {
                assert_eq!(
                    *orig_dst,
                    FieldData::from(dst[0..orig_dst.len()].to_vec())
                );
                assert_eq!(
                    *orig_src,
                    FieldData::from(dst[orig_dst.len()..dst.len()].to_vec())
                );
                assert_eq!(dst.len(), orig_dst.len() + orig_src.len());
            });
        }

        // all Cells involved should have same set of fields
        assert_eq!(orig_dst.fields.len(), dst.fields.len());
        assert_eq!(orig_src.fields.len(), dst.fields.len());
    }

    fn do_cells_sort(cells: Cells, keys: Vec<String>) {
        let cells_sorted = cells.sorted(keys.as_slice());
        assert!(cells_sorted.is_sorted(keys.as_slice()));

        assert_eq!(cells.fields().len(), cells_sorted.fields().len());

        if cells.is_sorted(keys.as_slice()) {
            // running the sort should not have changed anything
            assert_eq!(cells, cells_sorted);
        }

        /*
         * We want to verify that the contents of the records are the
         * same before and after the sort. We can precisely do that
         * with a hash join, though it's definitely tricky to turn
         * the columnar data into rows, or we can approximate it
         * by sorting and comparing each column, which is not fully
         * precise but way easier.
         */
        for (fname, data) in cells.fields().iter() {
            let Some(data_sorted) = cells_sorted.fields().get(fname) else {
                unreachable!()
            };

            let orig_sorted = {
                let mut orig = data.clone();
                orig.sort();
                orig
            };
            let sorted_sorted = {
                let mut sorted = data_sorted.clone();
                sorted.sort();
                sorted
            };
            assert_eq!(orig_sorted, sorted_sorted);
        }
    }

    fn do_cells_slice_1d(cells: Cells, slice: Range<usize>) {
        let cells = StructuredCells::new(vec![cells.len()], cells);
        let sliced = cells.slice(vec![slice.clone()]).into_inner();
        let cells = cells.into_inner();

        assert_eq!(cells.fields().len(), sliced.fields().len());

        for (key, value) in cells.fields().iter() {
            let Some(sliced) = sliced.fields().get(key) else {
                unreachable!()
            };
            assert_eq!(
                value.slice(slice.start, slice.end - slice.start),
                *sliced
            );
        }
    }

    fn do_cells_slice_2d(
        cells: Cells,
        d1: usize,
        d2: usize,
        s1: Range<usize>,
        s2: Range<usize>,
    ) {
        let mut cells = cells;
        cells.truncate(d1 * d2);

        let cells = StructuredCells::new(vec![d1, d2], cells);
        let sliced = cells.slice(vec![s1.clone(), s2.clone()]).into_inner();
        let cells = cells.into_inner();

        assert_eq!(cells.fields().len(), sliced.fields().len());

        for (key, value) in cells.fields.iter() {
            let Some(sliced) = sliced.fields().get(key) else {
                unreachable!()
            };

            assert_eq!(s1.len() * s2.len(), sliced.len());

            typed_field_data_cmp!(
                value,
                sliced,
                _DT,
                value_data,
                sliced_data,
                {
                    for r in s1.clone() {
                        let value_start = (r * d2) + s2.start;
                        let value_end = (r * d2) + s2.end;
                        let value_expect = &value_data[value_start..value_end];

                        let sliced_start = (r - s1.start) * s2.len();
                        let sliced_end = (r + 1 - s1.start) * s2.len();
                        let sliced_cmp = &sliced_data[sliced_start..sliced_end];

                        assert_eq!(value_expect, sliced_cmp);
                    }
                },
                unreachable!()
            );
        }
    }

    fn do_cells_slice_3d(
        cells: Cells,
        d1: usize,
        d2: usize,
        d3: usize,
        s1: Range<usize>,
        s2: Range<usize>,
        s3: Range<usize>,
    ) {
        let mut cells = cells;
        cells.truncate(d1 * d2 * d3);

        let cells = StructuredCells::new(vec![d1, d2, d3], cells);
        let sliced = cells
            .slice(vec![s1.clone(), s2.clone(), s3.clone()])
            .into_inner();
        let cells = cells.into_inner();

        assert_eq!(cells.fields().len(), sliced.fields().len());

        for (key, value) in cells.fields.iter() {
            let Some(sliced) = sliced.fields.get(key) else {
                unreachable!()
            };

            assert_eq!(s1.len() * s2.len() * s3.len(), sliced.len());

            typed_field_data_cmp!(
                value,
                sliced,
                _DT,
                value_data,
                sliced_data,
                {
                    for z in s1.clone() {
                        for y in s2.clone() {
                            let value_start =
                                (z * d2 * d3) + (y * d3) + s3.start;
                            let value_end = (z * d2 * d3) + (y * d3) + s3.end;
                            let value_expect =
                                &value_data[value_start..value_end];

                            let sliced_start =
                                ((z - s1.start) * s2.len() * s3.len())
                                    + ((y - s2.start) * s3.len());
                            let sliced_end =
                                ((z - s1.start) * s2.len() * s3.len())
                                    + ((y + 1 - s2.start) * s3.len());
                            let sliced_cmp =
                                &sliced_data[sliced_start..sliced_end];

                            assert_eq!(value_expect, sliced_cmp);
                        }
                    }
                },
                unreachable!()
            );
        }
    }

    /// Assert that the output of [Cells::identify_groups] produces
    /// correct output for the given `keys`.
    fn do_cells_identify_groups(cells: Cells, keys: &[String]) {
        let Some(actual) = cells.identify_groups(keys) else {
            assert!(cells.is_empty());
            return;
        };

        for w in actual.windows(2) {
            let (start, end) = (w[0], w[1]);
            assert!(start < end);
        }

        for w in actual.windows(2) {
            let (start, end) = (w[0], w[1]);
            for k in keys.iter() {
                let f = cells.fields().get(k).unwrap();
                typed_field_data_go!(f, field_cells, {
                    for i in start..end {
                        assert!(field_cells[start].bits_eq(&field_cells[i]));
                    }
                })
            }
            if end < cells.len() {
                let some_ne = keys.iter().any(|k| {
                    let f = cells.fields().get(k).unwrap();
                    typed_field_data_go!(f, field_cells, {
                        field_cells[start].bits_ne(&field_cells[end])
                    })
                });
                assert!(some_ne);
            }
        }

        assert_eq!(Some(cells.len()), actual.last().copied());
    }

    fn do_cells_count_distinct_1d(cells: Cells) {
        for (key, field_cells) in cells.fields().iter() {
            let expect_count =
                typed_field_data_go!(field_cells, field_cells, {
                    let mut c = field_cells.clone();
                    c.sort_by(|l, r| l.bits_cmp(r));
                    c.dedup_by(|l, r| l.bits_eq(r));
                    c.len()
                });

            let keys_for_distinct = vec![key.clone()];
            let actual_count =
                cells.count_distinct(keys_for_distinct.as_slice());

            assert_eq!(expect_count, actual_count);
        }
    }

    fn do_cells_count_distinct_2d(cells: Cells) {
        let keys = cells.fields().keys().collect::<Vec<_>>();

        for i in 0..keys.len() {
            for j in 0..keys.len() {
                let expect_count = {
                    typed_field_data_go!(
                        cells.fields().get(keys[i]).unwrap(),
                        ki_cells,
                        {
                            typed_field_data_go!(
                                cells.fields().get(keys[j]).unwrap(),
                                kj_cells,
                                {
                                    let mut unique = HashMap::new();

                                    for r in 0..ki_cells.len() {
                                        let values = match unique
                                            .entry(BitsKeyAdapter(&ki_cells[r]))
                                        {
                                            Entry::Vacant(v) => {
                                                v.insert(HashSet::new())
                                            }
                                            Entry::Occupied(o) => o.into_mut(),
                                        };
                                        values.insert(BitsKeyAdapter(
                                            &kj_cells[r],
                                        ));
                                    }

                                    unique.values().flatten().count()
                                }
                            )
                        }
                    )
                };

                let keys_for_distinct = vec![keys[i].clone(), keys[j].clone()];
                let actual_count =
                    cells.count_distinct(keys_for_distinct.as_slice());

                assert_eq!(expect_count, actual_count);
            }
        }
    }

    fn do_cells_dedup(cells: Cells, keys: Vec<String>) {
        let dedup = cells.dedup(keys.as_slice());
        assert_eq!(dedup.len(), dedup.count_distinct(keys.as_slice()));

        // invariant check
        for field in dedup.fields().values() {
            assert_eq!(dedup.len(), field.len());
        }

        if dedup.is_empty() {
            assert!(cells.is_empty());
            return;
        } else if dedup.len() == cells.len() {
            assert_eq!(cells, dedup);
            return;
        }

        // check that order within the original cells is preserved
        assert_eq!(cells.view(&keys, 0..1), dedup.view(&keys, 0..1));

        let mut in_cursor = 1;
        let mut out_cursor = 1;

        while in_cursor < cells.len() && out_cursor < dedup.len() {
            if cells.view(&keys, in_cursor..(in_cursor + 1))
                == dedup.view(&keys, out_cursor..(out_cursor + 1))
            {
                out_cursor += 1;
                in_cursor += 1;
            } else {
                in_cursor += 1;
            }
        }
        assert_eq!(dedup.len(), out_cursor);
    }

    fn do_cells_projection(cells: Cells, keys: Vec<String>) {
        let proj = cells
            .projection(&keys.iter().map(|s| s.as_ref()).collect::<Vec<&str>>())
            .unwrap();

        for key in keys.iter() {
            let Some(field_in) = cells.fields().get(key) else {
                unreachable!()
            };
            let Some(field_out) = proj.fields().get(key) else {
                unreachable!()
            };

            assert_eq!(field_in, field_out);
        }

        // everything in `keys` is in the projection, there should be no other fields
        assert_eq!(keys.len(), proj.fields().len());
    }

    macro_rules! qc_equality_op_body {
        ($fname:ident, $op:ident, $cmp:tt) => {
            fn $fname (
                cells: Cells,
                field: String,
                pivot: usize,
            ) {
                let fdata = cells.fields().get(&field).unwrap();
                let qc = {
                    let f = QueryConditionExpr::field(field);
                    match fdata {
                        FieldData::UInt8(cells) => f.$op(cells[pivot]),
                        FieldData::UInt16(cells) => f.$op(cells[pivot]),
                        FieldData::UInt32(cells) => f.$op(cells[pivot]),
                        FieldData::UInt64(cells) => f.$op(cells[pivot]),
                        FieldData::Int8(cells) => f.$op(cells[pivot]),
                        FieldData::Int16(cells) => f.$op(cells[pivot]),
                        FieldData::Int32(cells) => f.$op(cells[pivot]),
                        FieldData::Int64(cells) => f.$op(cells[pivot]),
                        FieldData::Float32(cells) => f.$op(cells[pivot]),
                        FieldData::Float64(cells) => f.$op(cells[pivot]),
                        FieldData::VecUInt8(cells) => f
                            .$op(String::from_utf8(cells[pivot].to_vec())
                                .unwrap()),
                        _ => unreachable!(
                            "Invalid field for query condition: {:?}",
                            fdata
                        ),
                    }
                };

                let expect =
                    cells.filter(&typed_field_data_go!(fdata, cells, {
                        cells
                            .iter()
                            .enumerate()
                            .filter(|(_, c)| *c $cmp &cells[pivot])
                            .map(|(i, _)| i)
                            .collect::<VarBitSet>()
                    }));
                let cells_out = cells.query_condition(&qc);
                assert_eq!(expect, cells_out);
            }
        };
    }

    qc_equality_op_body!(do_query_condition_lt, lt, <);
    qc_equality_op_body!(do_query_condition_le, le, <=);
    qc_equality_op_body!(do_query_condition_eq, eq, ==);
    qc_equality_op_body!(do_query_condition_ge, ge, >=);
    qc_equality_op_body!(do_query_condition_gt, gt, >);
    qc_equality_op_body!(do_query_condition_ne, ne, !=);

    fn do_query_condition_set_membership_in(
        cells: Cells,
        field: String,
        member_idx: Vec<usize>,
    ) {
        let fdata = cells.fields().get(&field).unwrap();
        let qc = {
            let f = QueryConditionExpr::field(field);
            match fdata {
                FieldData::UInt8(cells) => f.is_in(
                    member_idx.iter().map(|i| cells[*i]).collect::<Vec<_>>(),
                ),
                FieldData::UInt16(cells) => f.is_in(
                    member_idx.iter().map(|i| cells[*i]).collect::<Vec<_>>(),
                ),
                FieldData::UInt32(cells) => f.is_in(
                    member_idx.iter().map(|i| cells[*i]).collect::<Vec<_>>(),
                ),
                FieldData::UInt64(cells) => f.is_in(
                    member_idx.iter().map(|i| cells[*i]).collect::<Vec<_>>(),
                ),
                FieldData::Int8(cells) => f.is_in(
                    member_idx.iter().map(|i| cells[*i]).collect::<Vec<_>>(),
                ),
                FieldData::Int16(cells) => f.is_in(
                    member_idx.iter().map(|i| cells[*i]).collect::<Vec<_>>(),
                ),
                FieldData::Int32(cells) => f.is_in(
                    member_idx.iter().map(|i| cells[*i]).collect::<Vec<_>>(),
                ),
                FieldData::Int64(cells) => f.is_in(
                    member_idx.iter().map(|i| cells[*i]).collect::<Vec<_>>(),
                ),
                FieldData::Float32(cells) => f.is_in(
                    member_idx.iter().map(|i| cells[*i]).collect::<Vec<_>>(),
                ),
                FieldData::Float64(cells) => f.is_in(
                    member_idx.iter().map(|i| cells[*i]).collect::<Vec<_>>(),
                ),
                FieldData::VecUInt8(cells) => f.is_in(
                    member_idx
                        .iter()
                        .map(|i| String::from_utf8(cells[*i].to_vec()).unwrap())
                        .collect::<Vec<_>>(),
                ),
                _ => unreachable!(
                    "Invalid field for query condition: {:?}",
                    fdata
                ),
            }
        };

        let expect = cells.filter(&typed_field_data_go!(fdata, cells, {
            cells
                .iter()
                .enumerate()
                .filter(|(_, c)| {
                    member_idx.iter().any(|i| cells[*i].bits_eq(*c))
                })
                .map(|(i, _)| i)
                .collect::<VarBitSet>()
        }));
        let cells_out = cells.query_condition(&qc);
        assert_eq!(expect, cells_out);
    }

    fn do_query_condition_set_membership_not_in(
        cells: Cells,
        field: String,
        member_idx: Vec<usize>,
    ) {
        let fdata = cells.fields().get(&field).unwrap();
        let qc = {
            let f = QueryConditionExpr::field(field);
            match fdata {
                FieldData::UInt8(cells) => f.not_in(
                    member_idx.iter().map(|i| cells[*i]).collect::<Vec<_>>(),
                ),
                FieldData::UInt16(cells) => f.not_in(
                    member_idx.iter().map(|i| cells[*i]).collect::<Vec<_>>(),
                ),
                FieldData::UInt32(cells) => f.not_in(
                    member_idx.iter().map(|i| cells[*i]).collect::<Vec<_>>(),
                ),
                FieldData::UInt64(cells) => f.not_in(
                    member_idx.iter().map(|i| cells[*i]).collect::<Vec<_>>(),
                ),
                FieldData::Int8(cells) => f.not_in(
                    member_idx.iter().map(|i| cells[*i]).collect::<Vec<_>>(),
                ),
                FieldData::Int16(cells) => f.not_in(
                    member_idx.iter().map(|i| cells[*i]).collect::<Vec<_>>(),
                ),
                FieldData::Int32(cells) => f.not_in(
                    member_idx.iter().map(|i| cells[*i]).collect::<Vec<_>>(),
                ),
                FieldData::Int64(cells) => f.not_in(
                    member_idx.iter().map(|i| cells[*i]).collect::<Vec<_>>(),
                ),
                FieldData::Float32(cells) => f.not_in(
                    member_idx.iter().map(|i| cells[*i]).collect::<Vec<_>>(),
                ),
                FieldData::Float64(cells) => f.not_in(
                    member_idx.iter().map(|i| cells[*i]).collect::<Vec<_>>(),
                ),
                FieldData::VecUInt8(cells) => f.not_in(
                    member_idx
                        .iter()
                        .map(|i| String::from_utf8(cells[*i].to_vec()).unwrap())
                        .collect::<Vec<_>>(),
                ),
                _ => unreachable!(
                    "Invalid field for query condition: {:?}",
                    fdata
                ),
            }
        };

        let expect = cells.filter(&typed_field_data_go!(fdata, cells, {
            cells
                .iter()
                .enumerate()
                .filter(|(_, c)| {
                    !member_idx.iter().any(|i| cells[*i].bits_eq(*c))
                })
                .map(|(i, _)| i)
                .collect::<VarBitSet>()
        }));
        let cells_out = cells.query_condition(&qc);
        assert_eq!(expect, cells_out);
    }

    fn strat_cells_with_qc_fields() -> impl Strategy<Value = Cells> {
        let mut cell_params = CellsParameters {
            schema: Some(CellsStrategySchema::Fields(HashMap::from([
                ("uint8".to_owned(), (Datatype::UInt8, CellValNum::single())),
                (
                    "uint16".to_owned(),
                    (Datatype::UInt16, CellValNum::single()),
                ),
                (
                    "uint32".to_owned(),
                    (Datatype::UInt32, CellValNum::single()),
                ),
                (
                    "uint64".to_owned(),
                    (Datatype::UInt64, CellValNum::single()),
                ),
                ("int8".to_owned(), (Datatype::Int8, CellValNum::single())),
                ("int16".to_owned(), (Datatype::Int16, CellValNum::single())),
                ("int32".to_owned(), (Datatype::Int32, CellValNum::single())),
                ("int64".to_owned(), (Datatype::Int64, CellValNum::single())),
                (
                    "float32".to_owned(),
                    (Datatype::Float32, CellValNum::single()),
                ),
                (
                    "float64".to_owned(),
                    (Datatype::Float64, CellValNum::single()),
                ),
                ("string".to_owned(), (Datatype::StringUtf8, CellValNum::Var)),
            ]))),
            ..Default::default()
        };
        cell_params.min_records = std::cmp::max(1, cell_params.min_records);
        any_with::<Cells>(cell_params).prop_map(|mut c| {
            if let Some(ref mut strfield) = c.fields.get_mut("string") {
                let FieldData::VecUInt8(ss) = strfield else {
                    unreachable!()
                };
                for s in ss.iter_mut() {
                    *s = String::from_utf8_lossy(s).as_bytes().to_vec();
                }
            }
            c
        })
    }

    /// Returns a strategy which produces `Cells` whose fields can
    /// be filtered via query condition.
    fn strat_cells_for_qc() -> impl Strategy<Value = (Cells, String, usize)> {
        strat_cells_with_qc_fields().prop_flat_map(|c| {
            let keys = c.fields().keys().cloned().collect::<Vec<String>>();
            let nrecords = c.len();
            (Just(c), proptest::sample::select(keys), 0..nrecords)
        })
    }

    fn strat_cells_for_qc_set_membership()
    -> impl Strategy<Value = (Cells, String, Vec<usize>)> {
        strat_cells_with_qc_fields().prop_flat_map(|c| {
            let keys = c.fields().keys().cloned().collect::<Vec<String>>();
            let nrecords = c.len();
            (
                Just(c),
                proptest::sample::select(keys),
                proptest::collection::vec(0..nrecords, 1..=nrecords),
            )
        })
    }

    fn do_query_condition_and(
        cells: Rc<Cells>,
        lhs: QueryConditionExpr,
        rhs: QueryConditionExpr,
    ) {
        let lhs_bitmap = cells.query_condition_bitmap(&lhs);
        let rhs_bitmap = cells.query_condition_bitmap(&rhs);
        let and_bitmap = cells.query_condition_bitmap(&(lhs & rhs));

        assert_eq!(lhs_bitmap.len(), rhs_bitmap.len());
        assert_eq!(lhs_bitmap.len(), and_bitmap.len());

        for i in 0..lhs_bitmap.len() {
            assert_eq!(
                lhs_bitmap.test(i) && rhs_bitmap.test(i),
                and_bitmap.test(i)
            );
        }
    }

    fn strat_query_condition_combination_op()
    -> impl Strategy<Value = (Rc<Cells>, QueryConditionExpr, QueryConditionExpr)>
    {
        strat_cells_with_qc_fields().prop_flat_map(|c| {
            let c = Rc::new(c);
            let params = QueryConditionParameters {
                domain: Some(Rc::new(CellsAsQueryConditionSchema::new(
                    Rc::clone(&c),
                ))),
                ..Default::default()
            };
            let strat_qc = any_with::<Predicate>(params.clone())
                .prop_map(QueryConditionExpr::Cond)
                .boxed();
            (Just(c), strat_qc.clone(), strat_qc.clone())
        })
    }

    proptest! {
        #[test]
        fn cells_extend((dst, src) in any::<SchemaData>().prop_flat_map(|s| {
            let params = CellsParameters {
                schema: Some(CellsStrategySchema::WriteSchema(Rc::new(s))),
                ..Default::default()
            };
            (any_with::<Cells>(params.clone()), any_with::<Cells>(params.clone()))
        })) {
            do_cells_extend(dst, src)
        }

        #[test]
        fn cells_sort((cells, keys) in any::<Cells>().prop_flat_map(|c| {
            let keys = c.fields().keys().cloned().collect::<Vec<String>>();
            let nkeys = keys.len();
            (Just(c), proptest::sample::subsequence(keys, 0..=nkeys).prop_shuffle())
        })) {
            do_cells_sort(cells, keys)
        }

        #[test]
        fn cells_slice_1d((cells, bound1, bound2) in any::<Cells>().prop_flat_map(|cells| {
            let slice_min = 0;
            let slice_max = cells.len();
            (Just(cells),
            slice_min..=slice_max,
            slice_min..=slice_max)
        })) {
            let start = std::cmp::min(bound1, bound2);
            let end = std::cmp::max(bound1, bound2);
            do_cells_slice_1d(cells, start.. end)
        }

        #[test]
        fn cells_slice_2d((cells, d1, d2, b11, b12, b21, b22) in any_with::<Cells>(CellsParameters {
            min_records: 1,
            ..Default::default()
        }).prop_flat_map(|cells| {
            let ncells = cells.len();
            (Just(cells),
            1..=((ncells as f64).sqrt() as usize),
            1..=((ncells as f64).sqrt() as usize))
                .prop_flat_map(|(cells, d1, d2)| {
                    (Just(cells),
                    Just(d1),
                    Just(d2),
                    0..=d1,
                    0..=d1,
                    0..=d2,
                    0..=d2)
                })
        })) {
            let s1 = std::cmp::min(b11, b12).. std::cmp::max(b11, b12);
            let s2 = std::cmp::min(b21, b22).. std::cmp::max(b21, b22);
            do_cells_slice_2d(cells, d1, d2, s1, s2)
        }

        #[test]
        fn cells_slice_3d((cells, d1, d2, d3, b11, b12, b21, b22, b31, b32) in any_with::<Cells>(CellsParameters {
            min_records: 1,
            ..Default::default()
        }).prop_flat_map(|cells| {
            let ncells = cells.len();
            (Just(cells),
            1..=((ncells as f64).cbrt() as usize),
            1..=((ncells as f64).cbrt() as usize),
            1..=((ncells as f64).cbrt() as usize))
                .prop_flat_map(|(cells, d1, d2, d3)| {
                    (Just(cells),
                    Just(d1),
                    Just(d2),
                    Just(d3),
                    0..=d1,
                    0..=d1,
                    0..=d2,
                    0..=d2,
                    0..=d3,
                    0..=d3)
                })
        })) {
            let s1 = std::cmp::min(b11, b12).. std::cmp::max(b11, b12);
            let s2 = std::cmp::min(b21, b22).. std::cmp::max(b21, b22);
            let s3 = std::cmp::min(b31, b32).. std::cmp::max(b31, b32);
            do_cells_slice_3d(cells, d1, d2, d3, s1, s2, s3)
        }

        #[test]
        fn cells_identify_groups((cells, keys) in any::<Cells>().prop_flat_map(|c| {
            let keys = c.fields().keys().cloned().collect::<Vec<String>>();
            let nkeys = keys.len();
            (Just(c), proptest::sample::subsequence(keys, 0..=nkeys))
        }))
        {
            do_cells_identify_groups(cells, &keys)
        }

        #[test]
        fn cells_count_distinct_1d(cells in any::<Cells>()) {
            do_cells_count_distinct_1d(cells)
        }

        #[test]
        fn cells_count_distinct_2d(cells in any::<Cells>()) {
            prop_assume!(cells.fields().len() >= 2);
            do_cells_count_distinct_2d(cells)
        }

        #[test]
        fn cells_dedup((cells, keys) in any::<Cells>().prop_flat_map(|c| {
            let keys = c.fields().keys().cloned().collect::<Vec<String>>();
            let nkeys = keys.len();
            (Just(c), proptest::sample::subsequence(keys, 0..=nkeys).prop_shuffle())
        }))
        {
            do_cells_dedup(cells, keys)
        }

        #[test]
        fn cells_projection((cells, keys) in any::<Cells>().prop_flat_map(|c| {
            let keys = c.fields().keys().cloned().collect::<Vec<String>>();
            let nkeys = keys.len();
            (Just(c), proptest::sample::subsequence(keys, 0..=nkeys).prop_shuffle())
        })) {
            do_cells_projection(cells, keys)
        }

        #[test]
        fn query_condition_lt((cells, field, pivot) in strat_cells_for_qc()) {
            do_query_condition_lt(cells, field, pivot)
        }

        #[test]
        fn query_condition_le((cells, field, pivot) in strat_cells_for_qc()) {
            do_query_condition_le(cells, field, pivot)
        }

        #[test]
        fn query_condition_eq((cells, field, pivot) in strat_cells_for_qc()) {
            do_query_condition_eq(cells, field, pivot)
        }

        #[test]
        fn query_condition_ge((cells, field, pivot) in strat_cells_for_qc()) {
            do_query_condition_ge(cells, field, pivot)
        }

        #[test]
        fn query_condition_gt((cells, field, pivot) in strat_cells_for_qc()) {
            do_query_condition_gt(cells, field, pivot)
        }

        #[test]
        fn query_condition_ne((cells, field, pivot) in strat_cells_for_qc()) {
            do_query_condition_ne(cells, field, pivot)
        }

        #[test]
        fn query_condition_set_membership_in((cells, field, member_idx) in strat_cells_for_qc_set_membership()) {
            do_query_condition_set_membership_in(cells, field, member_idx)
        }

        #[test]
        fn query_condition_set_membership_not_in((cells, field, member_idx) in strat_cells_for_qc_set_membership()) {
            do_query_condition_set_membership_not_in(cells, field, member_idx)
        }

        #[test]
        fn query_condition_and((cells, lhs, rhs) in strat_query_condition_combination_op()) {
            do_query_condition_and(cells, lhs, rhs)
        }
    }
}
