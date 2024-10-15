use itertools::Itertools;
use tiledb_common::range::Range;

/// Encapsulates data for a subarray.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SubarrayData {
    /// List of requested ranges on each dimension.
    /// The outer `Vec` is the list of dimensions and the inner `Vec`
    /// is the list of requested ranges for that dimension.
    /// If a list is empty for a dimension, then all the coordinates
    /// of that dimension are selected.
    pub dimension_ranges: Vec<Vec<Range>>,
}

impl SubarrayData {
    /// Returns a new `SubarrayData` which represents the intersection
    /// of all the ranges of `self` with a new set of `ranges` on each dimension.
    ///
    /// If any dimension does not have any intersection with `ranges`, then
    /// this returns `None` as the resulting subarray would select no coordinates.
    pub fn intersect_ranges(&self, ranges: &[Range]) -> Option<Self> {
        let updated_ranges = self
            .dimension_ranges
            .iter()
            .zip(ranges.iter())
            .map(|(current_ranges, new_range)| {
                if current_ranges.is_empty() {
                    // empty means select the whole thing
                    vec![new_range.clone()]
                } else {
                    current_ranges
                        .iter()
                        .filter_map(|current_range| {
                            current_range.intersection(new_range)
                        })
                        .collect::<Vec<Range>>()
                }
            })
            .collect::<Vec<Vec<Range>>>();

        if updated_ranges.iter().any(|dim| dim.is_empty()) {
            None
        } else {
            Some(SubarrayData {
                dimension_ranges: updated_ranges,
            })
        }
    }
    /// Returns a new `SubarrayData` which represents the intersection
    /// of all the ranges of `self` with all of the ranges of `other` on each dimension.
    ///
    /// ```
    /// use tiledb_common::range::Range;
    /// use tiledb_serde::query::subarray::SubarrayData;
    ///
    /// let s1 = SubarrayData {
    ///     dimension_ranges: vec![
    ///         vec![Range::from(&[0, 100]), Range::from(&[200, 300])],
    ///         vec![Range::from(&[2, 6]), Range::from(&[8, 12])],
    ///         vec![Range::from(&[20, 30]), Range::from(&[40, 50])]
    ///     ]
    /// };
    /// let s2 = SubarrayData {
    ///     dimension_ranges: vec![
    ///         vec![Range::from(&[150, 250])],
    ///         vec![Range::from(&[4, 10]), Range::from(&[12, 12])],
    ///         vec![Range::from(&[25, 45])]
    ///     ]
    /// };
    /// let intersection = s1.intersect(&s2);
    ///
    /// assert_eq!(intersection, Some(SubarrayData {
    ///     dimension_ranges: vec![
    ///         vec![Range::from(&[200, 250])],
    ///         vec![Range::from(&[4, 6]), Range::from(&[8, 10]), Range::from(&[12, 12])],
    ///         vec![Range::from(&[25, 30]), Range::from(&[40, 45])]
    ///     ]
    /// }));
    /// ```
    ///
    /// If any dimension does not have any intersection, then this returns `None`
    /// as the resulting subarray would select no coordinates.
    /// ```
    /// use tiledb_common::range::Range;
    /// use tiledb_serde::query::subarray::SubarrayData;
    ///
    /// let s1 = SubarrayData {
    ///     dimension_ranges: vec![
    ///         vec![Range::from(&[50, 100]), Range::from(&[400, 450])]
    ///     ]
    /// };
    /// let s2 = SubarrayData {
    ///     dimension_ranges: vec![
    ///         vec![Range::from(&[150, 250]), Range::from(&[300, 350])],
    ///     ]
    /// };
    /// let intersection = s1.intersect(&s2);
    /// assert_eq!(intersection, None);
    /// ```
    ///
    /// If a dimension in `self` (without loss of generality) has no ranges,
    /// then it is a special case which means to select the all coordinates.
    /// The intersection is equal to the ranges of `other`.
    /// ```
    /// use tiledb_common::range::Range;
    /// use tiledb_serde::query::subarray::SubarrayData;
    ///
    /// let s1 = SubarrayData {
    ///     dimension_ranges: vec![
    ///         vec![]
    ///     ]
    /// };
    /// let s2 = SubarrayData {
    ///     dimension_ranges: vec![
    ///         vec![Range::from(&[150, 250]), Range::from(&[300, 350])],
    ///     ]
    /// };
    /// let intersection = s1.intersect(&s2);
    /// assert_eq!(intersection, Some(s2.clone()));
    /// ```
    pub fn intersect(&self, other: &SubarrayData) -> Option<Self> {
        let updated_ranges = self
            .dimension_ranges
            .iter()
            .zip(other.dimension_ranges.iter())
            .map(|(my_dimension, their_dimension)| {
                if my_dimension.is_empty() {
                    // empty means select all coordinates
                    their_dimension.clone()
                } else if their_dimension.is_empty() {
                    // empty means select all coordinates
                    my_dimension.clone()
                } else {
                    my_dimension
                        .iter()
                        .cartesian_product(their_dimension.iter())
                        .filter_map(|(rm, rt)| rm.intersection(rt))
                        .collect::<Vec<Range>>()
                }
            })
            .collect::<Vec<Vec<Range>>>();

        if updated_ranges.iter().any(|dim| dim.is_empty()) {
            None
        } else {
            Some(SubarrayData {
                dimension_ranges: updated_ranges,
            })
        }
    }
}

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy {
    use std::rc::Rc;

    use proptest::prelude::*;

    use super::*;
    use crate::array::schema::SchemaData;

    impl Arbitrary for SubarrayData {
        type Parameters = Option<Rc<SchemaData>>;
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
            let strat_dimension_ranges = if let Some(schema) = params {
                schema
                    .domain
                    .dimension
                    .iter()
                    .map(|d| d.subarray_strategy(None).unwrap())
                    .collect::<Vec<BoxedStrategy<Range>>>()
            } else {
                todo!()
            };

            const DIMENSION_MIN_RANGES: usize = 0;
            const DIMENSION_MAX_RANGES: usize = 4;

            strat_dimension_ranges
                .into_iter()
                .map(|strat_range| {
                    proptest::collection::vec(
                        strat_range,
                        DIMENSION_MIN_RANGES..=DIMENSION_MAX_RANGES,
                    )
                    .boxed()
                })
                .collect::<Vec<BoxedStrategy<Vec<Range>>>>()
                .prop_map(|dimension_ranges| SubarrayData { dimension_ranges })
                .boxed()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use itertools::izip;
    use proptest::prelude::*;

    use super::*;
    use crate::array::domain::strategy::Requirements as DomainRequirements;
    use crate::array::schema::strategy::Requirements as SchemaRequirements;
    use crate::array::schema::SchemaData;

    fn do_subarray_intersect_ranges(subarray: &SubarrayData, ranges: &[Range]) {
        if let Some(intersection) = subarray.intersect_ranges(ranges) {
            assert_eq!(
                subarray.dimension_ranges.len(),
                intersection.dimension_ranges.len()
            );
            assert_eq!(subarray.dimension_ranges.len(), ranges.len());

            for (before, after, update) in izip!(
                subarray.dimension_ranges.iter(),
                intersection.dimension_ranges.iter(),
                ranges.iter()
            ) {
                if before.is_empty() {
                    assert_eq!(vec![update.clone()], *after);
                    continue;
                }

                assert!(after.len() <= before.len());

                let mut r_after = after.iter();
                for r_before in before.iter() {
                    if let Some(r) = r_before.intersection(update) {
                        assert_eq!(*r_after.next().unwrap(), r);
                    }
                }
                assert_eq!(None, r_after.next());
            }
        } else {
            // for at least one dimension, none of the ranges could have intersected
            let found_empty_intersection = subarray
                .dimension_ranges
                .iter()
                .zip(ranges.iter())
                .any(|(current, new)| {
                    if current.is_empty() {
                        false
                    } else {
                        current.iter().all(|r| r.intersection(new).is_none())
                    }
                });
            assert!(
                found_empty_intersection,
                "dimensions: {:?}",
                subarray
                    .dimension_ranges
                    .iter()
                    .zip(ranges.iter())
                    .map(|(d, r)| format!(
                        "({:?} && {:?} = {:?}",
                        d,
                        r,
                        d.iter()
                            .map(|dr| dr.intersection(r))
                            .collect::<Vec<Option<Range>>>()
                    ))
                    .collect::<Vec<_>>()
            );
        }
    }

    /// Validate the intersection of two subarrays.
    /// `s1` and `s2` are two subarrays for the same schema.
    fn do_subarray_intersect_subarray(s1: &SubarrayData, s2: &SubarrayData) {
        if let Some(intersection) = s1.intersect(s2) {
            for (di, ds1, ds2) in izip!(
                intersection.dimension_ranges.iter(),
                s1.dimension_ranges.iter(),
                s2.dimension_ranges.iter(),
            ) {
                if ds1.is_empty() {
                    assert_eq!(di, ds2);
                    continue;
                } else if ds2.is_empty() {
                    assert_eq!(di, ds1);
                    continue;
                }
                // there must be some pair from (rs1, rs2) where di is the intersection
                for ri in di.iter() {
                    let found_input = ds1
                        .iter()
                        .cartesian_product(ds2.iter())
                        .any(|(rs1, rs2)| {
                            Some(ri) == rs1.intersection(rs2).as_ref()
                        });
                    assert!(found_input, "ri = {:?}", ri);
                }

                // and for all pairs (rs1, rs2), there must be some ri which covers
                for (rs1, rs2) in ds1.iter().cartesian_product(ds2.iter()) {
                    let Some(intersection) = rs1.intersection(rs2) else {
                        continue;
                    };

                    let found_output = di.iter().any(|ri| intersection == *ri);
                    assert!(
                        found_output,
                        "rs1 = {:?}, rs2 = {:?}, intersection = {:?}",
                        rs1, rs2, intersection
                    );
                }
            }
        } else {
            // for each least one dimension, none of the ranges of `s1`
            // intersected with any range from `s2`
            let found_empty_intersection = s1
                .dimension_ranges
                .iter()
                .zip(s2.dimension_ranges.iter())
                .any(|(ds1, ds2)| {
                    ds1.iter()
                        .cartesian_product(ds2.iter())
                        .all(|(rs1, rs2)| rs1.intersection(rs2).is_none())
                });
            assert!(found_empty_intersection);
        }
    }

    fn strat_subarray_intersect_ranges(
    ) -> impl Strategy<Value = (SubarrayData, Vec<Range>)> {
        let req = Rc::new(SchemaRequirements {
            domain: Some(Rc::new(DomainRequirements {
                num_dimensions: 1..=1,
                ..Default::default()
            })),
            ..Default::default()
        });

        any_with::<SchemaData>(req).prop_flat_map(|schema| {
            let schema = Rc::new(schema);
            (
                any_with::<SubarrayData>(Some(Rc::clone(&schema))),
                schema.domain.subarray_strategy(),
            )
        })
    }

    fn strat_subarray_intersect_subarray(
    ) -> impl Strategy<Value = (SubarrayData, SubarrayData)> {
        use crate::array::domain::strategy::Requirements as DomainRequirements;
        use crate::array::schema::strategy::Requirements as SchemaRequirements;

        let req = Rc::new(SchemaRequirements {
            domain: Some(Rc::new(DomainRequirements {
                num_dimensions: 1..=1,
                ..Default::default()
            })),
            ..Default::default()
        });

        any_with::<SchemaData>(req).prop_flat_map(|schema| {
            let schema = Rc::new(schema);
            let strat_subarray =
                any_with::<SubarrayData>(Some(Rc::clone(&schema)));
            (strat_subarray.clone(), strat_subarray.clone())
        })
    }

    proptest! {
        #[test]
        fn subarray_intersect_ranges((subarray, range) in strat_subarray_intersect_ranges()) {
            do_subarray_intersect_ranges(&subarray, &range)
        }

        #[test]
        fn subarray_intersect_subarray((s1, s2) in strat_subarray_intersect_subarray()) {
            do_subarray_intersect_subarray(&s1, &s2)
        }
    }
}
