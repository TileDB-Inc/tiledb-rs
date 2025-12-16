use std::rc::Rc;

use proptest::prelude::*;
use proptest::sample::SizeRange;
use proptest::strategy::ValueTree;
use strategy_ext::lexicographic::Between;

use super::*;
use crate::range::{Range, SingleValueRange, VarValueRange};
use crate::{Datatype, set_members_go, single_value_range_go};

pub trait QueryConditionSchema {
    /// Returns a list of fields which can have query conditions applied to them.
    fn fields(&self) -> Vec<&dyn QueryConditionField>;
}

pub trait QueryConditionField {
    /// Returns the name of this field.
    fn name(&self) -> &str;

    /// Returns the equality ops which can be used to filter this field.
    ///
    /// A return of `None` means all ops are allowed.
    fn equality_ops(&self) -> Option<Vec<EqualityOp>>;

    /// Returns the domain of this field.
    fn domain(&self) -> Option<Range>;

    /// Returns the set of set members of this field if it is a sparse range.
    fn set_members(&self) -> Option<SetMembers>;
}

#[derive(Clone)]
pub struct Parameters {
    pub domain: Option<Rc<dyn QueryConditionSchema>>,
    pub num_set_members: SizeRange,
    pub recursion: RecursionParameters,
}

type EqualityOpDomain = Vec<(String, Range, Option<Vec<EqualityOp>>)>;
type SetMembershipOpDomain = Vec<(String, Option<Range>, Option<SetMembers>)>;

impl Parameters {
    fn nullness_op_domain(&self) -> Option<Vec<String>> {
        self.domain
            .as_ref()
            .map(|d| {
                d.fields()
                    .into_iter()
                    .map(|f| f.name().to_owned())
                    .collect::<Vec<_>>()
            })
            .and_then(|v| (!v.is_empty()).then_some(v))
    }

    fn equality_op_domain(&self) -> Option<EqualityOpDomain> {
        self.domain
            .as_ref()
            .map(|d| {
                d.fields()
                    .into_iter()
                    .filter_map(|f| {
                        f.domain().map(|domain| {
                            (f.name().to_owned(), domain, f.equality_ops())
                        })
                    })
                    .filter(|(_, _, ops)| {
                        ops.as_ref().map(|ops| !ops.is_empty()).unwrap_or(true)
                    })
                    .collect::<Vec<_>>()
            })
            .and_then(|v| (!v.is_empty()).then_some(v))
    }

    fn set_membership_op_domain(&self) -> Option<SetMembershipOpDomain> {
        self.domain
            .as_ref()
            .map(|d| {
                d.fields()
                    .into_iter()
                    .map(|f| (f.name().to_owned(), f.domain(), f.set_members()))
                    .filter(|(_, domain, members)| {
                        domain.is_some() || members.is_some()
                    })
                    .collect::<Vec<_>>()
            })
            .and_then(|v| (!v.is_empty()).then_some(v))
    }
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            domain: None,
            num_set_members: (1..=(SizeRange::default().end_incl())).into(),
            recursion: Default::default(),
        }
    }
}

#[derive(Clone)]
pub struct RecursionParameters {
    pub max_depth: u32,
    pub desired_size: u32,
}

impl Default for RecursionParameters {
    fn default() -> Self {
        Self {
            max_depth: 3,
            desired_size: 4,
        }
    }
}

impl Arbitrary for EqualityOp {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            Just(EqualityOp::Less),
            Just(EqualityOp::LessEqual),
            Just(EqualityOp::Equal),
            Just(EqualityOp::Greater),
            Just(EqualityOp::GreaterEqual),
            Just(EqualityOp::NotEqual),
        ]
        .boxed()
    }
}

impl Arbitrary for SetMembershipOp {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![Just(SetMembershipOp::In), Just(SetMembershipOp::NotIn)]
            .boxed()
    }
}

impl Arbitrary for NullnessOp {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![Just(NullnessOp::IsNull), Just(NullnessOp::NotNull)].boxed()
    }
}

impl Arbitrary for CombinationOp {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![Just(CombinationOp::And), Just(CombinationOp::Or)].boxed()
    }
}

impl Arbitrary for Field {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = Parameters;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        let Some(fnames) = params.domain.map(|d| {
            d.fields()
                .into_iter()
                .map(|f| f.name().to_owned())
                .collect::<Vec<_>>()
        }) else {
            unimplemented!()
        };
        if fnames.is_empty() {
            unimplemented!()
        }
        proptest::sample::select(fnames)
            .prop_map(QueryConditionExpr::field)
            .boxed()
    }
}

impl Arbitrary for Literal {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = Option<Range>;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        let Some(range) = params else {
            return prop_oneof![
                8 => any::<SingleValueRange>().prop_map(Range::Single),
                2 => any_with::<VarValueRange>(Some(Datatype::StringUtf8)).prop_map(Range::Var)
            ]
            .prop_flat_map(|range| Self::arbitrary_with(Some(range)))
            .boxed();
        };

        match range {
            Range::Single(svr) => single_value_range_go!(
                svr,
                _DT,
                lb,
                ub,
                if lb.bits_eq(&ub) {
                    Just(Literal::from(lb)).boxed()
                } else {
                    (lb..=ub).prop_map(Literal::from).boxed()
                }
            ),
            Range::Multi(_) => unimplemented!(),
            Range::Var(VarValueRange::UInt8(lb, ub)) => Between::new(&lb, &ub)
                .prop_map(|bytes| {
                    Literal::from(String::from_utf8_lossy(&bytes).into_owned())
                })
                .boxed(),
            Range::Var(_) => unimplemented!(),
        }
    }
}

impl Arbitrary for SetMembers {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = (Option<Range>, SizeRange);

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        let Some(range) = params.0 else {
            return prop_oneof![
                8 => any::<SingleValueRange>().prop_map(Range::Single),
                2 => any_with::<VarValueRange>(Some(Datatype::StringUtf8)).prop_map(Range::Var)
            ]
            .prop_flat_map(move |range| Self::arbitrary_with((Some(range), params.1.clone())))
            .boxed();
        };

        match range {
            Range::Single(svr) => single_value_range_go!(
                svr,
                _DT,
                lb,
                ub,
                strategy_ext::records::vec_records_strategy(
                    if lb.bits_eq(&ub) {
                        Just(lb).boxed()
                    } else {
                        (lb..=ub).boxed()
                    },
                    params.1
                )
                .prop_map(SetMembers::from_iter)
                .boxed()
            ),
            Range::Multi(_) => unimplemented!(),
            Range::Var(VarValueRange::UInt8(lb, ub)) => {
                strategy_ext::records::vec_records_strategy(
                    Between::new(&lb, &ub).prop_map(|bytes| {
                        String::from_utf8_lossy(&bytes).into_owned()
                    }),
                    params.1,
                )
                .prop_map(SetMembers::from_iter)
                .boxed()
            }
            Range::Var(_) => unimplemented!(),
        }
    }
}

impl Arbitrary for EqualityPredicate {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = Parameters;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        let Some(domain) = params.equality_op_domain() else {
            unimplemented!()
        };
        if domain.is_empty() {
            unimplemented!()
        }
        proptest::sample::select(domain)
            .prop_flat_map(|(fname, domain, ops)| {
                (
                    Just(fname),
                    if let Some(ops) = ops {
                        proptest::sample::select(ops).boxed()
                    } else {
                        any::<EqualityOp>().boxed()
                    },
                    any_with::<Literal>(Some(domain)),
                )
            })
            .prop_map(|(field, op, value)| EqualityPredicate {
                field,
                op,
                value,
            })
            .boxed()
    }
}

impl Arbitrary for SetMembershipPredicate {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = Parameters;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        let Some(domain) = params.set_membership_op_domain() else {
            unimplemented!()
        };
        if domain.is_empty() {
            unimplemented!()
        }
        (proptest::sample::select(domain), any::<SetMembershipOp>())
            .prop_flat_map(move |((fname, domain, members), op)| {
                let strat_members = if let Some(members) = members {
                    let num_set_members = params.num_set_members.clone();
                    set_members_go!(members, ref m, {
                        Just(m.to_vec())
                            .prop_shuffle()
                            .prop_flat_map(move |m| {
                                let subseq_size_range = std::cmp::min(
                                    num_set_members.start(),
                                    m.len(),
                                )
                                    ..=std::cmp::min(
                                        num_set_members.end_incl(),
                                        m.len(),
                                    );
                                proptest::sample::subsequence(
                                    m,
                                    subseq_size_range,
                                )
                            })
                            .prop_map(SetMembers::from_iter)
                            .boxed()
                    })
                } else if let Some(domain) = domain {
                    any_with::<SetMembers>((
                        Some(domain),
                        params.num_set_members.clone(),
                    ))
                } else {
                    unimplemented!()
                };
                (Just(fname), Just(op), strat_members)
            })
            .prop_map(|(field, op, members)| SetMembershipPredicate {
                field,
                op,
                members,
            })
            .boxed()
    }
}

impl Arbitrary for NullnessPredicate {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = Parameters;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        let Some(fnames) = params.nullness_op_domain() else {
            unimplemented!()
        };
        // all fields should support null test
        if fnames.is_empty() {
            unimplemented!()
        }
        (proptest::sample::select(fnames), any::<NullnessOp>())
            .prop_map(|(field, op)| NullnessPredicate { field, op })
            .boxed()
    }
}

impl Arbitrary for Predicate {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = Parameters;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        let mut preds = Vec::new();
        if params.nullness_op_domain().is_some() {
            preds.push(
                any_with::<NullnessPredicate>(params.clone())
                    .prop_map(Predicate::Nullness)
                    .boxed(),
            )
        }
        if params.equality_op_domain().is_some() {
            preds.push(
                any_with::<EqualityPredicate>(params.clone())
                    .prop_map(Predicate::Equality)
                    .boxed(),
            )
        }
        if params.set_membership_op_domain().is_some() {
            preds.push(
                any_with::<SetMembershipPredicate>(params.clone())
                    .prop_map(Predicate::SetMembership)
                    .boxed(),
            )
        }
        proptest::strategy::Union::new(preds).boxed()
    }
}

impl Arbitrary for QueryConditionExpr {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = Parameters;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        let rec = params.recursion.clone();

        any_with::<Predicate>(params)
            .boxed()
            .prop_map(QueryConditionExpr::Cond)
            .prop_recursive(rec.max_depth, rec.desired_size, 2, |leaf| {
                prop_oneof![
                    (leaf.clone(), any::<CombinationOp>(), leaf.clone())
                        .prop_map(|(lhs, op, rhs)| QueryConditionExpr::Comb {
                            lhs: Box::new(lhs),
                            op,
                            rhs: Box::new(rhs)
                        }),
                    leaf.prop_map(|pred| QueryConditionExpr::Negate(Box::new(
                        pred
                    )))
                ]
            })
            .boxed()
    }
}

#[derive(Clone, Debug)]
enum CombinationOpState {
    /// No values have been produced yet. Try the full combination op.
    New,
    /// The initial combination op failed. Try just the left side.
    TryLeft,
    /// Just the left side passes. Try just the right side.
    TryRight,
    /// Failed at TryLeft, we will never need the right side.
    JustLeft,
    /// Failed at TryRight, we will never need the left side.
    JustRight,
    /// TryLeft and TryRight both passed, we will need both sides
    Combine,
}

#[derive(Clone, Debug)]
enum NegationState {
    /// No values have been produced yet. Try the full negation op.
    New,
    /// The initial negation op failed. Try the equivalent predicate with
    /// `QueryConditionExpr::negate`.
    TryFnNegation,
    /// The outer `QueryConditionExpr::Negate` is required.
    RequiresNegateWrapping,
    /// The equivalent predicate fails as well, the negation is unnecessary.
    JustFnNegation,
}

#[derive(Clone, Debug)]
enum QueryConditionValueTreeImpl {
    Cond(PredicateValueTree),
    Comb {
        lhs: Box<QueryConditionValueTree>,
        rhs: Box<QueryConditionValueTree>,
        op: CombinationOp,
        opstate: CombinationOpState,
    },
    Negate {
        arg: Box<QueryConditionValueTree>,
        opstate: NegationState,
    },
}

#[derive(Clone, Debug)]
pub struct QueryConditionValueTree(QueryConditionValueTreeImpl);

impl QueryConditionValueTree {
    pub fn new(qc: QueryConditionExpr) -> Self {
        use QueryConditionValueTreeImpl as Impl;
        match qc {
            QueryConditionExpr::Cond(predicate) => {
                Self(Impl::Cond(PredicateValueTree::new(predicate)))
            }
            QueryConditionExpr::Comb { lhs, rhs, op } => Self(Impl::Comb {
                lhs: Box::new(Self::new(QueryConditionExpr::clone(&lhs))),
                rhs: Box::new(Self::new(QueryConditionExpr::clone(&rhs))),
                op,
                opstate: CombinationOpState::New,
            }),
            QueryConditionExpr::Negate(predicate) => Self(Impl::Negate {
                arg: Box::new(Self::new(QueryConditionExpr::clone(&predicate))),
                opstate: NegationState::New,
            }),
        }
    }
}

impl ValueTree for QueryConditionValueTree {
    type Value = QueryConditionExpr;

    fn current(&self) -> Self::Value {
        use QueryConditionValueTreeImpl as Impl;
        match self.0 {
            Impl::Cond(ref p) => QueryConditionExpr::Cond(p.current()),
            Impl::Comb {
                ref lhs,
                ref rhs,
                ref op,
                ref opstate,
            } => match opstate {
                CombinationOpState::New | CombinationOpState::Combine => {
                    QueryConditionExpr::Comb {
                        lhs: Box::new(lhs.current()),
                        rhs: Box::new(rhs.current()),
                        op: *op,
                    }
                }
                CombinationOpState::TryLeft | CombinationOpState::JustLeft => {
                    lhs.current()
                }
                CombinationOpState::TryRight
                | CombinationOpState::JustRight => rhs.current(),
            },
            Impl::Negate {
                ref arg,
                ref opstate,
            } => match opstate {
                NegationState::New | NegationState::RequiresNegateWrapping => {
                    QueryConditionExpr::Negate(Box::new(arg.current()))
                }
                NegationState::TryFnNegation
                | NegationState::JustFnNegation => arg.current().negate(),
            },
        }
    }

    fn simplify(&mut self) -> bool {
        use QueryConditionValueTreeImpl as Impl;
        match self.0 {
            Impl::Cond(ref mut p) => p.simplify(),
            Impl::Comb {
                ref mut lhs,
                ref mut rhs,
                ref mut opstate,
                ..
            } => match opstate {
                CombinationOpState::New => {
                    *opstate = CombinationOpState::TryLeft;
                    true
                }
                CombinationOpState::TryLeft => {
                    *opstate = CombinationOpState::JustLeft;
                    lhs.simplify()
                }
                CombinationOpState::TryRight => {
                    *opstate = CombinationOpState::JustRight;
                    rhs.simplify()
                }
                CombinationOpState::JustLeft => lhs.simplify(),
                CombinationOpState::JustRight => rhs.simplify(),
                CombinationOpState::Combine => lhs.simplify() || rhs.simplify(),
            },
            Impl::Negate {
                ref mut arg,
                ref mut opstate,
            } => match opstate {
                NegationState::New => {
                    *opstate = NegationState::TryFnNegation;
                    true
                }
                NegationState::TryFnNegation => {
                    *opstate = NegationState::JustFnNegation;
                    arg.simplify()
                }
                NegationState::RequiresNegateWrapping => arg.simplify(),
                NegationState::JustFnNegation => arg.simplify(),
            },
        }
    }

    fn complicate(&mut self) -> bool {
        use QueryConditionValueTreeImpl as Impl;
        match self.0 {
            Impl::Cond(ref mut p) => p.complicate(),
            Impl::Comb {
                ref mut lhs,
                ref mut rhs,
                ref mut opstate,
                ..
            } => {
                match opstate {
                    CombinationOpState::New => false,
                    CombinationOpState::TryLeft => {
                        // passed with right unused
                        *opstate = CombinationOpState::TryRight;
                        true
                    }
                    CombinationOpState::TryRight => {
                        // passed with left unused, and passed with right unused
                        *opstate = CombinationOpState::Combine;

                        // "repeated calls to complicate() will return false only once the 'current'
                        // value has returned to what it was before the last call to simplify()"
                        // just return `true`, which will try the initial input again, but that's
                        // better than breaking the invariant
                        true
                    }
                    CombinationOpState::JustLeft => lhs.complicate(),
                    CombinationOpState::JustRight => rhs.complicate(),
                    CombinationOpState::Combine => {
                        lhs.complicate() || rhs.complicate()
                    }
                }
            }
            Impl::Negate {
                ref mut arg,
                ref mut opstate,
            } => match opstate {
                NegationState::New => false,
                NegationState::TryFnNegation => {
                    *opstate = NegationState::RequiresNegateWrapping;
                    // "repeated calls to complicate() will return false only once the 'current'
                    // value has returned to what it was before the last call to simplify()"
                    // just return `true`, which will try the initial input again, but that's
                    // better than breaking the invariant
                    true
                }
                NegationState::RequiresNegateWrapping => arg.complicate(),
                NegationState::JustFnNegation => arg.complicate(),
            },
        }
    }
}

#[derive(Clone, Debug)]
enum PredicateValueTree {
    Equality(EqualityPredicateValueTree),
    SetMembership(SetMembershipValueTree),
    Nullness(NullnessPredicate),
}

impl PredicateValueTree {
    pub fn new(predicate: Predicate) -> Self {
        match predicate {
            Predicate::Equality(eq) => {
                Self::Equality(EqualityPredicateValueTree::new(eq))
            }
            Predicate::SetMembership(s) => {
                Self::SetMembership(SetMembershipValueTree::new(s))
            }
            Predicate::Nullness(n) => Self::Nullness(n),
        }
    }
}

impl ValueTree for PredicateValueTree {
    type Value = Predicate;

    fn current(&self) -> Self::Value {
        match self {
            Self::Equality(e) => Predicate::Equality(e.current()),
            Self::SetMembership(m) => Predicate::SetMembership(m.current()),
            Self::Nullness(m) => Predicate::Nullness(m.clone()),
        }
    }

    fn simplify(&mut self) -> bool {
        match self {
            Self::Equality(e) => e.simplify(),
            Self::SetMembership(m) => m.simplify(),
            Self::Nullness(_) => false,
        }
    }

    fn complicate(&mut self) -> bool {
        match self {
            Self::Equality(e) => e.complicate(),
            Self::SetMembership(m) => m.complicate(),
            Self::Nullness(_) => false,
        }
    }
}

#[derive(Clone, Debug)]
struct EqualityPredicateValueTree {
    // FIXME: something which can shrink
    // definitely want to shrink the number, and maybe the op
    value: EqualityPredicate,
}

impl EqualityPredicateValueTree {
    pub fn new(predicate: EqualityPredicate) -> Self {
        Self { value: predicate }
    }
}

impl ValueTree for EqualityPredicateValueTree {
    type Value = EqualityPredicate;

    fn current(&self) -> Self::Value {
        self.value.clone()
    }

    fn simplify(&mut self) -> bool {
        false
    }

    fn complicate(&mut self) -> bool {
        false
    }
}

#[derive(Clone, Debug)]
struct SetMembershipValueTree {
    // FIXME: something which can shrink
    // definitely we want to reduce the members of the set
    value: SetMembershipPredicate,
}

impl SetMembershipValueTree {
    pub fn new(predicate: SetMembershipPredicate) -> Self {
        Self { value: predicate }
    }
}

impl ValueTree for SetMembershipValueTree {
    type Value = SetMembershipPredicate;

    fn current(&self) -> Self::Value {
        self.value.clone()
    }

    fn simplify(&mut self) -> bool {
        false
    }

    fn complicate(&mut self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test shrinking to just the left side
    #[test]
    fn shrink_combine_and_just_left() {
        let lhs = QueryConditionExpr::field("foo").eq(7);
        let rhs = QueryConditionExpr::field("bar").ne(22);

        let qc = lhs.clone() & rhs.clone();

        let mut vt = QueryConditionValueTree::new(qc.clone());
        assert_eq!(qc, vt.current());

        assert!(vt.simplify());
        assert_eq!(lhs, vt.current());

        // it should not attempt to shrink the right side
        assert!(!vt.simplify());
        assert_eq!(lhs, vt.current());
    }

    /// Test shrinking to just the right side
    #[test]
    fn shrink_combine_and_just_right() {
        let lhs = QueryConditionExpr::field("foo").eq(7);
        let rhs = QueryConditionExpr::field("bar").ne(22);

        let qc = lhs.clone() & rhs.clone();

        let mut vt = QueryConditionValueTree::new(qc.clone());
        assert_eq!(qc, vt.current());

        assert!(vt.simplify());
        assert_eq!(lhs, vt.current());

        // it should not attempt to shrink the right side
        assert!(vt.complicate());
        assert_eq!(rhs, vt.current());

        assert!(!vt.simplify());
        assert_eq!(rhs, vt.current());
    }

    /// Test shrinking to need both sides after all
    #[test]
    fn shrink_combine_and() {
        let lhs_lhs = QueryConditionExpr::field("foo").eq(7);
        let lhs_rhs = QueryConditionExpr::field("bar").ne(22);
        let rhs_lhs = QueryConditionExpr::field("gub").le(45);
        let rhs_rhs = QueryConditionExpr::field("gux").ge(72);

        let lhs = lhs_lhs.clone() & lhs_rhs.clone();
        let rhs = rhs_lhs.clone() & rhs_rhs.clone();

        let qc = lhs.clone() & rhs.clone();

        let mut vt = QueryConditionValueTree::new(qc.clone());
        assert_eq!(qc, vt.current());

        assert!(vt.simplify());
        assert_eq!(lhs, vt.current());

        // it should not attempt to shrink the right side
        assert!(vt.complicate());
        assert_eq!(rhs, vt.current());

        // to preserve the invariant we have to shrink back to the initial input
        assert!(vt.complicate());
        assert_eq!(qc, vt.current());

        let mut vt_complicate = vt.clone();
        assert!(!vt_complicate.complicate());
        assert_eq!(qc, vt_complicate.current());

        // now it begins to shrink the left side
        assert!(vt.simplify());
        assert_eq!(lhs_lhs.clone() & rhs.clone(), vt.current());

        // now shrinking can apply to either side
        while vt.simplify() {}
        assert_eq!(lhs_lhs & rhs_lhs, vt.current());
    }

    #[test]
    fn shrink_negation_fn_negation() {
        let arg = QueryConditionExpr::field("foobar").lt(100);
        let qc = !arg.clone();

        let mut vt = QueryConditionValueTree::new(qc.clone());
        assert_eq!(qc, vt.current());

        assert!(vt.simplify());
        assert_eq!(arg.negate(), vt.current());

        assert!(!vt.simplify());
        assert_eq!(arg.negate(), vt.current());
    }

    #[test]
    fn shrink_negation_not_is_required() {
        let arg = QueryConditionExpr::field("foobar").lt(100);
        let qc = !arg.clone();

        let mut vt = QueryConditionValueTree::new(qc.clone());
        assert_eq!(qc, vt.current());

        assert!(vt.simplify());
        assert_eq!(arg.negate(), vt.current());

        assert!(vt.complicate());
        assert_eq!(qc, vt.current());

        let mut vt_2 = vt.clone();

        assert!(!vt.simplify());
        assert_eq!(qc, vt.current());

        assert!(!vt_2.simplify());
        assert_eq!(qc, vt_2.current());
    }
}
