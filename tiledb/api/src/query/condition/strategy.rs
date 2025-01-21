use proptest::strategy::ValueTree;

use super::*;

#[derive(Debug)]
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

#[derive(Debug)]
enum QueryConditionValueTreeImpl {
    Cond(PredicateValueTree),
    Comb {
        lhs: Box<QueryConditionValueTree>,
        rhs: Box<QueryConditionValueTree>,
        op: CombinationOp,
        opstate: CombinationOpState,
    },
    Negate(Box<QueryConditionValueTree>),
}

#[derive(Debug)]
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
            QueryConditionExpr::Negate(predicate) => Self(Impl::Negate(
                Box::new(Self::new(QueryConditionExpr::clone(&predicate))),
            )),
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
            Impl::Negate(ref p) => {
                QueryConditionExpr::Negate(Box::new(p.current()))
            }
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
            Impl::Negate(ref mut p) => {
                // FIXME: consider removing negation
                p.simplify()
            }
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
                        // we already tried the initial input so simplify one of the sides
                        lhs.simplify() || rhs.simplify()
                    }
                    CombinationOpState::JustLeft => lhs.complicate(),
                    CombinationOpState::JustRight => rhs.complicate(),
                    CombinationOpState::Combine => {
                        lhs.complicate() || rhs.complicate()
                    }
                }
            }
            Impl::Negate(ref mut p) => p.complicate(),
        }
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
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
    fn shrink_just_left() {
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
    fn shrink_just_right() {
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
    fn shrink_combine() {
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

        // now it begins to shrink the left side
        assert!(vt.complicate());
        assert_eq!(lhs_lhs.clone() & rhs.clone(), vt.current());

        // now shrinking can apply to either side
        while vt.simplify() {}
        assert_eq!(lhs_lhs & rhs_lhs, vt.current());
    }
}
