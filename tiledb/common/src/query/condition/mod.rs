#[cfg(feature = "proptest-strategies")]
pub mod strategy;

use std::fmt::{Display, Formatter, Result as FmtResult};
use std::hash::{Hash, Hasher};
use std::ops::{BitAnd, BitOr, Not};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::array::CellValNum;
use crate::datatype::Datatype;
use crate::datatype::physical::{BitsEq, BitsHash, BitsKeyAdapter};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum EqualityOp {
    Less,
    LessEqual,
    Equal,
    NotEqual,
    GreaterEqual,
    Greater,
}

impl Display for EqualityOp {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::Less => write!(f, "<"),
            Self::LessEqual => write!(f, "<="),
            Self::Equal => write!(f, "="),
            Self::NotEqual => write!(f, "<>"),
            Self::GreaterEqual => write!(f, ">="),
            Self::Greater => write!(f, ">"),
        }
    }
}

impl Not for EqualityOp {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            Self::Less => Self::GreaterEqual,
            Self::LessEqual => Self::Greater,
            Self::Equal => Self::NotEqual,
            Self::NotEqual => Self::Equal,
            Self::GreaterEqual => Self::Less,
            Self::Greater => Self::LessEqual,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum SetMembershipOp {
    In,
    NotIn,
}

impl Display for SetMembershipOp {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::In => write!(f, "IN"),
            Self::NotIn => write!(f, "NOT IN"),
        }
    }
}

impl Not for SetMembershipOp {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            Self::In => Self::NotIn,
            Self::NotIn => Self::In,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum NullnessOp {
    IsNull,
    NotNull,
}

impl Display for NullnessOp {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::IsNull => write!(f, "IS NULL"),
            Self::NotNull => write!(f, "IS NOT NULL"),
        }
    }
}

impl Not for NullnessOp {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            Self::IsNull => Self::NotNull,
            Self::NotNull => Self::IsNull,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum CombinationOp {
    And,
    Or,
}

impl Display for CombinationOp {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::And => write!(f, "AND"),
            Self::Or => write!(f, "OR"),
        }
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum Literal {
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
}

#[macro_export]
macro_rules! literal_go {
    ($expr:expr, $DT:ident, $value:pat, $integral:expr, $float:expr, $str:expr) => {{
        match $expr {
            Literal::UInt8($value) => {
                type $DT = u8;
                $integral
            }
            Literal::UInt16($value) => {
                type $DT = u16;
                $integral
            }
            Literal::UInt32($value) => {
                type $DT = u32;
                $integral
            }
            Literal::UInt64($value) => {
                type $DT = u64;
                $integral
            }
            Literal::Int8($value) => {
                type $DT = i8;
                $integral
            }
            Literal::Int16($value) => {
                type $DT = i16;
                $integral
            }
            Literal::Int32($value) => {
                type $DT = i32;
                $integral
            }
            Literal::Int64($value) => {
                type $DT = i64;
                $integral
            }
            Literal::Float32($value) => {
                type $DT = f32;
                $float
            }
            Literal::Float64($value) => {
                type $DT = f64;
                $float
            }
            Literal::String($value) => {
                type $DT = String;
                $str
            }
        }
    }};

    ($expr:expr, $value:pat, $then:expr) => {{ literal_go!($expr, _DT, $value, $then, $then, $then) }};

    ($expr:expr, $value:pat, $numeric:expr, $str:expr) => {{ literal_go!($expr, _DT, $value, $numeric, $numeric, $str) }};

    ($expr:expr, $value:pat, $numeric:expr, $float:expr, $str:expr) => {{ literal_go!($expr, _DT, $value, $numeric, $float, $str) }};
}

impl Literal {
    pub fn to_bytes(&self) -> Vec<u8> {
        literal_go!(
            self,
            val,
            val.to_le_bytes().to_vec(),
            val.as_bytes().to_vec()
        )
    }
}

impl Display for Literal {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        literal_go!(
            self,
            val,
            write!(f, "{val}"),
            write!(f, "'{}'", escape_string_literal(val))
        )
    }
}

/// Uses the [BitsHash] implementation of the wrapped value.
impl Hash for Literal {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        literal_go!(
            self,
            v,
            v.hash(state),
            BitsKeyAdapter(v).hash(state),
            v.hash(state)
        )
    }
}

impl PartialEq for Literal {
    fn eq(&self, other: &Self) -> bool {
        use self::Literal::*;
        match (self, other) {
            (UInt8(mine), UInt8(theirs)) => mine == theirs,
            (UInt16(mine), UInt16(theirs)) => mine == theirs,
            (UInt32(mine), UInt32(theirs)) => mine == theirs,
            (UInt64(mine), UInt64(theirs)) => mine == theirs,
            (Int8(mine), Int8(theirs)) => mine == theirs,
            (Int16(mine), Int16(theirs)) => mine == theirs,
            (Int32(mine), Int32(theirs)) => mine == theirs,
            (Int64(mine), Int64(theirs)) => mine == theirs,
            (Float32(mine), Float32(theirs)) => mine.bits_eq(theirs),
            (Float64(mine), Float64(theirs)) => mine.bits_eq(theirs),
            (String(mine), String(theirs)) => mine == theirs,
            _ => false,
        }
    }
}

/// The [PartialEq] implementation of [Literal] compares the
/// floating-point variants using [BitsEq],
/// and as such is an equivalence relation.
impl Eq for Literal {}

macro_rules! literal_from_impl {
    ($ty:ty, $constructor:expr) => {
        impl From<$ty> for Literal {
            fn from(value: $ty) -> Literal {
                $constructor(value)
            }
        }
    };
}

literal_from_impl!(u8, Literal::UInt8);
literal_from_impl!(u16, Literal::UInt16);
literal_from_impl!(u32, Literal::UInt32);
literal_from_impl!(u64, Literal::UInt64);
literal_from_impl!(i8, Literal::Int8);
literal_from_impl!(i16, Literal::Int16);
literal_from_impl!(i32, Literal::Int32);
literal_from_impl!(i64, Literal::Int64);
literal_from_impl!(f32, Literal::Float32);
literal_from_impl!(f64, Literal::Float64);
literal_from_impl!(String, Literal::String);

impl From<&str> for Literal {
    fn from(val: &str) -> Literal {
        Literal::String(val.to_string())
    }
}

fn escape_string_literal(s: &str) -> impl Display + '_ {
    s.escape_default()
}

// N.B. I initially tried slices here, but that breaks the Deserialize trait.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum SetMembers {
    UInt8(Vec<u8>),
    UInt16(Vec<u16>),
    UInt32(Vec<u32>),
    UInt64(Vec<u64>),
    Int8(Vec<i8>),
    Int16(Vec<i16>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    String(Vec<String>),
}

#[macro_export]
macro_rules! set_members_go {
    ($expr:expr, $DT:ident, $members:pat, $integral:expr, $float:expr, $str:expr) => {{
        match $expr {
            SetMembers::UInt8($members) => {
                type $DT = u8;
                $integral
            }
            SetMembers::UInt16($members) => {
                type $DT = u16;
                $integral
            }
            SetMembers::UInt32($members) => {
                type $DT = u32;
                $integral
            }
            SetMembers::UInt64($members) => {
                type $DT = u64;
                $integral
            }
            SetMembers::Int8($members) => {
                type $DT = i8;
                $integral
            }
            SetMembers::Int16($members) => {
                type $DT = i16;
                $integral
            }
            SetMembers::Int32($members) => {
                type $DT = i32;
                $integral
            }
            SetMembers::Int64($members) => {
                type $DT = i64;
                $integral
            }
            SetMembers::Float32($members) => {
                type $DT = f32;
                $float
            }
            SetMembers::Float64($members) => {
                type $DT = f64;
                $float
            }
            SetMembers::String($members) => {
                type $DT = String;
                $str
            }
        }
    }};

    ($expr:expr, $members:pat, $then:expr) => {
        set_members_go!($expr, __DT__, $members, $then, $then, $then)
    };
}

macro_rules! slice_to_ptr_and_size {
    ($val:expr) => {
        Some((
            $val.as_ptr() as *const std::ffi::c_void,
            std::mem::size_of_val($val.as_slice()) as u64,
        ))
    };
}

impl SetMembers {
    pub fn is_empty(&self) -> bool {
        set_members_go!(self, members, members.is_empty())
    }

    pub fn len(&self) -> usize {
        set_members_go!(self, members, members.len())
    }

    pub fn elem_size(&self) -> usize {
        match self {
            Self::UInt8(_) => std::mem::size_of::<u8>(),
            Self::UInt16(_) => std::mem::size_of::<u16>(),
            Self::UInt32(_) => std::mem::size_of::<u32>(),
            Self::UInt64(_) => std::mem::size_of::<u64>(),
            Self::Int8(_) => std::mem::size_of::<i8>(),
            Self::Int16(_) => std::mem::size_of::<i16>(),
            Self::Int32(_) => std::mem::size_of::<i32>(),
            Self::Int64(_) => std::mem::size_of::<i64>(),
            Self::Float32(_) => std::mem::size_of::<f32>(),
            Self::Float64(_) => std::mem::size_of::<f64>(),
            Self::String(_) => 0,
        }
    }

    pub fn as_ptr_and_size(&self) -> Option<(*const std::ffi::c_void, u64)> {
        match self {
            Self::UInt8(val) => slice_to_ptr_and_size!(val),
            Self::UInt16(val) => slice_to_ptr_and_size!(val),
            Self::UInt32(val) => slice_to_ptr_and_size!(val),
            Self::UInt64(val) => slice_to_ptr_and_size!(val),
            Self::Int8(val) => slice_to_ptr_and_size!(val),
            Self::Int16(val) => slice_to_ptr_and_size!(val),
            Self::Int32(val) => slice_to_ptr_and_size!(val),
            Self::Int64(val) => slice_to_ptr_and_size!(val),
            Self::Float32(val) => slice_to_ptr_and_size!(val),
            Self::Float64(val) => slice_to_ptr_and_size!(val),
            Self::String(_) => None,
        }
    }

    /// Helper function for `impl Display`
    fn display<T>(f: &mut Formatter, members: &[T]) -> FmtResult
    where
        T: Display,
    {
        if let Some((first, rest)) = members.split_first() {
            write!(f, "({first}")?;
            rest.iter().try_for_each(|value| write!(f, ", {value}"))?;
            write!(f, ")")
        } else {
            write!(f, "()")
        }
    }
}

impl Display for SetMembers {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::UInt8(members) => Self::display(f, members),
            Self::UInt16(members) => Self::display(f, members),
            Self::UInt32(members) => Self::display(f, members),
            Self::UInt64(members) => Self::display(f, members),
            Self::Int8(members) => Self::display(f, members),
            Self::Int16(members) => Self::display(f, members),
            Self::Int32(members) => Self::display(f, members),
            Self::Int64(members) => Self::display(f, members),
            Self::Float32(members) => Self::display(f, members),
            Self::Float64(members) => Self::display(f, members),
            Self::String(members) => {
                if let Some((first, rest)) = members.split_first() {
                    write!(f, "('{}'", escape_string_literal(first))?;
                    rest.iter().try_for_each(|value| {
                        write!(f, ", '{}'", escape_string_literal(value))
                    })?;
                    write!(f, ")")
                } else {
                    write!(f, "()")
                }
            }
        }
    }
}

/// Uses the [BitsHash] implementation of the wrapped values.
impl Hash for SetMembers {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        use self::SetMembers::*;

        match self {
            UInt8(v) => v.hash(state),
            UInt16(v) => v.hash(state),
            UInt32(v) => v.hash(state),
            UInt64(v) => v.hash(state),
            Int8(v) => v.hash(state),
            Int16(v) => v.hash(state),
            Int32(v) => v.hash(state),
            Int64(v) => v.hash(state),
            Float32(v) => v.bits_hash(state),
            Float64(v) => v.bits_hash(state),
            String(v) => v.hash(state),
        }
    }
}

impl PartialEq for SetMembers {
    fn eq(&self, other: &Self) -> bool {
        use self::SetMembers::*;

        match (self, other) {
            (UInt8(mine), UInt8(theirs)) => mine == theirs,
            (UInt16(mine), UInt16(theirs)) => mine == theirs,
            (UInt32(mine), UInt32(theirs)) => mine == theirs,
            (UInt64(mine), UInt64(theirs)) => mine == theirs,
            (Int8(mine), Int8(theirs)) => mine == theirs,
            (Int16(mine), Int16(theirs)) => mine == theirs,
            (Int32(mine), Int32(theirs)) => mine == theirs,
            (Int64(mine), Int64(theirs)) => mine == theirs,
            (Float32(mine), Float32(theirs)) => mine.bits_eq(theirs),
            (Float64(mine), Float64(theirs)) => mine.bits_eq(theirs),
            (String(mine), String(theirs)) => mine == theirs,
            _ => false,
        }
    }
}

/// The [PartialEq] implementation of [SetMembers] compares the
/// floating-point variants using [BitsEq],
/// and as such is an equivalence relation.
impl Eq for SetMembers {}

macro_rules! set_member_value_impl {
    ($ty:ty, $constructor:expr) => {
        impl From<&[$ty]> for SetMembers {
            fn from(value: &[$ty]) -> SetMembers {
                $constructor(value.to_vec())
            }
        }

        impl FromIterator<$ty> for SetMembers {
            fn from_iter<T>(iter: T) -> Self
            where
                T: IntoIterator<Item = $ty>,
            {
                $constructor(iter.into_iter().collect::<Vec<_>>())
            }
        }
    };
}

set_member_value_impl!(u8, SetMembers::UInt8);
set_member_value_impl!(u16, SetMembers::UInt16);
set_member_value_impl!(u32, SetMembers::UInt32);
set_member_value_impl!(u64, SetMembers::UInt64);
set_member_value_impl!(i8, SetMembers::Int8);
set_member_value_impl!(i16, SetMembers::Int16);
set_member_value_impl!(i32, SetMembers::Int32);
set_member_value_impl!(i64, SetMembers::Int64);
set_member_value_impl!(f32, SetMembers::Float32);
set_member_value_impl!(f64, SetMembers::Float64);
set_member_value_impl!(String, SetMembers::String);

impl<'a> FromIterator<&'a str> for SetMembers {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = &'a str>,
    {
        SetMembers::String(
            iter.into_iter().map(|s| s.to_owned()).collect::<Vec<_>>(),
        )
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct EqualityPredicate {
    field: String,
    op: EqualityOp,
    value: Literal,
}

impl EqualityPredicate {
    pub fn field(&self) -> &str {
        &self.field
    }

    pub fn operation(&self) -> EqualityOp {
        self.op
    }

    pub fn value(&self) -> &Literal {
        &self.value
    }

    /// Returns an [EqualityPredicate] which is true if and only if `self` is false.
    pub fn negate(&self) -> Self {
        Self {
            field: self.field.clone(),
            op: !self.op,
            value: self.value.clone(),
        }
    }
}

impl Display for EqualityPredicate {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{} {} {}", self.field, self.op, self.value)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct SetMembershipPredicate {
    field: String,
    op: SetMembershipOp,
    members: SetMembers,
}

impl SetMembershipPredicate {
    pub fn field(&self) -> &str {
        &self.field
    }

    pub fn operation(&self) -> SetMembershipOp {
        self.op
    }

    pub fn members(&self) -> &SetMembers {
        &self.members
    }

    /// Returns a [SetMembershipPredicate] which is true if and only if `self` is false.
    pub fn negate(&self) -> Self {
        Self {
            field: self.field.clone(),
            op: !self.op,
            members: self.members.clone(),
        }
    }
}

impl Display for SetMembershipPredicate {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{} {} {}", self.field, self.op, self.members)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct NullnessPredicate {
    field: String,
    op: NullnessOp,
}

impl NullnessPredicate {
    pub fn field(&self) -> &str {
        &self.field
    }

    pub fn operation(&self) -> NullnessOp {
        self.op
    }

    /// Returns a [NullnessPredicate] which is true if and only if `self` is false.
    pub fn negate(&self) -> Self {
        Self {
            field: self.field.clone(),
            op: !self.op,
        }
    }
}

impl Display for NullnessPredicate {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{} {}", self.field, self.op)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum Predicate {
    Equality(EqualityPredicate),
    SetMembership(SetMembershipPredicate),
    Nullness(NullnessPredicate),
}

impl Predicate {
    /// Returns a [Predicate] which is true if and only if `self` is false.
    pub fn negate(&self) -> Self {
        match self {
            Self::Equality(eq) => Self::Equality(eq.negate()),
            Self::SetMembership(set) => Self::SetMembership(set.negate()),
            Self::Nullness(nn) => Self::Nullness(nn.negate()),
        }
    }
}

impl Display for Predicate {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::Equality(e) => write!(f, "{e}"),
            Self::SetMembership(m) => write!(f, "{m}"),
            Self::Nullness(n) => write!(f, "{n}"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct Field {
    field: String,
}

impl Field {
    /// Returns whether the `Datatype` and `CellValNum` are allowed
    /// as the type of a query condition field.
    pub fn is_allowed_type(
        datatype: Datatype,
        cell_val_num: CellValNum,
    ) -> bool {
        match cell_val_num {
            CellValNum::Var => {
                matches!(datatype, Datatype::StringAscii | Datatype::StringUtf8)
            }
            CellValNum::Fixed(nz) if nz.get() == 1 => !matches!(
                datatype,
                Datatype::Any
                    | Datatype::Blob
                    | Datatype::GeometryWkb
                    | Datatype::GeometryWkt
                    | Datatype::StringUtf16
                    | Datatype::StringUtf32
                    | Datatype::StringUcs2
                    | Datatype::StringUcs4
            ),
            _ => false,
        }
    }

    pub fn lt<V: Into<Literal>>(self, value: V) -> QueryConditionExpr {
        QueryConditionExpr::Cond(Predicate::Equality(EqualityPredicate {
            field: self.field,
            op: EqualityOp::Less,
            value: value.into(),
        }))
    }

    pub fn le<V: Into<Literal>>(self, value: V) -> QueryConditionExpr {
        QueryConditionExpr::Cond(Predicate::Equality(EqualityPredicate {
            field: self.field,
            op: EqualityOp::LessEqual,
            value: value.into(),
        }))
    }

    pub fn eq<V: Into<Literal>>(self, value: V) -> QueryConditionExpr {
        QueryConditionExpr::Cond(Predicate::Equality(EqualityPredicate {
            field: self.field,
            op: EqualityOp::Equal,
            value: value.into(),
        }))
    }

    pub fn ne<V: Into<Literal>>(self, value: V) -> QueryConditionExpr {
        QueryConditionExpr::Cond(Predicate::Equality(EqualityPredicate {
            field: self.field,
            op: EqualityOp::NotEqual,
            value: value.into(),
        }))
    }

    pub fn ge<V: Into<Literal>>(self, value: V) -> QueryConditionExpr {
        QueryConditionExpr::Cond(Predicate::Equality(EqualityPredicate {
            field: self.field,
            op: EqualityOp::GreaterEqual,
            value: value.into(),
        }))
    }

    pub fn gt<V: Into<Literal>>(self, value: V) -> QueryConditionExpr {
        QueryConditionExpr::Cond(Predicate::Equality(EqualityPredicate {
            field: self.field,
            op: EqualityOp::Greater,
            value: value.into(),
        }))
    }

    pub fn is_in<V>(self, value: V) -> QueryConditionExpr
    where
        V: IntoIterator,
        SetMembers: FromIterator<<V as IntoIterator>::Item>,
    {
        QueryConditionExpr::Cond(Predicate::SetMembership(
            SetMembershipPredicate {
                field: self.field,
                op: SetMembershipOp::In,
                members: value.into_iter().collect(),
            },
        ))
    }

    pub fn not_in<V>(self, value: V) -> QueryConditionExpr
    where
        V: IntoIterator,
        SetMembers: FromIterator<<V as IntoIterator>::Item>,
    {
        QueryConditionExpr::Cond(Predicate::SetMembership(
            SetMembershipPredicate {
                field: self.field,
                op: SetMembershipOp::NotIn,
                members: value.into_iter().collect(),
            },
        ))
    }

    pub fn is_null(self) -> QueryConditionExpr {
        QueryConditionExpr::Cond(Predicate::Nullness(NullnessPredicate {
            field: self.field,
            op: NullnessOp::IsNull,
        }))
    }

    pub fn not_null(self) -> QueryConditionExpr {
        QueryConditionExpr::Cond(Predicate::Nullness(NullnessPredicate {
            field: self.field,
            op: NullnessOp::NotNull,
        }))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum QueryConditionExpr {
    Cond(Predicate),
    Comb {
        lhs: Box<QueryConditionExpr>,
        rhs: Box<QueryConditionExpr>,
        op: CombinationOp,
    },
    Negate(Box<QueryConditionExpr>),
}

impl QueryConditionExpr {
    pub fn field<F: AsRef<str>>(field: F) -> Field {
        Field {
            field: field.as_ref().to_owned(),
        }
    }

    /// Returns a [QueryConditionExpr] which is true if and only if `self` is false.
    ///
    /// The returned expression tree is equivalent to, but distinct from,
    /// `QueryConditionExpr::Negate(Box::new(self.clone()))`.
    pub fn negate(&self) -> Self {
        match self {
            Self::Cond(predicate) => Self::Cond(predicate.negate()),
            Self::Comb { lhs, rhs, op } => Self::Comb {
                lhs: Box::new(lhs.negate()),
                rhs: Box::new(rhs.negate()),
                op: match op {
                    CombinationOp::And => CombinationOp::Or,
                    CombinationOp::Or => CombinationOp::And,
                },
            },
            Self::Negate(predicate) => predicate.as_ref().clone(),
        }
    }
}

impl BitAnd for QueryConditionExpr {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self::Output {
        QueryConditionExpr::Comb {
            lhs: Box::new(self),
            rhs: Box::new(rhs),
            op: CombinationOp::And,
        }
    }
}

impl BitOr for QueryConditionExpr {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self::Output {
        QueryConditionExpr::Comb {
            lhs: Box::new(self),
            rhs: Box::new(rhs),
            op: CombinationOp::Or,
        }
    }
}

impl Not for QueryConditionExpr {
    type Output = Self;
    fn not(self) -> Self::Output {
        QueryConditionExpr::Negate(Box::new(self))
    }
}

impl Display for QueryConditionExpr {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::Cond(pred) => write!(f, "{pred}"),
            Self::Comb { lhs, rhs, op } => {
                write!(f, "({lhs}) {op} ({rhs})")
            }
            Self::Negate(pred) => write!(f, "NOT ({pred})"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::QueryConditionExpr as QC;
    use super::*;

    #[test]
    fn display() {
        let qc_cmp = QC::field("field").lt(5);
        assert_eq!("field < 5", qc_cmp.to_string());

        let qc_setmemb = QC::field("field").is_in(["one", "two", "three"]);
        assert_eq!("field IN ('one', 'two', 'three')", qc_setmemb.to_string());

        let qc_setmemb = QC::field("field").not_in(["one", "two", "three"]);
        assert_eq!(
            "field NOT IN ('one', 'two', 'three')",
            qc_setmemb.to_string()
        );

        let qc_nullness = QC::field("field").not_null();
        assert_eq!("field IS NOT NULL", qc_nullness.to_string());

        let qc_comb = qc_cmp.clone() & qc_setmemb.clone();
        assert_eq!(
            format!("({qc_cmp}) AND ({qc_setmemb})"),
            qc_comb.to_string()
        );

        let qc_neg = !qc_nullness.clone();
        assert_eq!(format!("NOT ({qc_nullness})"), qc_neg.to_string());

        /* parentheses should leave no ambiguity */
        let atom = QC::field("x").lt(5);
        let qc_tree = (atom.clone() | atom.clone())
            & (!atom.clone() | !(atom.clone() & atom.clone()));

        assert_eq!(
            "((x < 5) OR (x < 5)) AND ((NOT (x < 5)) OR (NOT ((x < 5) AND (x < 5))))",
            qc_tree.to_string()
        );
    }

    #[test]
    fn display_literal() {
        assert_eq!("'foo'", Literal::String("foo".to_owned()).to_string());
        assert_eq!("'f\\\\o'", Literal::String("f\\o".to_owned()).to_string());
        assert_eq!("'f\\\"o'", Literal::String("f\"o".to_owned()).to_string());
        assert_eq!("'f\\'o'", Literal::String("f'o".to_owned()).to_string());
    }
}
