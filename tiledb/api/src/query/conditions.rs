use std::fmt::{Display, Formatter, Result as FmtResult};
use std::hash::{Hash, Hasher};
use std::ops::{BitAnd, BitOr, Deref, Not};

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use crate::context::Context;
use crate::datatype::physical::{BitsEq, BitsHash};
use crate::error::Error;
use crate::Result as TileDBResult;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum EqualityOp {
    Less,
    LessEqual,
    Equal,
    NotEqual,
    GreaterEqual,
    Greater,
}

impl EqualityOp {
    pub(crate) fn capi_enum(&self) -> ffi::tiledb_query_condition_op_t {
        match self {
            Self::Less => ffi::tiledb_query_condition_op_t_TILEDB_LT,
            Self::LessEqual => ffi::tiledb_query_condition_op_t_TILEDB_LE,
            Self::Equal => ffi::tiledb_query_condition_op_t_TILEDB_EQ,
            Self::NotEqual => ffi::tiledb_query_condition_op_t_TILEDB_NE,
            Self::GreaterEqual => ffi::tiledb_query_condition_op_t_TILEDB_GE,
            Self::Greater => ffi::tiledb_query_condition_op_t_TILEDB_GT,
        }
    }
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum SetMembershipOp {
    In,
    NotIn,
}

impl SetMembershipOp {
    pub(crate) fn capi_enum(&self) -> ffi::tiledb_query_condition_op_t {
        match self {
            Self::In => ffi::tiledb_query_condition_op_t_TILEDB_IN,
            Self::NotIn => ffi::tiledb_query_condition_op_t_TILEDB_NOT_IN,
        }
    }
}

impl Display for SetMembershipOp {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::In => write!(f, "IN"),
            Self::NotIn => write!(f, "NOT IN"),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum NullnessOp {
    IsNull,
    NotNull,
}

impl NullnessOp {
    pub(crate) fn capi_enum(&self) -> ffi::tiledb_query_condition_op_t {
        match self {
            Self::IsNull => ffi::tiledb_query_condition_op_t_TILEDB_EQ,
            Self::NotNull => ffi::tiledb_query_condition_op_t_TILEDB_NE,
        }
    }
}

impl Display for NullnessOp {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::IsNull => write!(f, "IS NULL"),
            Self::NotNull => write!(f, "IS NOT NULL"),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum CombinationOp {
    And,
    Or,
}

impl CombinationOp {
    pub(crate) fn capi_enum(
        &self,
    ) -> ffi::tiledb_query_condition_combination_op_t {
        match self {
            Self::And => {
                ffi::tiledb_query_condition_combination_op_t_TILEDB_AND
            }
            Self::Or => ffi::tiledb_query_condition_combination_op_t_TILEDB_OR,
        }
    }
}

impl Display for CombinationOp {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::And => write!(f, "AND"),
            Self::Or => write!(f, "OR"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
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

impl Literal {
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::UInt8(val) => val.to_le_bytes().to_vec(),
            Self::UInt16(val) => val.to_le_bytes().to_vec(),
            Self::UInt32(val) => val.to_le_bytes().to_vec(),
            Self::UInt64(val) => val.to_le_bytes().to_vec(),
            Self::Int8(val) => val.to_le_bytes().to_vec(),
            Self::Int16(val) => val.to_le_bytes().to_vec(),
            Self::Int32(val) => val.to_le_bytes().to_vec(),
            Self::Int64(val) => val.to_le_bytes().to_vec(),
            Self::Float32(val) => val.to_le_bytes().to_vec(),
            Self::Float64(val) => val.to_le_bytes().to_vec(),
            Self::String(val) => val.as_bytes().to_vec(),
        }
    }
}

impl Display for Literal {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::UInt8(value) => write!(f, "{}", value),
            Self::UInt16(value) => write!(f, "{}", value),
            Self::UInt32(value) => write!(f, "{}", value),
            Self::UInt64(value) => write!(f, "{}", value),
            Self::Int8(value) => write!(f, "{}", value),
            Self::Int16(value) => write!(f, "{}", value),
            Self::Int32(value) => write!(f, "{}", value),
            Self::Int64(value) => write!(f, "{}", value),
            Self::Float32(value) => write!(f, "{}", value),
            Self::Float64(value) => write!(f, "{}", value),
            Self::String(value) => {
                write!(f, "'{}'", escape_string_literal(value))
            }
        }
    }
}

impl Hash for Literal {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        use self::Literal::*;

        match self {
            UInt8(ref v) => v.hash(state),
            UInt16(ref v) => v.hash(state),
            UInt32(ref v) => v.hash(state),
            UInt64(ref v) => v.hash(state),
            Int8(ref v) => v.hash(state),
            Int16(ref v) => v.hash(state),
            Int32(ref v) => v.hash(state),
            Int64(ref v) => v.hash(state),
            Float32(ref v) => v.to_bits().hash(state),
            Float64(ref v) => v.to_bits().hash(state),
            String(ref v) => v.hash(state),
        }
    }
}

impl PartialEq for Literal {
    fn eq(&self, other: &Self) -> bool {
        use self::Literal::*;
        match (self, other) {
            (UInt8(ref mine), UInt8(ref theirs)) => mine == theirs,
            (UInt16(ref mine), UInt16(ref theirs)) => mine == theirs,
            (UInt32(ref mine), UInt32(ref theirs)) => mine == theirs,
            (UInt64(ref mine), UInt64(ref theirs)) => mine == theirs,
            (Int8(ref mine), Int8(ref theirs)) => mine == theirs,
            (Int16(ref mine), Int16(ref theirs)) => mine == theirs,
            (Int32(ref mine), Int32(ref theirs)) => mine == theirs,
            (Int64(ref mine), Int64(ref theirs)) => mine == theirs,
            (Float32(ref mine), Float32(ref theirs)) => mine.bits_eq(theirs),
            (Float64(ref mine), Float64(ref theirs)) => mine.bits_eq(theirs),
            (String(ref mine), String(ref theirs)) => mine == theirs,
            _ => false,
        }
    }
}

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
#[derive(Clone, Debug, Deserialize, Serialize)]
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

macro_rules! slice_to_ptr_and_size {
    ($val:expr) => {
        Some((
            $val.as_ptr() as *const std::ffi::c_void,
            std::mem::size_of_val($val) as u64,
        ))
    };
}

impl SetMembers {
    fn len(&self) -> usize {
        match self {
            Self::UInt8(val) => val.len(),
            Self::UInt16(val) => val.len(),
            Self::UInt32(val) => val.len(),
            Self::UInt64(val) => val.len(),
            Self::Int8(val) => val.len(),
            Self::Int16(val) => val.len(),
            Self::Int32(val) => val.len(),
            Self::Int64(val) => val.len(),
            Self::Float32(val) => val.len(),
            Self::Float64(val) => val.len(),
            Self::String(val) => val.len(),
        }
    }

    fn elem_size(&self) -> usize {
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

    fn as_ptr_and_size(&self) -> Option<(*const std::ffi::c_void, u64)> {
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

    fn display<T>(f: &mut Formatter, members: &[T]) -> FmtResult
    where
        T: Display,
    {
        if let Some((first, rest)) = members.split_first() {
            write!(f, "({}", first)?;
            rest.iter().try_for_each(|value| write!(f, ", {}", value))?;
            write!(f, ")")
        } else {
            write!(f, "()")
        }
    }
}

impl Display for SetMembers {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::UInt8(ref members) => Self::display(f, members),
            Self::UInt16(ref members) => Self::display(f, members),
            Self::UInt32(ref members) => Self::display(f, members),
            Self::UInt64(ref members) => Self::display(f, members),
            Self::Int8(ref members) => Self::display(f, members),
            Self::Int16(ref members) => Self::display(f, members),
            Self::Int32(ref members) => Self::display(f, members),
            Self::Int64(ref members) => Self::display(f, members),
            Self::Float32(ref members) => Self::display(f, members),
            Self::Float64(ref members) => Self::display(f, members),
            Self::String(ref members) => {
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

impl Hash for SetMembers {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        use self::SetMembers::*;

        match self {
            UInt8(ref v) => v.hash(state),
            UInt16(ref v) => v.hash(state),
            UInt32(ref v) => v.hash(state),
            UInt64(ref v) => v.hash(state),
            Int8(ref v) => v.hash(state),
            Int16(ref v) => v.hash(state),
            Int32(ref v) => v.hash(state),
            Int64(ref v) => v.hash(state),
            Float32(ref v) => v.bits_hash(state),
            Float64(ref v) => v.bits_hash(state),
            String(ref v) => v.hash(state),
        }
    }
}

impl PartialEq for SetMembers {
    fn eq(&self, other: &Self) -> bool {
        use self::SetMembers::*;

        match (self, other) {
            (UInt8(ref mine), UInt8(ref theirs)) => mine == theirs,
            (UInt16(ref mine), UInt16(ref theirs)) => mine == theirs,
            (UInt32(ref mine), UInt32(ref theirs)) => mine == theirs,
            (UInt64(ref mine), UInt64(ref theirs)) => mine == theirs,
            (Int8(ref mine), Int8(ref theirs)) => mine == theirs,
            (Int16(ref mine), Int16(ref theirs)) => mine == theirs,
            (Int32(ref mine), Int32(ref theirs)) => mine == theirs,
            (Int64(ref mine), Int64(ref theirs)) => mine == theirs,
            (Float32(ref mine), Float32(ref theirs)) => mine.bits_eq(theirs),
            (Float64(ref mine), Float64(ref theirs)) => mine.bits_eq(theirs),
            (String(ref mine), String(ref theirs)) => mine == theirs,
            _ => false,
        }
    }
}

impl Eq for SetMembers {}

macro_rules! set_member_value_impl {
    ($ty:ty, $constructor:expr) => {
        impl From<&[$ty]> for SetMembers {
            fn from(value: &[$ty]) -> SetMembers {
                $constructor(value.to_vec())
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

impl From<&[&str]> for SetMembers {
    fn from(val: &[&str]) -> SetMembers {
        let mut owned = Vec::new();
        for v in val {
            owned.push(v.to_string())
        }
        SetMembers::String(owned)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct EqualityPredicate {
    field: String,
    op: EqualityOp,
    value: Literal,
}

impl EqualityPredicate {
    fn build(&self, ctx: &Context) -> TileDBResult<RawQueryCondition> {
        let mut c_cond: *mut ffi::tiledb_query_condition_t = out_ptr!();
        ctx.capi_call(|ctx| unsafe {
            ffi::tiledb_query_condition_alloc(ctx, &mut c_cond)
        })?;

        let raw = RawQueryCondition::Owned(c_cond);

        let c_cond = *raw;
        let c_name = cstring!(self.field.as_str());
        let val = self.value.to_bytes();
        let c_ptr = val.as_ptr() as *const std::ffi::c_void;
        let c_size = val.len() as u64;
        let c_op = self.op.capi_enum();
        ctx.capi_call(|ctx| unsafe {
            ffi::tiledb_query_condition_init(
                ctx,
                c_cond,
                c_name.as_ptr(),
                c_ptr,
                c_size,
                c_op,
            )
        })?;

        Ok(raw)
    }
}

impl Display for EqualityPredicate {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{} {} {}", self.field, self.op, self.value)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct SetMembershipPredicate {
    field: String,
    op: SetMembershipOp,
    members: SetMembers,
}

impl SetMembershipPredicate {
    fn build(&self, ctx: &Context) -> TileDBResult<RawQueryCondition> {
        // First things first, sets require a non-zero length vector. I would
        // prefer if we couldn't even create SetMemberValues with zero length
        // vectors, but that would make creation fallible which would make the
        // API rather clunky.
        if self.members.len() == 0 {
            return Err(Error::InvalidArgument(anyhow!(
                "Set member values must have non-zero length."
            )));
        }

        let mut c_cond: *mut ffi::tiledb_query_condition_t = out_ptr!();

        if let Some((c_data, c_data_size)) = self.members.as_ptr_and_size() {
            // This handles all value variants that aren't strings. First we
            // create our offsets buffer and then create the query condition.
            assert!(!c_data.is_null());
            assert!(c_data_size > 0);

            let mut offsets = vec![0u64; self.members.len()];
            let mut curr_offset = 0;
            let elem_size = self.members.elem_size() as u64;

            // Guard against suddenly (and impossibly having a String variant)
            assert!(elem_size > 0);

            for offset in offsets.iter_mut().take(self.members.len()) {
                *offset = curr_offset;
                curr_offset += elem_size;
            }

            let c_offsets = offsets.as_ptr() as *const std::ffi::c_void;
            let c_offsets_size = std::mem::size_of_val(&offsets) as u64;

            // Create the query condition
            let c_name = cstring!(self.field.as_str());
            let c_op = self.op.capi_enum();
            ctx.capi_call(|ctx| unsafe {
                ffi::tiledb_query_condition_alloc_set_membership(
                    ctx,
                    c_name.as_ptr(),
                    c_data,
                    c_data_size,
                    c_offsets,
                    c_offsets_size,
                    c_op,
                    &mut c_cond,
                )
            })?;
        } else {
            // Handle the String case. First we create our offsets vector
            // and then allocate and fill the data buffer.

            let values = match &self.members {
                SetMembers::String(val) => val,
                _ => unreachable!(),
            };

            let mut offsets = vec![0u64; values.len()];
            let mut curr_offset = 0u64;
            for (i, v) in values.iter().enumerate() {
                offsets[i] = curr_offset;
                curr_offset += v.len() as u64;
            }

            let mut data = vec![0u8; curr_offset as usize];
            for (i, v) in values.iter().enumerate() {
                let start = offsets[i] as usize;
                data[start..(start + v.len())].copy_from_slice(v.as_bytes())
            }

            let c_data = data.as_ptr() as *const std::ffi::c_void;
            let c_data_size = data.len() as u64;
            let c_offsets = offsets.as_ptr() as *const std::ffi::c_void;
            let c_offsets_size = std::mem::size_of_val(&offsets) as u64;

            // And create the query condition
            let c_name = cstring!(self.field.as_str());
            let c_op = self.op.capi_enum();
            ctx.capi_call(|ctx| unsafe {
                ffi::tiledb_query_condition_alloc_set_membership(
                    ctx,
                    c_name.as_ptr(),
                    c_data,
                    c_data_size,
                    c_offsets,
                    c_offsets_size,
                    c_op,
                    &mut c_cond,
                )
            })?;
        }

        Ok(RawQueryCondition::Owned(c_cond))
    }
}

impl Display for SetMembershipPredicate {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{} {} {}", self.field, self.op, self.members)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct NullnessPredicate {
    field: String,
    op: NullnessOp,
}

impl NullnessPredicate {
    fn build(&self, ctx: &Context) -> TileDBResult<RawQueryCondition> {
        let mut c_cond: *mut ffi::tiledb_query_condition_t = out_ptr!();
        ctx.capi_call(|ctx| unsafe {
            ffi::tiledb_query_condition_alloc(ctx, &mut c_cond)
        })?;

        let raw = RawQueryCondition::Owned(c_cond);

        let c_cond = *raw;
        let c_name = cstring!(self.field.as_str());
        let c_op = self.op.capi_enum();
        ctx.capi_call(|ctx| unsafe {
            ffi::tiledb_query_condition_init(
                ctx,
                c_cond,
                c_name.as_ptr(),
                std::ptr::null(),
                0,
                c_op,
            )
        })?;

        Ok(raw)
    }
}

impl Display for NullnessPredicate {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{} {}", self.field, self.op)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum Predicate {
    Equality(EqualityPredicate),
    SetMembership(SetMembershipPredicate),
    Nullness(NullnessPredicate),
}

impl Predicate {
    fn build(&self, ctx: &Context) -> TileDBResult<RawQueryCondition> {
        match self {
            Self::Equality(pred) => pred.build(ctx),
            Self::SetMembership(pred) => pred.build(ctx),
            Self::Nullness(pred) => pred.build(ctx),
        }
    }
}

impl Display for Predicate {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::Equality(ref e) => write!(f, "{}", e),
            Self::SetMembership(ref m) => write!(f, "{}", m),
            Self::Nullness(ref n) => write!(f, "{}", n),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Field {
    field: String,
}

impl Field {
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

    pub fn is_in<V: Into<SetMembers>>(self, value: V) -> QueryConditionExpr {
        QueryConditionExpr::Cond(Predicate::SetMembership(
            SetMembershipPredicate {
                field: self.field,
                op: SetMembershipOp::In,
                members: value.into(),
            },
        ))
    }

    pub fn not_in<V: Into<SetMembers>>(self, value: V) -> QueryConditionExpr {
        QueryConditionExpr::Cond(Predicate::SetMembership(
            SetMembershipPredicate {
                field: self.field,
                op: SetMembershipOp::NotIn,
                members: value.into(),
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

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
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

    pub(crate) fn build(
        &self,
        ctx: &Context,
    ) -> TileDBResult<RawQueryCondition> {
        match self {
            Self::Cond(cond) => cond.build(ctx),
            Self::Comb { lhs, rhs, op } => {
                let lhs = lhs.build(ctx)?;
                let rhs = rhs.build(ctx)?;

                let c_lhs = *lhs;
                let c_rhs = *rhs;
                let c_op = op.capi_enum();
                let mut c_cond: *mut ffi::tiledb_query_condition_t = out_ptr!();
                ctx.capi_call(|ctx| unsafe {
                    ffi::tiledb_query_condition_combine(
                        ctx,
                        c_lhs,
                        c_rhs,
                        c_op,
                        &mut c_cond,
                    )
                })?;

                Ok(RawQueryCondition::Owned(c_cond))
            }
            Self::Negate(expr) => {
                let cond = expr.build(ctx)?;
                let c_cond = *cond;
                let mut c_neg_cond: *mut ffi::tiledb_query_condition_t =
                    out_ptr!();
                ctx.capi_call(|ctx| unsafe {
                    ffi::tiledb_query_condition_negate(
                        ctx,
                        c_cond,
                        &mut c_neg_cond,
                    )
                })?;

                Ok(RawQueryCondition::Owned(c_neg_cond))
            }
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
            Self::Cond(ref pred) => write!(f, "{}", pred),
            Self::Comb {
                ref lhs,
                ref rhs,
                op,
            } => {
                write!(f, "({}) {} ({})", lhs, op, rhs)
            }
            Self::Negate(ref pred) => write!(f, "NOT ({})", pred),
        }
    }
}

pub(crate) enum RawQueryCondition {
    Owned(*mut ffi::tiledb_query_condition_t),
}

impl Deref for RawQueryCondition {
    type Target = *mut ffi::tiledb_query_condition_t;
    fn deref(&self) -> &Self::Target {
        let RawQueryCondition::Owned(ref ffi) = *self;
        ffi
    }
}

impl Drop for RawQueryCondition {
    fn drop(&mut self) {
        let RawQueryCondition::Owned(ref mut ffi) = *self;
        unsafe {
            ffi::tiledb_query_condition_free(ffi);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::QueryConditionExpr as QC;
    use super::*;

    #[test]
    fn basic_op_test() -> TileDBResult<()> {
        let qc1 = QC::field("field").lt(5);
        let qc2 = QC::field("field").ge(6);
        let qc = qc1 | qc2;

        let ctx = Context::new()?;
        assert!(qc.build(&ctx).is_ok());

        Ok(())
    }

    #[test]
    fn basic_op_string_test() -> TileDBResult<()> {
        let qc1 = QC::field("field").lt("ohai");
        let qc2 = QC::field("field").ge("ohai again");
        let qc = qc1 | qc2;

        let ctx = Context::new()?;
        assert!(qc.build(&ctx).is_ok());

        Ok(())
    }

    #[test]
    fn basic_set_test() -> TileDBResult<()> {
        let qc = QC::field("foo").is_in(&[1u32, 2, 3, 4, 5][..]);
        let ctx = Context::new()?;
        assert!(qc.build(&ctx).is_ok());

        Ok(())
    }

    #[test]
    fn basic_string_set_test() -> TileDBResult<()> {
        let qc = QC::field("foo").is_in(&["foo", "bar", "baz"][..]);
        let ctx = Context::new()?;
        assert!(qc.build(&ctx).is_ok());

        Ok(())
    }

    #[test]
    fn basic_combine_test() -> TileDBResult<()> {
        let qc1 = QC::field("x").lt(5);
        let qc2 = QC::field("y").gt(7);
        let qc = qc1 & qc2;
        let qc = qc | QC::field("z").ne(42);

        let ctx = Context::new()?;
        assert!(qc.build(&ctx).is_ok());

        Ok(())
    }

    #[test]
    fn basic_negation_test() -> TileDBResult<()> {
        let qc1 = QC::field("x").lt(5);
        let qc2 = QC::field("y").gt(7);
        let qc = qc1 & qc2;
        let qc = qc | QC::field("z").ne(42);
        let qc = !qc;

        let ctx = Context::new()?;
        assert!(qc.build(&ctx).is_ok());

        Ok(())
    }

    #[test]
    fn display() {
        let qc_cmp = QC::field("field").lt(5);
        assert_eq!("field < 5", qc_cmp.to_string());

        let qc_setmemb =
            QC::field("field").is_in(["one", "two", "three"].as_slice());
        assert_eq!("field IN ('one', 'two', 'three')", qc_setmemb.to_string());

        let qc_nullness = QC::field("field").not_null();
        assert_eq!("field IS NOT NULL", qc_nullness.to_string());

        let qc_comb = qc_cmp.clone() & qc_setmemb.clone();
        assert_eq!(
            format!("({}) AND ({})", qc_cmp, qc_setmemb),
            qc_comb.to_string()
        );

        let qc_neg = !qc_nullness.clone();
        assert_eq!(format!("NOT ({})", qc_nullness), qc_neg.to_string());

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
