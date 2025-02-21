use std::ops::Deref;

use anyhow::anyhow;
pub use tiledb_common::query::condition::*;

use crate::context::Context;
use crate::error::Error;
use crate::Result as TileDBResult;

pub(super) trait QueryConditionBuilder {
    fn build(&self, ctx: &Context) -> TileDBResult<RawQueryCondition>;
}

pub(super) trait CApiEnum {
    type FFI;

    fn capi_enum(&self) -> Self::FFI;
}

impl CApiEnum for EqualityOp {
    type FFI = ffi::tiledb_query_condition_op_t;

    fn capi_enum(&self) -> Self::FFI {
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

impl CApiEnum for SetMembershipOp {
    type FFI = ffi::tiledb_query_condition_op_t;

    fn capi_enum(&self) -> Self::FFI {
        match self {
            Self::In => ffi::tiledb_query_condition_op_t_TILEDB_IN,
            Self::NotIn => ffi::tiledb_query_condition_op_t_TILEDB_NOT_IN,
        }
    }
}

impl CApiEnum for NullnessOp {
    type FFI = ffi::tiledb_query_condition_op_t;

    fn capi_enum(&self) -> Self::FFI {
        match self {
            Self::IsNull => ffi::tiledb_query_condition_op_t_TILEDB_EQ,
            Self::NotNull => ffi::tiledb_query_condition_op_t_TILEDB_NE,
        }
    }
}

impl CApiEnum for CombinationOp {
    type FFI = ffi::tiledb_query_condition_combination_op_t;

    fn capi_enum(&self) -> Self::FFI {
        match self {
            Self::And => {
                ffi::tiledb_query_condition_combination_op_t_TILEDB_AND
            }
            Self::Or => ffi::tiledb_query_condition_combination_op_t_TILEDB_OR,
        }
    }
}

impl QueryConditionBuilder for EqualityPredicate {
    fn build(&self, ctx: &Context) -> TileDBResult<RawQueryCondition> {
        let mut c_cond: *mut ffi::tiledb_query_condition_t = out_ptr!();
        ctx.capi_call(|ctx| unsafe {
            ffi::tiledb_query_condition_alloc(ctx, &mut c_cond)
        })?;

        let raw = RawQueryCondition::Owned(c_cond);

        let c_cond = *raw;
        let c_name = cstring!(self.field());
        let val = self.value().to_bytes();
        let c_ptr = val.as_ptr() as *const std::ffi::c_void;
        let c_size = val.len() as u64;
        let c_op = self.operation().capi_enum();
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

impl QueryConditionBuilder for SetMembershipPredicate {
    fn build(&self, ctx: &Context) -> TileDBResult<RawQueryCondition> {
        // First things first, sets require a non-zero length vector. I would
        // prefer if we couldn't even create SetMemberValues with zero length
        // vectors, but that would make creation fallible which would make the
        // API rather clunky.
        if self.members().len() == 0 {
            return Err(Error::InvalidArgument(anyhow!(
                "Set member values must have non-zero length."
            )));
        }

        let mut c_cond: *mut ffi::tiledb_query_condition_t = out_ptr!();

        if let Some((c_data, c_data_size)) = self.members().as_ptr_and_size() {
            // This handles all value variants that aren't strings. First we
            // create our offsets buffer and then create the query condition.
            assert!(!c_data.is_null());
            assert!(c_data_size > 0);

            let mut offsets = vec![0u64; self.members().len()];
            let mut curr_offset = 0;
            let elem_size = self.members().elem_size() as u64;

            // Guard against suddenly (and impossibly having a String variant)
            assert!(elem_size > 0);

            for offset in offsets.iter_mut().take(self.members().len()) {
                *offset = curr_offset;
                curr_offset += elem_size;
            }

            let c_offsets = offsets.as_ptr() as *const std::ffi::c_void;
            let c_offsets_size =
                std::mem::size_of_val(offsets.as_slice()) as u64;

            // Create the query condition
            let c_name = cstring!(self.field());
            let c_op = self.operation().capi_enum();
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

            let values = match self.members() {
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
            let c_name = cstring!(self.field());
            let c_op = self.operation().capi_enum();
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

impl QueryConditionBuilder for NullnessPredicate {
    fn build(&self, ctx: &Context) -> TileDBResult<RawQueryCondition> {
        let mut c_cond: *mut ffi::tiledb_query_condition_t = out_ptr!();
        ctx.capi_call(|ctx| unsafe {
            ffi::tiledb_query_condition_alloc(ctx, &mut c_cond)
        })?;

        let raw = RawQueryCondition::Owned(c_cond);

        let c_cond = *raw;
        let c_name = cstring!(self.field());
        let c_op = self.operation().capi_enum();
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

impl QueryConditionBuilder for Predicate {
    fn build(&self, ctx: &Context) -> TileDBResult<RawQueryCondition> {
        match self {
            Self::Equality(pred) => pred.build(ctx),
            Self::SetMembership(pred) => pred.build(ctx),
            Self::Nullness(pred) => pred.build(ctx),
        }
    }
}

impl QueryConditionBuilder for QueryConditionExpr {
    fn build(&self, ctx: &Context) -> TileDBResult<RawQueryCondition> {
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
}
