use std::ffi::CString;
use std::ops::Deref;

use thiserror::Error;
use tiledb_api::context::{Context, ContextBound};
use tiledb_common::array::{CellValNum, CellValNumError};
use tiledb_common::datatype::{Datatype, TryFromFFIError as DatatypeError};

use crate::RawQuery;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Internal cell val num error: {0}")]
    CellValNum(#[from] CellValNumError),
    #[error("Internal datatype error: {0}")]
    Datatype(#[from] DatatypeError),
    #[error("Field name '{0}' error: {1}")]
    NameError(String, #[source] std::ffi::NulError),
    #[error("libtiledb error: {0}")]
    LibTileDB(#[from] tiledb_api::error::Error),
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub enum RawQueryField {
    Owned(Context, *mut ffi::tiledb_query_field_t),
}

impl ContextBound for RawQueryField {
    fn context(&self) -> Context {
        let Self::Owned(ref ctx, _) = self;
        ctx.clone()
    }
}

impl Deref for RawQueryField {
    type Target = *mut ffi::tiledb_query_field_t;

    fn deref(&self) -> &Self::Target {
        let Self::Owned(_, ref ffi) = self;
        ffi
    }
}

impl Drop for RawQueryField {
    fn drop(&mut self) {
        let Self::Owned(ref mut ctx, ref mut ffi) = self;
        ctx.capi_call(|ctx| unsafe { ffi::tiledb_query_field_free(ctx, ffi) })
            .expect("Internal error dropping `RawQueryField`");
    }
}

pub struct QueryField {
    raw: RawQueryField,
}

impl QueryField {
    pub(crate) fn get(
        context: &Context,
        query: &RawQuery,
        name: &str,
    ) -> Result<Self> {
        let c_query = **query;
        let c_name = CString::new(name)
            .map_err(|e| Error::NameError(name.to_owned(), e))?;
        let mut c_field = out_ptr!();
        context.capi_call(|ctx| unsafe {
            ffi::tiledb_query_get_field(
                ctx,
                c_query,
                c_name.as_c_str().as_ptr(),
                &mut c_field,
            )
        })?;

        let raw = RawQueryField::Owned(context.clone(), c_field);
        Ok(Self { raw })
    }

    pub fn datatype(&self) -> Result<Datatype> {
        let c_field = *self.raw;
        let mut c_datatype = out_ptr!();
        self.context().capi_call(|ctx| unsafe {
            ffi::tiledb_field_datatype(ctx, c_field, &mut c_datatype)
        })?;
        Ok(Datatype::try_from(c_datatype)?)
    }

    pub fn cell_val_num(&self) -> Result<CellValNum> {
        let c_field = *self.raw;
        let mut c_cvn = out_ptr!();
        self.context().capi_call(|ctx| unsafe {
            ffi::tiledb_field_cell_val_num(ctx, c_field, &mut c_cvn)
        })?;
        Ok(CellValNum::try_from(c_cvn)?)
    }

    pub fn nullable(&self) -> Result<bool> {
        let c_field = *self.raw;
        let mut c_nullable = out_ptr!();
        self.context().capi_call(|ctx| unsafe {
            ffi::tiledb_field_get_nullable(ctx, c_field, &mut c_nullable)
        })?;
        Ok(c_nullable != 0)
    }

    pub fn origin(&self) -> Result<QueryFieldOrigin> {
        todo!()
    }
}

impl ContextBound for QueryField {
    fn context(&self) -> Context {
        self.raw.context()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryFieldOrigin {
    Attribute,
    Dimension,
    Aggregate,
}

#[derive(Debug, Error)]
pub enum QueryFieldOriginError {
    #[error("Invalid discriminant for QueryFieldOriginError: {0}")]
    InvalidDiscriminant(u64),
}

impl From<QueryFieldOrigin> for ffi::tiledb_field_origin_t {
    fn from(value: QueryFieldOrigin) -> Self {
        match value {
            QueryFieldOrigin::Attribute => {
                ffi::tiledb_field_origin_t_TILEDB_ATTRIBUTE_FIELD
            }
            QueryFieldOrigin::Dimension => {
                ffi::tiledb_field_origin_t_TILEDB_DIMENSION_FIELD
            }
            QueryFieldOrigin::Aggregate => {
                ffi::tiledb_field_origin_t_TILEDB_AGGREGATE_FIELD
            }
        }
    }
}

impl TryFrom<ffi::tiledb_field_origin_t> for QueryFieldOrigin {
    type Error = QueryFieldOriginError;

    fn try_from(
        value: ffi::tiledb_field_origin_t,
    ) -> Result<Self, Self::Error> {
        match value {
            ffi::tiledb_field_origin_t_TILEDB_ATTRIBUTE_FIELD => {
                Ok(Self::Attribute)
            }
            ffi::tiledb_field_origin_t_TILEDB_DIMENSION_FIELD => {
                Ok(Self::Dimension)
            }
            ffi::tiledb_field_origin_t_TILEDB_AGGREGATE_FIELD => {
                Ok(Self::Aggregate)
            }
            _ => Err(QueryFieldOriginError::InvalidDiscriminant(value as u64)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ffi_query_origin() {
        for q in [
            QueryFieldOrigin::Attribute,
            QueryFieldOrigin::Dimension,
            QueryFieldOrigin::Aggregate,
        ]
        .into_iter()
        {
            assert_eq!(
                q,
                QueryFieldOrigin::try_from(ffi::tiledb_field_origin_t::from(q))
                    .unwrap()
            );
        }
    }
}
