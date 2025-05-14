use tiledb_common::array::ArrayType;

use crate::error::TryFromFFIError;

#[cxx::bridge(namespace = "tiledb::rs")]
mod ffi {
    #[derive(Debug)]
    pub enum ArrayType {
        /// A dense array
        Dense,
        /// A sparse array
        Sparse,
    }
}

pub use ffi::ArrayType as FFIArrayType;

impl From<ArrayType> for FFIArrayType {
    fn from(at: ArrayType) -> FFIArrayType {
        match at {
            ArrayType::Dense => FFIArrayType::Dense,
            ArrayType::Sparse => FFIArrayType::Sparse,
        }
    }
}

impl TryFrom<FFIArrayType> for ArrayType {
    type Error = TryFromFFIError;

    fn try_from(at: FFIArrayType) -> Result<Self, Self::Error> {
        let at = match at {
            FFIArrayType::Dense => ArrayType::Dense,
            FFIArrayType::Sparse => ArrayType::Sparse,
            _ => return Err(TryFromFFIError::from_array_type(at)),
        };
        Ok(at)
    }
}
