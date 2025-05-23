use crate::array_type::FFIArrayType;
use crate::datatype::FFIDatatype;
use crate::encryption_type::FFIEncryptionType;
use crate::layout::{FFICellOrder, FFITileOrder};
use crate::mode::FFIMode;
use crate::query_status::FFIQueryStatus;

#[derive(Clone, Debug, thiserror::Error)]
pub enum TryFromFFIError {
    #[error("Invalid discriminant for {0}: {1:?}")]
    InvalidDiscriminant(String, u64),
}

impl TryFromFFIError {
    pub fn from_array_type(at: FFIArrayType) -> Self {
        Self::InvalidDiscriminant(
            std::any::type_name::<FFIArrayType>().to_string(),
            at.repr as u64,
        )
    }

    pub fn from_cell_order(order: FFICellOrder) -> Self {
        Self::InvalidDiscriminant(
            std::any::type_name::<FFICellOrder>().to_string(),
            order.repr as u64,
        )
    }

    pub fn from_datatype(dt: FFIDatatype) -> Self {
        Self::InvalidDiscriminant(
            std::any::type_name::<FFIDatatype>().to_string(),
            dt.repr as u64,
        )
    }

    pub fn from_encryption_type(etype: FFIEncryptionType) -> Self {
        Self::InvalidDiscriminant(
            std::any::type_name::<FFIEncryptionType>().to_string(),
            etype.repr as u64,
        )
    }

    pub fn from_mode(mode: FFIMode) -> Self {
        Self::InvalidDiscriminant(
            std::any::type_name::<FFIMode>().to_string(),
            mode.repr as u64,
        )
    }

    pub fn from_query_status(status: FFIQueryStatus) -> Self {
        Self::InvalidDiscriminant(
            std::any::type_name::<FFIQueryStatus>().to_string(),
            status.repr as u64,
        )
    }

    pub fn from_tile_order(order: FFITileOrder) -> Self {
        Self::InvalidDiscriminant(
            std::any::type_name::<FFITileOrder>().to_string(),
            order.repr as u64,
        )
    }
}
