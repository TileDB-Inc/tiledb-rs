use tiledb_common::array::EncryptionType;

use crate::error::TryFromFFIError;

#[cxx::bridge(namespace = "tiledb::rs")]
mod ffi {
    #[derive(Debug)]
    pub enum EncryptionType {
        None,
        Aes256Gcm,
    }
}

pub use ffi::EncryptionType as FFIEncryptionType;

impl From<EncryptionType> for FFIEncryptionType {
    fn from(etype: EncryptionType) -> Self {
        match etype {
            EncryptionType::None => Self::None,
            EncryptionType::Aes256Gcm => Self::Aes256Gcm,
        }
    }
}

impl TryFrom<FFIEncryptionType> for EncryptionType {
    type Error = TryFromFFIError;

    fn try_from(etype: FFIEncryptionType) -> Result<Self, Self::Error> {
        let etype = match etype {
            FFIEncryptionType::None => Self::None,
            FFIEncryptionType::Aes256Gcm => Self::Aes256Gcm,
            _ => return Err(TryFromFFIError::from_encryption_type(etype)),
        };
        Ok(etype)
    }
}
