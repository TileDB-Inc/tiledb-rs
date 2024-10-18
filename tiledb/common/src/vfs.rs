use thiserror::Error;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum VFSModeError {
    #[error("Invalid discriminant for {}: {0}", std::any::type_name::<VFSMode>())]
    InvalidDiscriminant(u64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum VFSMode {
    Read,
    Write,
    Append,
}

impl From<VFSMode> for ffi::tiledb_vfs_mode_t {
    fn from(value: VFSMode) -> Self {
        match value {
            VFSMode::Read => ffi::tiledb_vfs_mode_t_TILEDB_VFS_READ,
            VFSMode::Write => ffi::tiledb_vfs_mode_t_TILEDB_VFS_WRITE,
            VFSMode::Append => ffi::tiledb_vfs_mode_t_TILEDB_VFS_APPEND,
        }
    }
}

impl TryFrom<ffi::tiledb_vfs_mode_t> for VFSMode {
    type Error = VFSModeError;
    fn try_from(value: ffi::tiledb_vfs_mode_t) -> Result<Self, Self::Error> {
        match value {
            ffi::tiledb_vfs_mode_t_TILEDB_VFS_READ => Ok(VFSMode::Read),
            ffi::tiledb_vfs_mode_t_TILEDB_VFS_WRITE => Ok(VFSMode::Write),
            ffi::tiledb_vfs_mode_t_TILEDB_VFS_APPEND => Ok(VFSMode::Append),
            _ => Err(VFSModeError::InvalidDiscriminant(value as u64)),
        }
    }
}
