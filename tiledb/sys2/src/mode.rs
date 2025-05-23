use tiledb_common::array::Mode;

use crate::error::TryFromFFIError;

#[cxx::bridge(namespace = "tiledb::rs")]
mod ffi {
    #[derive(Debug)]
    pub enum Mode {
        Read,
        Write,
        Delete,
        Update,
        ModifyExclusive,
    }
}

pub use ffi::Mode as FFIMode;

impl From<Mode> for FFIMode {
    fn from(mode: Mode) -> FFIMode {
        match mode {
            Mode::Read => FFIMode::Read,
            Mode::Write => FFIMode::Write,
            Mode::Delete => FFIMode::Delete,
            Mode::Update => FFIMode::Update,
            Mode::ModifyExclusive => FFIMode::ModifyExclusive,
        }
    }
}

impl TryFrom<FFIMode> for Mode {
    type Error = TryFromFFIError;

    fn try_from(mode: FFIMode) -> Result<Self, Self::Error> {
        let mode = match mode {
            FFIMode::Read => Mode::Read,
            FFIMode::Write => Mode::Write,
            FFIMode::Delete => Mode::Delete,
            FFIMode::Update => Mode::Update,
            FFIMode::ModifyExclusive => Mode::ModifyExclusive,
            _ => return Err(TryFromFFIError::from_mode(mode)),
        };
        Ok(mode)
    }
}
