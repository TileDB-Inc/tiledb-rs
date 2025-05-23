use tiledb_common::query::QueryStatus;

use crate::error::TryFromFFIError;

#[cxx::bridge(namespace = "tiledb::rs")]
mod ffi {
    #[derive(Debug)]
    pub enum QueryStatus {
        Failed,
        Completed,
        InProgress,
        Incomplete,
        Uninitialized,
        Initialized,
    }
}

pub use ffi::QueryStatus as FFIQueryStatus;

impl From<QueryStatus> for FFIQueryStatus {
    fn from(status: QueryStatus) -> FFIQueryStatus {
        match status {
            QueryStatus::Failed => Self::Failed,
            QueryStatus::Completed => Self::Completed,
            QueryStatus::InProgress => Self::InProgress,
            QueryStatus::Incomplete => Self::Incomplete,
            QueryStatus::Uninitialized => Self::Uninitialized,
            QueryStatus::Initialized => Self::Initialized,
        }
    }
}

impl TryFrom<FFIQueryStatus> for QueryStatus {
    type Error = TryFromFFIError;

    fn try_from(status: FFIQueryStatus) -> Result<Self, Self::Error> {
        let status = match status {
            FFIQueryStatus::Failed => Self::Failed,
            FFIQueryStatus::Completed => Self::Completed,
            FFIQueryStatus::InProgress => Self::InProgress,
            FFIQueryStatus::Incomplete => Self::Incomplete,
            FFIQueryStatus::Uninitialized => Self::Uninitialized,
            FFIQueryStatus::Initialized => Self::Initialized,
            _ => return Err(TryFromFFIError::from_query_status(status)),
        };
        Ok(status)
    }
}
