#[derive(Clone)]
pub enum QueryStatus {
    Failed,
    Completed,
    InProgress,
    Incomplete,
    Uninitialized,
    Initialized,
    Invalid,
}

impl From<ffi::tiledb_query_status_t> for QueryStatus {
    fn from(value: ffi::tiledb_query_status_t) -> QueryStatus {
        match value {
            ffi::tiledb_query_status_t_TILEDB_FAILED => QueryStatus::Failed,
            ffi::tiledb_query_status_t_TILEDB_COMPLETED => {
                QueryStatus::Completed
            }
            ffi::tiledb_query_status_t_TILEDB_INPROGRESS => {
                QueryStatus::InProgress
            }
            ffi::tiledb_query_status_t_TILEDB_INCOMPLETE => {
                QueryStatus::Incomplete
            }
            ffi::tiledb_query_status_t_TILEDB_UNINITIALIZED => {
                QueryStatus::Uninitialized
            }
            ffi::tiledb_query_status_t_TILEDB_INITIALIZED => {
                QueryStatus::Initialized
            }
            _ => QueryStatus::Invalid,
        }
    }
}

#[derive(Clone)]
pub enum QueryStatusDetails {
    None,
    UserBufferSize,
    MemoryBudget,
    Invalid,
}

impl QueryStatusDetails {
    pub fn user_buffer_size(&self) -> bool {
        matches!(self, QueryStatusDetails::UserBufferSize)
    }
}

impl From<ffi::tiledb_query_status_details_reason_t> for QueryStatusDetails {
    fn from(
        value: ffi::tiledb_query_status_details_reason_t,
    ) -> QueryStatusDetails {
        match value {
            ffi::tiledb_query_status_details_reason_t_TILEDB_REASON_NONE =>
                QueryStatusDetails::None,
            ffi::tiledb_query_status_details_reason_t_TILEDB_REASON_USER_BUFFER_SIZE =>
                QueryStatusDetails::UserBufferSize,
            ffi::tiledb_query_status_details_reason_t_TILEDB_REASON_MEMORY_BUDGET =>
                QueryStatusDetails::MemoryBudget,
            _ => QueryStatusDetails::Invalid,
        }
    }
}
