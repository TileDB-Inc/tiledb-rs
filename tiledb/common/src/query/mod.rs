pub mod condition;

#[derive(Debug, PartialEq, Eq)]
pub enum QueryStatus {
    Failed,
    Completed,
    InProgress,
    Incomplete,
    Uninitialized,
    Initialized,
}
