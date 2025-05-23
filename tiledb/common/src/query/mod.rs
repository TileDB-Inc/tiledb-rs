pub mod condition;

#[derive(Debug)]
pub enum QueryStatus {
    Failed,
    Completed,
    InProgress,
    Incomplete,
    Uninitialized,
    Initialized,
}
