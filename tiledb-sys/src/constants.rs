pub const TILEDB_OK: i32 = 0;
pub const TILEDB_ERR: i32 = -1;
pub const TILEDB_OOM: i32 = -2;
pub const TILEDB_INVALID_CONTEXT: i32 = -3;
pub const TILEDB_INVALID_ERROR: i32 = -4;
pub const TILEDB_BUDGET_UNAVAILABLE: i32 = -5;

pub enum FilesystemType {
    HDFS = 0,
    S3 = 1,
    AZURE = 2,
    GCS = 3,
    MEMFS = 4,
}
