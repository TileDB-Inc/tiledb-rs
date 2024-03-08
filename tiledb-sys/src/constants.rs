pub const TILEDB_OK: i32 = 0;
pub const TILEDB_ERR: i32 = -1;
pub const TILEDB_OOM: i32 = -2;

pub enum FilesystemType {
    HDFS = 0,
    S3 = 1,
    AZURE = 2,
    GCS = 3,
    MEMFS = 4,
}
