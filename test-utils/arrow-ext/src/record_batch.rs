use arrow::record_batch::RecordBatch;
use tiledb_common::range::Range;

pub trait RecordBatchExt {
    fn domain(&self) -> Vec<(String, Option<Range>)>;
}

impl RecordBatchExt for RecordBatch {
    fn domain(&self) -> Vec<(String, Option<Range>)> {
        todo!()
    }
}
