use std::pin::Pin;

#[derive(Debug)]
pub struct SizeEntry {
    pub data_size: Pin<Box<u64>>,
    pub offsets_size: Option<Pin<Box<u64>>>,
    pub validity_size: Option<Pin<Box<u64>>>,
}
