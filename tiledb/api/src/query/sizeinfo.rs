use std::pin::Pin;

#[derive(Debug, Clone)]
pub(crate) struct SizeEntry {
    pub(crate) data_size: Pin<Box<u64>>,
    pub(crate) offsets_size: Option<Pin<Box<u64>>>,
    pub(crate) validity_size: Option<Pin<Box<u64>>>,
}

#[derive(Debug, Clone)]
pub struct SizeInfo {
    pub data_size: u64,
    pub offsets_size: Option<u64>,
    pub validity_size: Option<u64>,
}

impl From<&SizeEntry> for SizeInfo {
    fn from(value: &SizeEntry) -> SizeInfo {
        SizeInfo {
            data_size: *value.data_size,
            offsets_size: value.offsets_size.as_ref().map(|o| **o),
            validity_size: value.validity_size.as_ref().map(|v| **v),
        }
    }
}
