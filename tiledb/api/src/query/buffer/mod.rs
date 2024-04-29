pub mod read;
pub mod write;

pub trait BufferCollectionItem {
    fn name(&self) -> &str;

    fn data_ptr(&self) -> *mut std::ffi::c_void;
    fn data_size(&self) -> u64;

    fn offsets_ptr(&self) -> Option<*mut u64>;
    fn offsets_size(&self) -> Option<u64>;

    fn validity_ptr(&self) -> Option<*mut u8>;
    fn validity_size(&self) -> Option<u64>;
}

pub use read::{
    ReadBuffer, ReadBufferCollection, ReadBufferCollectionEntry,
    ReadBufferCollectionItem,
};
pub use write::{
    AllocatedWriteBuffer, WriteBuffer, WriteBufferCollection,
    WriteBufferCollectionEntry, WriteBufferCollectionItem,
};
