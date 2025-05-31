use tiledb_sys2::buffer::Buffer;
use tiledb_sys2::datatype::Datatype;
use tiledb_sys2::types::PhysicalType;

use crate::error::TileDBError;

pub struct QueryBuffers {
    pub data: Buffer,
    pub offsets: Option<Buffer>,
    pub validity: Option<Buffer>,
}

const DEFAULT_SIZE: usize = 10_485_760; // 10MiB

impl QueryBuffers {
    /// Create a new data-only QueryBuffers instance.
    ///
    /// This defaults to allocating 10MiB of whatever datatype is provided.
    pub fn new(datatype: Datatype) -> Self {
        let capacity = DEFAULT_SIZE / datatype.size();
        Self {
            data: Buffer::with_capacity(datatype, capacity),
            offsets: None,
            validity: None,
        }
    }

    pub fn new_with_offsets(datatype: Datatype) -> Self {
        let data_capacity = DEFAULT_SIZE / datatype.size();
        let offsets_capacity = DEFAULT_SIZE / 8;
        Self {
            data: Buffer::with_capacity(datatype, data_capacity),
            offsets: Some(Buffer::with_capacity(
                Datatype::UInt64,
                offsets_capacity,
            )),
            validity: None,
        }
    }

    pub fn new_with_validity(datatype: Datatype) -> Self {
        let data_capacity = DEFAULT_SIZE / datatype.size();
        let validity_capacity = DEFAULT_SIZE;
        Self {
            data: Buffer::with_capacity(datatype, data_capacity),
            offsets: None,
            validity: Some(Buffer::with_capacity(
                Datatype::UInt8,
                validity_capacity,
            )),
        }
    }

    pub fn new_with_offsets_and_validity(datatype: Datatype) -> Self {
        let data_capacity = DEFAULT_SIZE / datatype.size();
        let offsets_capacity = DEFAULT_SIZE / 8;
        let validity_capacity = DEFAULT_SIZE;
        Self {
            data: Buffer::with_capacity(datatype, data_capacity),
            offsets: Some(Buffer::with_capacity(
                Datatype::UInt64,
                offsets_capacity,
            )),
            validity: Some(Buffer::with_capacity(
                Datatype::UInt8,
                validity_capacity,
            )),
        }
    }

    pub fn with_capacity(datatype: Datatype, capacity: usize) -> Self {
        Self {
            data: Buffer::with_capacity(datatype, capacity),
            offsets: None,
            validity: None,
        }
    }

    pub fn with_offsets(self, capacity: usize) -> Self {
        Self {
            offsets: Some(Buffer::with_capacity(Datatype::UInt64, capacity)),
            ..self
        }
    }

    pub fn with_validity(self, capacity: usize) -> Self {
        Self {
            validity: Some(Buffer::with_capacity(Datatype::UInt8, capacity)),
            ..self
        }
    }
}

impl From<Buffer> for QueryBuffers {
    fn from(data: Buffer) -> Self {
        Self {
            data,
            offsets: None,
            validity: None,
        }
    }
}

impl<T: PhysicalType> TryFrom<(Datatype, Vec<T>)> for QueryBuffers {
    type Error = TileDBError;

    fn try_from(data: (Datatype, Vec<T>)) -> Result<Self, Self::Error> {
        let buffer = Buffer::try_from(data)?;
        Ok(buffer.into())
    }
}

impl<T: PhysicalType> TryFrom<(Datatype, Vec<T>, Vec<u64>)> for QueryBuffers {
    type Error = TileDBError;

    fn try_from(
        data: (Datatype, Vec<T>, Vec<u64>),
    ) -> Result<Self, Self::Error> {
        let values = Buffer::try_from((data.0, data.1))?;
        let offsets = Buffer::try_from((Datatype::UInt64, data.2))?;
        Ok(Self {
            data: values,
            offsets: Some(offsets),
            validity: None,
        })
    }
}

impl<T: PhysicalType> TryFrom<(Datatype, Vec<T>, Vec<u8>)> for QueryBuffers {
    type Error = TileDBError;

    fn try_from(
        data: (Datatype, Vec<T>, Vec<u8>),
    ) -> Result<Self, Self::Error> {
        let values = Buffer::try_from((data.0, data.1))?;
        let validity = Buffer::try_from((Datatype::UInt8, data.2))?;
        Ok(Self {
            data: values,
            offsets: None,
            validity: Some(validity),
        })
    }
}

impl<T: PhysicalType> TryFrom<(Datatype, Vec<T>, Vec<u64>, Vec<u8>)>
    for QueryBuffers
{
    type Error = TileDBError;

    fn try_from(
        data: (Datatype, Vec<T>, Vec<u64>, Vec<u8>),
    ) -> Result<Self, Self::Error> {
        let values = Buffer::try_from((data.0, data.1))?;
        let offsets = Buffer::try_from((Datatype::UInt64, data.2))?;
        let validity = Buffer::try_from((Datatype::UInt8, data.3))?;
        Ok(Self {
            data: values,
            offsets: Some(offsets),
            validity: Some(validity),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_only() {
        let buf = QueryBuffers::new(Datatype::Int32);
        assert_eq!(buf.data.capacity(), DEFAULT_SIZE);
        assert!(buf.offsets.is_none());
        assert!(buf.validity.is_none());
    }

    #[test]
    fn with_offsets() {
        let buf = QueryBuffers::new_with_offsets(Datatype::Int32);
        assert_eq!(buf.data.capacity(), DEFAULT_SIZE);
        assert!(buf.offsets.is_some());
        assert_eq!(buf.offsets.unwrap().capacity(), DEFAULT_SIZE);
        assert!(buf.validity.is_none());
    }

    #[test]
    fn with_validity() {
        let buf = QueryBuffers::new_with_validity(Datatype::Int32);
        assert_eq!(buf.data.capacity(), DEFAULT_SIZE);
        assert!(buf.offsets.is_none());
        assert!(buf.validity.is_some());
        assert_eq!(buf.validity.unwrap().capacity(), DEFAULT_SIZE);
    }

    #[test]
    fn with_offsets_and_validity() {
        let buf = QueryBuffers::new_with_offsets_and_validity(Datatype::Int32);
        assert_eq!(buf.data.capacity(), DEFAULT_SIZE);
        assert!(buf.offsets.is_some());
        assert_eq!(buf.offsets.unwrap().capacity(), DEFAULT_SIZE);
        assert!(buf.validity.is_some());
        assert_eq!(buf.validity.unwrap().capacity(), DEFAULT_SIZE);
    }

    #[test]
    fn custom_capacity() {
        let buf = QueryBuffers::with_capacity(Datatype::Int32, 100);
        assert_eq!(buf.data.capacity(), 400);
        assert!(buf.offsets.is_none());
        assert!(buf.validity.is_none());

        let buf = buf.with_offsets(100);
        assert!(buf.offsets.is_some());
        assert_eq!(buf.offsets.as_ref().unwrap().capacity(), 800);
        assert!(buf.validity.is_none());

        let buf = buf.with_validity(100);
        assert!(buf.offsets.is_some());
        assert!(buf.validity.is_some());
        assert_eq!(buf.validity.unwrap().capacity(), 100);
    }

    #[test]
    fn from_buffer() {
        let buffer = Buffer::new(Datatype::Int32);
        let buf = QueryBuffers::from(buffer);
        assert_eq!(buf.data.capacity(), 0);
        assert!(buf.offsets.is_none());
        assert!(buf.validity.is_none());
    }
}
