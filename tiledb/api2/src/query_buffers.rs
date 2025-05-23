use tiledb_sys2::buffer::Buffer;
use tiledb_sys2::datatype::Datatype;

pub struct QueryBuffers {
    pub data: Buffer,
    pub offsets: Option<Buffer>,
    pub validity: Option<Buffer>,
}

impl QueryBuffers {
    /// Create a new data-only QueryBuffers instance.
    ///
    /// This defaults to allocating 1MiB of whatever datatype is provided.
    pub fn new(datatype: Datatype) -> Self {
        let capacity = 1024 * 1024 / datatype.size();
        Self {
            data: Buffer::with_capacity(datatype, capacity),
            offsets: None,
            validity: None,
        }
    }

    pub fn new_with_offsets(datatype: Datatype) -> Self {
        let data_capacity = 1024 * 1024 / datatype.size();
        let offsets_capacity = 1024 * 1024 / 8;
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
        let data_capacity = 1024 * 1024 / datatype.size();
        let validity_capacity = 1024 * 1024;
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
        let data_capacity = 1024 * 1024 / datatype.size();
        let offsets_capacity = 1024 * 1024 / 8;
        let validity_capacity = 1024 * 1024;
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
