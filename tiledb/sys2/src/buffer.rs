use arrow::buffer::{
    MutableBuffer as ArrowMutableBuffer, ScalarBuffer as ArrowScalarBuffer,
};

use crate::datatype::{Datatype, DatatypeError, FFIDatatype};
use crate::types::{LogicalType, PhysicalType};

#[cxx::bridge(namespace = "tiledb::rs")]
pub(crate) mod ffi {
    extern "Rust" {
        pub(crate) type Buffer;

        fn as_mut_ptr(&mut self) -> *mut u8;
        fn as_ptr(&mut self) -> *const u8;
        fn len(&mut self) -> usize;
        fn resize(&mut self, elments: usize);
        fn resize_bytes(&mut self, bytes: usize);

        // Requires the buffer to be allocated with Datatype::Any and only
        // works one time per buffer. This is specifically for when the
        // C API doesn't have a way to get the datatype before passing a
        // Buffer in.
        fn cpp_init(&mut self, datatype: u8) -> bool;

        // ToDo: Not stopping for first query, but this should be FFIDatatype
        // or similar.
        fn cpp_is_compatible_type(&mut self, datatype: u8) -> bool;
    }
}

#[derive(Debug)]
pub struct Buffer {
    dtype: Datatype,
    buf: ArrowMutableBuffer,
}

impl Buffer {
    pub fn new(dtype: Datatype) -> Self {
        crate::logical_type_go!(dtype, LT, {
            Self {
                dtype,
                buf: ArrowMutableBuffer::from(Vec::<
                    <LT as LogicalType>::PhysicalType,
                >::new()),
            }
        })
    }

    pub fn with_capacity(dtype: Datatype, capacity: usize) -> Self {
        crate::logical_type_go!(dtype, LT, {
            let data = Vec::<<LT as LogicalType>::PhysicalType>::with_capacity(
                capacity,
            );
            Self {
                dtype,
                buf: ArrowMutableBuffer::from(data),
            }
        })
    }

    /// This is specifically for passing a buffer to the C API's that don't
    /// have a way for Rust to know what the Datatype is before the call. Use
    /// with caution. Nothing should panic if used incorrectly, although
    /// something will certainly return an error fairly quickly.
    pub fn uninit() -> Self {
        Self {
            dtype: Datatype::Any,
            buf: ArrowMutableBuffer::from(Vec::<u8>::new()),
        }
    }

    pub fn datatype(&self) -> Datatype {
        self.dtype
    }

    pub fn is_compatible(&self, dtype: Datatype) -> bool {
        self.dtype == dtype
    }

    pub fn buffer(&self) -> &ArrowMutableBuffer {
        &self.buf
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.buf.as_mut_ptr()
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.buf.as_ptr()
    }

    pub fn extend(&mut self, additional: usize) {
        self.buf.extend_zeros(additional * self.dtype.size())
    }

    pub fn extend_bytes_at_least(&mut self, additional: usize) {
        let mut count = additional / self.dtype.size();
        if additional % self.dtype.size() != 0 {
            count += 1;
        }

        let additional = count * self.dtype.size();
        self.buf.extend_zeros(additional);
    }

    pub fn from_vec<T: PhysicalType>(
        datatype: Datatype,
        data: Vec<T>,
    ) -> Result<Buffer, DatatypeError> {
        if !datatype.is_compatible_type::<T>() {
            return Err(DatatypeError::physical_type_incompatible::<T>(
                datatype,
            ));
        }

        Ok(Self {
            dtype: datatype,
            buf: ArrowMutableBuffer::from(data),
        })
    }

    pub fn into_vec<T: PhysicalType>(self) -> Result<Vec<T>, DatatypeError> {
        if !self.dtype.is_compatible_type::<T>() {
            return Err(DatatypeError::physical_type_incompatible::<T>(
                self.dtype,
            ));
        }

        let buf: ArrowScalarBuffer<T> = self.buf.into();
        Ok(buf.into())
    }

    pub fn as_slice<T: PhysicalType>(&self) -> Result<&[T], DatatypeError> {
        if !self.dtype.is_compatible_type::<T>() {
            return Err(DatatypeError::physical_type_incompatible::<T>(
                self.dtype,
            ));
        }

        Ok(self.buf.typed_data::<T>())
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn resize(&mut self, elements: usize) {
        let bytes = self.dtype.size() * elements;
        self.buf.resize(bytes, 0)
    }

    // TODO: For the moment, we're trusting that core will never set a byte
    // count that is not a multiple of the datatype which is way less safe
    // than I like.
    //
    // When I find time, I need to go back and add checks to the conversion
    // traits so that we don't cause panics if core ever messes up there.
    pub fn resize_bytes(&mut self, bytes: usize) {
        self.buf.resize(bytes, 0);
    }

    // Helpers for safety from the C++ wrapper layer. Everything below here is
    // meant to only be called from the C++ wrapperes. Hopefully I can enforce
    // that with keeping these private.

    fn cpp_init(&mut self, dtype: u8) -> bool {
        if self.dtype != Datatype::Any {
            return false;
        }

        let dtype = FFIDatatype { repr: dtype };
        if let Ok(dtype) = Datatype::try_from(dtype) {
            self.dtype = dtype;
            crate::logical_type_go!(dtype, LT, {
                self.buf = ArrowMutableBuffer::from(Vec::<
                    <LT as LogicalType>::PhysicalType,
                >::new());
            });
            true
        } else {
            false
        }
    }

    fn cpp_is_compatible_type(&mut self, dtype: u8) -> bool {
        let dtype = FFIDatatype { repr: dtype };
        if let Ok(dtype) = Datatype::try_from(dtype) {
            dtype == self.dtype
        } else {
            false
        }
    }
}

unsafe impl cxx::ExternType for Buffer {
    type Id = cxx::type_id!("tiledb::rs::Buffer");
    type Kind = cxx::kind::Opaque;
}

// Note, for now there's no inverse of this translation. I originally wrote
// an implementation of `From<(Datatype, ArrowMutableBuffer)>` but after
// further reflection I realized that's not sound without me learning and
// writing the alignment checks required for safety. So for now, users will
// have to convert to a ScalarBuffer on their own and then use that for us
// the check Datatype compatibility with the generic type.

impl From<Buffer> for ArrowMutableBuffer {
    fn from(buf: Buffer) -> Self {
        buf.buf
    }
}

impl<T: PhysicalType> TryFrom<Buffer> for ArrowScalarBuffer<T> {
    type Error = DatatypeError;

    fn try_from(buf: Buffer) -> Result<Self, Self::Error> {
        if !buf.dtype.is_compatible_type::<T>() {
            return Err(DatatypeError::physical_type_incompatible::<T>(
                buf.dtype,
            ));
        }

        Ok(buf.buf.into())
    }
}

impl<T: PhysicalType> TryFrom<Buffer> for Vec<T> {
    type Error = DatatypeError;

    fn try_from(buf: Buffer) -> Result<Self, Self::Error> {
        if !buf.dtype.is_compatible_type::<T>() {
            return Err(DatatypeError::physical_type_incompatible::<T>(
                buf.dtype,
            ));
        }

        buf.into_vec::<T>()
    }
}

impl<T: PhysicalType> TryFrom<(Datatype, Vec<T>)> for Buffer {
    type Error = DatatypeError;

    fn try_from(pair: (Datatype, Vec<T>)) -> Result<Self, Self::Error> {
        if !pair.0.is_compatible_type::<T>() {
            return Err(DatatypeError::physical_type_incompatible::<T>(pair.0));
        }

        Ok(Self {
            dtype: pair.0,
            buf: pair.1.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_u8_to_u64() -> Result<(), DatatypeError> {
        let mut buf = Buffer::new(Datatype::UInt8);
        buf.resize(1023);
        let _: Vec<u64> = buf.try_into()?;

        Ok(())
    }
}
