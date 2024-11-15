use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array as aa;
use arrow::buffer::{
    self as abuf, Buffer as ArrowBuffer, MutableBuffer as ArrowBufferMut,
};
use arrow::datatypes as adt;
use arrow::error::ArrowError;
use thiserror::Error;
use tiledb_api::array::schema::{Field, Schema};
use tiledb_api::error::Error as TileDBError;
use tiledb_common::array::CellValNum;

use super::arrow::ToArrowConverter;
use super::fields::{QueryField, QueryFields};

const AVERAGE_STRING_LENGTH: usize = 64;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Provided Arrow Array is externally referenced.")]
    ArrayInUse,
    #[error("Error converting to Arrow for field '{0}': {1}")]
    ArrowConversionError(String, super::arrow::Error),
    #[error("Failed to convert Arrow Array for field '{0}': {1}")]
    FailedConversionFromArrow(String, Box<Error>),
    #[error("Failed to allocate Arrow array: {0}")]
    ArrayCreationFailed(ArrowError),
    #[error("Capacity {0} is to small to hold {1} bytes per cell.")]
    CapacityTooSmall(usize, usize),
    #[error("Failed to convert owned buffers into a list array: {0}")]
    FailedListArrayCreation(ArrowError),
    #[error("Buffer is immutable")]
    ImmutableBuffer,
    #[error("Internal binary type mismatch error")]
    InternalBinaryType,
    #[error("Invalid buffer, no offsets present.")]
    InvalidBufferNoOffsets,
    #[error("Invalid buffer, no validity present.")]
    InvalidBufferNoValidity,
    #[error("Invalid data type for bytes array: {0}")]
    InvalidBytesType(adt::DataType),
    #[error("Invalid fixed sized list length {0} is less than 2")]
    InvalidFixedSizeListLength(i32),
    #[error("TileDB does not support nullable list elements")]
    InvalidNullableListElements,
    #[error("Invalid data type for primitive data: {0}")]
    InvalidPrimitiveType(adt::DataType),
    #[error("Internal error: Converted primitive array is not scalar")]
    InternalListTypeMismatch,
    #[error("Internal string type mismatch error")]
    InternalStringType,
    #[error("Error converting var sized buffers to arrow: {0}")]
    InvalidVarBuffers(ArrowError),
    #[error("Only the large variant is supported: {0}")]
    LargeVariantOnly(adt::DataType),
    #[error("Failed to convert internal list array: {0}")]
    ListSubarrayConversion(Box<Error>),
    #[error("Provided array had external references to its offsets buffer.")]
    OffsetsInUse,
    #[error("Internal TileDB Error: {0}")]
    TileDB(#[from] TileDBError),
    #[error("Unexpected var sized arrow type: {0}")]
    UnexpectedArrowVarType(adt::DataType),
    #[error("Mutable buffers are not shareable.")]
    UnshareableMutableBuffer,
    #[error("Unsupported arrow array type: {0}")]
    UnsupportedArrowType(adt::DataType),
    #[error(
        "TileDB only supports fixed size lists of primtiive types, not {0}"
    )]
    UnsupportedFixedSizeListType(adt::DataType),
    #[error("TileDB does not support timezones")]
    UnsupportedTimeZones,
}

type Result<T> = std::result::Result<T, Error>;

// The Arrow downcast_array function doesn't take an Arc which leaves us
// with an outstanding reference when we attempt the Buffer::into_mutable
// call. This function exists to consume the Arc after the cast.
fn downcast_consume<T>(array: Arc<dyn aa::Array>) -> T
where
    T: From<aa::ArrayData>,
{
    aa::downcast_array(&array)
}

/// The return type for the NewBufferTraitThing's into_arrow method. This
/// allows for fallible conversion without dropping the underlying buffers.
type IntoArrowResult = std::result::Result<
    Arc<dyn aa::Array>,
    (Box<dyn NewBufferTraitThing>, Error),
>;

/// The error type to use on TryFrom<Arc<dyn aa::Arrrow>> implementations
type FromArrowError = (Arc<dyn aa::Array>, Error);

/// The return type to use when implementing TryFrom<Arc<dyn aa::Array>
type FromArrowResult<T> = std::result::Result<T, FromArrowError>;

/// An interface to our mutable buffer implementations.
trait NewBufferTraitThing {
    /// Return this trait object as any for downcasting.
    fn as_any(&self) -> &dyn Any;

    /// The length of the buffer, in cells
    fn len(&self) -> usize;

    /// The data buffer
    fn data(&mut self) -> &mut QueryBuffer;

    /// The offsets buffer, for variants that have one
    fn offsets(&mut self) -> Option<&mut QueryBuffer>;

    /// The validity buffer, when present
    fn validity(&mut self) -> Option<&mut QueryBuffer>;

    /// Check if another buffer is compatible with this buffer
    fn is_compatible(&self, other: &Box<dyn NewBufferTraitThing>) -> bool;

    /// Consume self and return an Arc<dyn aa::Array>
    fn into_arrow(self: Box<Self>) -> IntoArrowResult;

    /// Reset all buffer lengths to match capacity.
    ///
    /// This should happen before a read query so that we're sure to be using
    /// the entire available buffer rather than just whatever fit in the
    /// previous iteration.
    fn reset_len(&mut self) {
        self.data().reset();
        if let Some(offsets) = self.offsets() {
            offsets.reset();
        }
        if let Some(validity) = self.validity() {
            validity.reset();
        }
    }

    /// Shrink len to data
    ///
    /// After a read query, this method is used to update the length of all
    /// buffers to match the number of bytes written by TileDB.
    fn shrink_len(&mut self) {
        self.data().resize();
        if let Some(offsets) = self.offsets() {
            offsets.resize();
        }
        if let Some(validity) = self.validity() {
            validity.resize();
        }
    }

    /// Returns a mutable pointer to the data buffer
    fn data_ptr(&mut self) -> *mut std::ffi::c_void {
        self.data().buffer.as_mut_ptr() as *mut std::ffi::c_void
    }

    /// Returns a mutable pointer to the data size
    fn data_size_ptr(&mut self) -> *mut u64 {
        self.data().size.as_mut().get_mut()
    }

    /// Returns a mutable poiniter to the offsets buffer.
    ///
    /// For variants that don't have offsets, it returns a null pointer.
    fn offsets_ptr(&mut self) -> *mut u64 {
        let Some(offsets) = self.offsets() else {
            return std::ptr::null_mut();
        };

        offsets.buffer.as_mut_ptr() as *mut u64
    }

    /// Returns a mutable pointer to the offsets size.
    ///
    /// For variants that don't have offsets, it returns a null pointer.
    ///
    /// FIXME: why does this not return `Option`
    fn offsets_size_ptr(&mut self) -> *mut u64 {
        let Some(offsets) = self.offsets() else {
            return std::ptr::null_mut();
        };

        offsets.size.as_mut().get_mut()
    }

    /// Returns a mutable pointer to the validity buffer, when present
    ///
    /// When validity is not present, it returns a null pointer.
    fn validity_ptr(&mut self) -> *mut u8 {
        let Some(validity) = self.validity() else {
            return std::ptr::null_mut();
        };

        validity.buffer.as_mut_ptr()
    }

    /// Returns a mutable pointer to the validity size, when present
    ///
    /// When validity is not present, it returns a null pointer.
    fn validity_size_ptr(&mut self) -> *mut u64 {
        let Some(validity) = self.validity() else {
            return std::ptr::null_mut();
        };

        validity.size.as_mut().get_mut()
    }
}

struct BooleanBuffers {
    data: QueryBuffer,
    validity: Option<QueryBuffer>,
}

impl TryFrom<Arc<dyn aa::Array>> for BooleanBuffers {
    type Error = FromArrowError;
    fn try_from(array: Arc<dyn aa::Array>) -> FromArrowResult<Self> {
        let array: aa::BooleanArray = downcast_consume(array);
        let (data, validity) = array.into_parts();
        let data = data
            .iter()
            .map(|v| if v { 1u8 } else { 0 })
            .collect::<Vec<_>>();
        let validity = to_tdb_validity(validity);

        Ok(BooleanBuffers {
            data: QueryBuffer::new(abuf::MutableBuffer::from(data)),
            validity: validity
                .map(|v| QueryBuffer::new(abuf::MutableBuffer::from(v))),
        })
    }
}

impl NewBufferTraitThing for BooleanBuffers {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn len(&self) -> usize {
        self.data.buffer.len()
    }

    fn data(&mut self) -> &mut QueryBuffer {
        &mut self.data
    }

    fn offsets(&mut self) -> Option<&mut QueryBuffer> {
        None
    }

    fn validity(&mut self) -> Option<&mut QueryBuffer> {
        self.validity.as_mut()
    }

    fn is_compatible(&self, other: &Box<dyn NewBufferTraitThing>) -> bool {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return false;
        };

        self.validity.is_some() == other.validity.is_some()
    }

    fn into_arrow(self: Box<Self>) -> IntoArrowResult {
        let data = abuf::BooleanBuffer::from_iter(
            self.data.buffer.iter().map(|b| *b != 0),
        );
        Ok(Arc::new(aa::BooleanArray::new(
            data,
            from_tdb_validity(&self.validity),
        )))
    }
}

struct ByteBuffers {
    dtype: adt::DataType,
    data: QueryBuffer,
    offsets: QueryBuffer,
    validity: Option<QueryBuffer>,
}

macro_rules! to_byte_buffers {
    ($ARRAY:expr, $ARROW_TYPE:expr, $ARROW_DT:ty) => {{
        let array: $ARROW_DT = downcast_consume($ARRAY);
        let (offsets, data, nulls) = array.into_parts();

        let data = data.into_mutable();
        let offsets = offsets.into_inner().into_inner().into_mutable();

        if data.is_ok() && offsets.is_ok() {
            let data = QueryBuffer::new(data.ok().unwrap());
            let offsets = QueryBuffer::new(offsets.ok().unwrap());
            let validity = to_tdb_validity(nulls).map(QueryBuffer::new);
            return Ok(ByteBuffers {
                dtype: $ARROW_TYPE,
                data,
                offsets,
                validity,
            });
        }

        let data = if data.is_ok() {
            ArrowBuffer::from(data.ok().unwrap())
        } else {
            data.err().unwrap()
        };

        let offsets = if offsets.is_ok() {
            offsets
                .map(abuf::ScalarBuffer::<i64>::from)
                .map(abuf::OffsetBuffer::new)
                .ok()
                .unwrap()
        } else {
            offsets
                .map_err(abuf::ScalarBuffer::<i64>::from)
                .map_err(abuf::OffsetBuffer::new)
                .err()
                .unwrap()
        };

        let array: Arc<dyn aa::Array> = Arc::new(
            aa::LargeBinaryArray::try_new(offsets, data.into(), nulls).unwrap(),
        );
        Err((array, Error::OffsetsInUse))
    }};
}

impl TryFrom<Arc<dyn aa::Array>> for ByteBuffers {
    type Error = FromArrowError;

    fn try_from(array: Arc<dyn aa::Array>) -> FromArrowResult<Self> {
        let dtype = array.data_type().clone();
        match dtype {
            adt::DataType::LargeBinary => {
                to_byte_buffers!(array, dtype.clone(), aa::LargeBinaryArray)
            }
            adt::DataType::LargeUtf8 => {
                to_byte_buffers!(array, dtype.clone(), aa::LargeStringArray)
            }
            t => Err((array, Error::InvalidBytesType(t))),
        }
    }
}

impl NewBufferTraitThing for ByteBuffers {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn len(&self) -> usize {
        (self.offsets.buffer.len() / std::mem::size_of::<i64>()) - 1
    }

    fn data(&mut self) -> &mut QueryBuffer {
        &mut self.data
    }

    fn offsets(&mut self) -> Option<&mut QueryBuffer> {
        Some(&mut self.offsets)
    }

    fn validity(&mut self) -> Option<&mut QueryBuffer> {
        self.validity.as_mut()
    }

    fn is_compatible(&self, other: &Box<dyn NewBufferTraitThing>) -> bool {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return false;
        };

        if self.dtype != other.dtype {
            return false;
        }

        self.validity.is_some() == other.validity.is_some()
    }

    fn into_arrow(self: Box<Self>) -> IntoArrowResult {
        // NB: by default the offsets are not arrow-shaped.
        // However we use the configuration options to make them so.

        let dtype = self.dtype;
        let data = ArrowBuffer::from(self.data.buffer);
        let offsets = ArrowBuffer::from(self.offsets.buffer);
        let validity = from_tdb_validity(&self.validity);

        // N.B., the calls to cloning the data/offsets/validity are as cheap
        // as an Arc::clone plus pointer and usize copy. They are *not* cloning
        // the underlying allocated data.
        match aa::ArrayData::try_new(
            dtype.clone(),
            (*self.offsets.size as usize / std::mem::size_of::<i64>()) - 1,
            validity.clone().map(|v| v.into_inner().into_inner()),
            0,
            vec![offsets.clone(), data.clone()],
            vec![],
        )
        .map(|data| aa::make_array(data))
        {
            Ok(arrow) => Ok(arrow),
            Err(e) => {
                // SAFETY: These unwraps are fine because the only other reference
                // was consumed in the failed try_new call. Unless of course
                // the ArrowError `e` ever ends up carrying a reference.
                let boxed: Box<dyn NewBufferTraitThing> =
                    Box::new(ByteBuffers {
                        dtype,
                        data: QueryBuffer {
                            buffer: data.into_mutable().unwrap(),
                            size: self.data.size,
                        },
                        offsets: QueryBuffer {
                            buffer: offsets.into_mutable().unwrap(),
                            size: self.offsets.size,
                        },
                        validity: self.validity,
                    });

                Err((boxed, Error::ArrayCreationFailed(e)))
            }
        }
    }
}

struct FixedListBuffers {
    field: Arc<adt::Field>,
    cell_val_num: CellValNum,
    data: QueryBuffer,
    validity: Option<QueryBuffer>,
}

impl TryFrom<Arc<dyn aa::Array>> for FixedListBuffers {
    type Error = FromArrowError;

    fn try_from(array: Arc<dyn aa::Array>) -> FromArrowResult<Self> {
        assert!(matches!(
            array.data_type(),
            adt::DataType::FixedSizeList(_, _)
        ));

        let array: aa::FixedSizeListArray = downcast_consume(array);
        let (field, cvn, array, nulls) = array.into_parts();

        if field.is_nullable() {
            return Err((array, Error::InvalidNullableListElements));
        }

        if cvn < 2 {
            return Err((array, Error::InvalidFixedSizeListLength(cvn)));
        }

        // SAFETY: We just showed cvn >= 2 && cvn is i32 whicih means
        // it can't be u32::MAX
        let cvn = CellValNum::try_from(cvn as u32)
            .expect("Internal cell val num error");

        let dtype = field.data_type().clone();
        if !dtype.is_primitive() {
            return Err((array, Error::UnsupportedFixedSizeListType(dtype)));
        }

        PrimitiveBuffers::try_from(array)
            .map(|buffers| {
                assert_eq!(buffers.dtype, dtype);
                let validity =
                    to_tdb_validity(nulls.clone()).map(QueryBuffer::new);
                FixedListBuffers {
                    field: Arc::clone(&field),
                    cell_val_num: cvn,
                    data: buffers.data,
                    validity,
                }
            })
            .map_err(|(array, e)| {
                let array: Arc<dyn aa::Array> = Arc::new(
                    aa::FixedSizeListArray::try_new(
                        field,
                        u32::from(cvn) as i32,
                        array,
                        nulls,
                    )
                    .unwrap(),
                );
                (array, e)
            })
    }
}

impl NewBufferTraitThing for FixedListBuffers {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn len(&self) -> usize {
        self.data.buffer.len() / u32::from(self.cell_val_num) as usize
    }

    fn data(&mut self) -> &mut QueryBuffer {
        &mut self.data
    }

    fn offsets(&mut self) -> Option<&mut QueryBuffer> {
        None
    }

    fn validity(&mut self) -> Option<&mut QueryBuffer> {
        self.validity.as_mut()
    }

    fn is_compatible(&self, other: &Box<dyn NewBufferTraitThing>) -> bool {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return false;
        };

        if self.field != other.field {
            return false;
        }

        if self.cell_val_num != other.cell_val_num {
            return false;
        }

        self.validity.is_some() == other.validity.is_some()
    }

    fn into_arrow(self: Box<Self>) -> IntoArrowResult {
        let field = self.field;
        let cell_val_num = self.cell_val_num;
        let data = ArrowBuffer::from(self.data.buffer);
        let validity = from_tdb_validity(&self.validity);

        assert!(field.data_type().is_primitive());
        let num_values = *self.data.size as usize
            / field.data_type().primitive_width().unwrap();
        let cvn = u32::from(cell_val_num) as i32;
        let len = num_values / cvn as usize;

        // N.B., data/validity clones are cheap. They are not cloning the
        // underlying data buffers. We have to clone so that we can put ourself
        // back together if the array conversion failes.
        match aa::ArrayData::try_new(
            field.data_type().clone(),
            len,
            validity.clone().map(|v| v.into_inner().into_inner()),
            0,
            vec![data.clone().into()],
            vec![],
        )
        .map(|data| {
            let array: Arc<dyn aa::Array> =
                Arc::new(aa::FixedSizeListArray::new(
                    Arc::clone(&field),
                    u32::from(cell_val_num) as i32,
                    aa::make_array(data),
                    validity.clone(),
                ));
            array
        }) {
            Ok(arrow) => Ok(arrow),
            Err(e) => {
                let boxed: Box<dyn NewBufferTraitThing> =
                    Box::new(FixedListBuffers {
                        field,
                        cell_val_num,
                        data: QueryBuffer {
                            buffer: data.into_mutable().unwrap(),
                            size: self.data.size,
                        },
                        validity: self.validity,
                    });

                Err((boxed, Error::ArrayCreationFailed(e)))
            }
        }
    }
}

struct QueryBuffer {
    buffer: ArrowBufferMut,
    size: Pin<Box<u64>>,
}

impl QueryBuffer {
    pub fn new(buffer: ArrowBufferMut) -> Self {
        let size = Box::pin(buffer.len() as u64);
        Self { buffer, size }
    }

    pub fn reset(&mut self) {
        *self.size = self.buffer.capacity() as u64;
        self.resize()
    }

    pub fn resize(&mut self) {
        self.buffer.resize(*self.size as usize, 0);
    }
}

struct ListBuffers {
    field: Arc<adt::Field>,
    data: QueryBuffer,
    offsets: QueryBuffer,
    validity: Option<QueryBuffer>,
}

impl TryFrom<Arc<dyn aa::Array>> for ListBuffers {
    type Error = FromArrowError;

    fn try_from(array: Arc<dyn aa::Array>) -> FromArrowResult<Self> {
        assert!(matches!(array.data_type(), adt::DataType::LargeList(_)));

        let array: aa::LargeListArray = downcast_consume(array);
        let (field, offsets, array, nulls) = array.into_parts();

        if field.is_nullable() {
            return Err((array, Error::InvalidNullableListElements));
        }

        let dtype = field.data_type().clone();
        if !dtype.is_primitive() {
            return Err((array, Error::UnsupportedFixedSizeListType(dtype)));
        }

        // N.B., I really, really tried to make this a fancy map/map_err
        // cascade like all of the others. But it turns out that keeping the
        // proper refcounts on either array or offsets turns into a bit of
        // an issue when passing things through multiple closures.
        let result = PrimitiveBuffers::try_from(array);
        if result.is_err() {
            let (array, err) = result.err().unwrap();
            let array: Arc<dyn aa::Array> = Arc::new(
                aa::LargeListArray::try_new(
                    Arc::clone(&field),
                    offsets,
                    array,
                    nulls.clone(),
                )
                .unwrap(),
            );
            return Err((array, err));
        }

        let data = result.ok().unwrap();

        let offsets = match offsets.into_inner().into_inner().into_mutable() {
            Ok(offsets) => offsets,
            Err(e) => {
                let offsets_buffer = e;
                let offsets = abuf::OffsetBuffer::new(
                    abuf::ScalarBuffer::<i64>::from(offsets_buffer),
                );
                let array: Arc<dyn aa::Array> = Arc::new(
                    aa::LargeListArray::try_new(
                        Arc::clone(&field),
                        offsets,
                        // Safety: We just turned this into a mutable buffer, so
                        // the inversion should never fail.
                        Box::new(data).into_arrow().ok().unwrap(),
                        nulls.clone(),
                    )
                    .unwrap(),
                );
                return Err((array, Error::ArrayInUse));
            }
        };

        // NB: by default the offsets are not arrow-shaped.
        // However we use the configuration options to make them so.

        let validity = to_tdb_validity(nulls).map(QueryBuffer::new);

        Ok(ListBuffers {
            field,
            data: data.data,
            offsets: QueryBuffer::new(offsets),
            validity,
        })
    }
}

impl NewBufferTraitThing for ListBuffers {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn len(&self) -> usize {
        (self.offsets.buffer.len() / std::mem::size_of::<i64>()) - 1
    }

    fn data(&mut self) -> &mut QueryBuffer {
        &mut self.data
    }

    fn offsets(&mut self) -> Option<&mut QueryBuffer> {
        Some(&mut self.offsets)
    }

    fn validity(&mut self) -> Option<&mut QueryBuffer> {
        self.validity.as_mut()
    }

    fn is_compatible(&self, other: &Box<dyn NewBufferTraitThing>) -> bool {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return false;
        };

        if self.field != other.field {
            return false;
        }

        self.validity.is_some() == other.validity.is_some()
    }

    fn into_arrow(self: Box<Self>) -> IntoArrowResult {
        let field = self.field;

        assert!(field.data_type().is_primitive());

        // NB: by default the offsets are not arrow-shaped.
        // However we use the configuration options to make them so.

        let data = ArrowBuffer::from(self.data.buffer);
        let offsets = from_tdb_offsets(self.offsets.buffer);
        let validity = from_tdb_validity(&self.validity);

        // N.B., the calls to cloning the data/offsets/validity are as cheap
        // as an Arc::clone plus pointer and usize copy. They are *not* cloning
        // the underlying allocated data.
        match aa::ArrayData::try_new(
            field.data_type().clone(),
            (*self.offsets.size as usize / std::mem::size_of::<i64>()) - 1,
            None,
            0,
            vec![data.clone().into()],
            vec![],
        )
        .and_then(|data| {
            aa::LargeListArray::try_new(
                field.clone(),
                offsets.clone(),
                aa::make_array(data),
                validity.clone(),
            )
        })
        .map(|array| {
            let array: Arc<dyn aa::Array> = Arc::new(array);
            array
        }) {
            Ok(arrow) => Ok(arrow),
            Err(e) => {
                let boxed: Box<dyn NewBufferTraitThing> =
                    Box::new(ListBuffers {
                        field,
                        data: QueryBuffer {
                            buffer: data.into_mutable().unwrap(),
                            size: self.data.size,
                        },
                        offsets: QueryBuffer {
                            buffer: to_tdb_offsets(offsets).unwrap(),
                            size: self.offsets.size,
                        },
                        validity: self.validity,
                    });

                Err((boxed, Error::ArrayCreationFailed(e)))
            }
        }
    }
}

struct PrimitiveBuffers {
    dtype: adt::DataType,
    data: QueryBuffer,
    validity: Option<QueryBuffer>,
}

macro_rules! to_primitive {
    ($ARRAY:expr, $ARROW_DT:ty) => {{
        let array: $ARROW_DT = downcast_consume($ARRAY);
        let len = array.len();
        let (dtype, buffer, nulls) = array.into_parts();

        buffer
            .into_inner()
            .into_mutable()
            .map(|data| {
                let validity =
                    to_tdb_validity(nulls.clone()).map(QueryBuffer::new);
                PrimitiveBuffers {
                    dtype: dtype.clone(),
                    data: QueryBuffer::new(data),
                    validity,
                }
            })
            .map_err(|buffer| {
                // Safety: We just broke an array open to get these so
                // unless someone did something unsafe they should go
                // right back together again. Sorry, Humpty.
                let data = aa::ArrayData::try_new(
                    dtype,
                    len,
                    nulls.map(|n| n.into_inner().into_inner()),
                    0,
                    vec![buffer],
                    vec![],
                )
                .unwrap();
                (aa::make_array(data), Error::ArrayInUse)
            })
    }};
}

impl TryFrom<Arc<dyn aa::Array>> for PrimitiveBuffers {
    type Error = FromArrowError;
    fn try_from(array: Arc<dyn aa::Array>) -> FromArrowResult<Self> {
        assert!(array.data_type().is_primitive());

        match array.data_type().clone() {
            adt::DataType::Int8 => to_primitive!(array, aa::Int8Array),
            adt::DataType::Int16 => to_primitive!(array, aa::Int16Array),
            adt::DataType::Int32 => to_primitive!(array, aa::Int32Array),
            adt::DataType::Int64 => to_primitive!(array, aa::Int64Array),
            adt::DataType::UInt8 => to_primitive!(array, aa::UInt8Array),
            adt::DataType::UInt16 => to_primitive!(array, aa::UInt16Array),
            adt::DataType::UInt32 => to_primitive!(array, aa::UInt32Array),
            adt::DataType::UInt64 => to_primitive!(array, aa::UInt64Array),
            adt::DataType::Float32 => to_primitive!(array, aa::Float32Array),
            adt::DataType::Float64 => to_primitive!(array, aa::Float64Array),
            t => Err((array, Error::InvalidPrimitiveType(t))),
        }
    }
}

impl NewBufferTraitThing for PrimitiveBuffers {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn len(&self) -> usize {
        assert!(self.dtype.is_primitive());
        self.data.buffer.len() / self.dtype.primitive_width().unwrap()
    }

    fn data(&mut self) -> &mut QueryBuffer {
        &mut self.data
    }

    fn offsets(&mut self) -> Option<&mut QueryBuffer> {
        None
    }

    fn validity(&mut self) -> Option<&mut QueryBuffer> {
        self.validity.as_mut()
    }

    fn is_compatible(&self, other: &Box<dyn NewBufferTraitThing>) -> bool {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return false;
        };

        if self.dtype != other.dtype {
            return false;
        }

        self.validity.is_some() == other.validity.is_some()
    }

    fn into_arrow(self: Box<Self>) -> IntoArrowResult {
        let data = ArrowBuffer::from(self.data.buffer);
        let validity = from_tdb_validity(&self.validity);

        assert!(self.dtype.is_primitive());

        // N.B., data/validity clones are cheap. They are not cloning the
        // underlying data buffers. We have to clone so that we can put ourself
        // back together if the array conversion failes.
        match aa::ArrayData::try_new(
            self.dtype.clone(),
            *self.data.size as usize / self.dtype.primitive_width().unwrap(),
            validity.clone().map(|v| v.into_inner().into_inner()),
            0,
            vec![data.clone()],
            vec![],
        )
        .map(aa::make_array)
        {
            Ok(arrow) => Ok(arrow),
            Err(e) => {
                let boxed: Box<dyn NewBufferTraitThing> =
                    Box::new(PrimitiveBuffers {
                        dtype: self.dtype,
                        data: QueryBuffer {
                            buffer: data.into_mutable().unwrap(),
                            size: self.data.size,
                        },
                        validity: self.validity,
                    });

                Err((boxed, Error::ArrayCreationFailed(e)))
            }
        }
    }
}

impl TryFrom<Arc<dyn aa::Array>> for Box<dyn NewBufferTraitThing> {
    type Error = FromArrowError;

    fn try_from(array: Arc<dyn aa::Array>) -> FromArrowResult<Self> {
        let dtype = array.data_type().clone();
        match dtype {
            adt::DataType::Boolean => {
                BooleanBuffers::try_from(array).map(|buffers| {
                    let boxed: Box<dyn NewBufferTraitThing> = Box::new(buffers);
                    boxed
                })
            }
            adt::DataType::LargeBinary | adt::DataType::LargeUtf8 => {
                ByteBuffers::try_from(array).map(|buffers| {
                    let boxed: Box<dyn NewBufferTraitThing> = Box::new(buffers);
                    boxed
                })
            }
            adt::DataType::FixedSizeList(_, _) => {
                FixedListBuffers::try_from(array).map(|buffers| {
                    let boxed: Box<dyn NewBufferTraitThing> = Box::new(buffers);
                    boxed
                })
            }
            adt::DataType::LargeList(_) => {
                ListBuffers::try_from(array).map(|buffers| {
                    let boxed: Box<dyn NewBufferTraitThing> = Box::new(buffers);
                    boxed
                })
            }
            adt::DataType::Int8
            | adt::DataType::Int16
            | adt::DataType::Int32
            | adt::DataType::Int64
            | adt::DataType::UInt8
            | adt::DataType::UInt16
            | adt::DataType::UInt32
            | adt::DataType::UInt64
            | adt::DataType::Float32
            | adt::DataType::Float64
            | adt::DataType::Timestamp(_, None)
            | adt::DataType::Time64(_) => PrimitiveBuffers::try_from(array)
                .map(|buffers| {
                    let boxed: Box<dyn NewBufferTraitThing> = Box::new(buffers);
                    boxed
                }),

            adt::DataType::Timestamp(_, Some(_)) => {
                Err((array, Error::UnsupportedTimeZones))
            }

            adt::DataType::Binary
            | adt::DataType::List(_)
            | adt::DataType::Utf8 => {
                Err((array, Error::LargeVariantOnly(dtype)))
            }

            adt::DataType::FixedSizeBinary(_) => {
                todo!("This can probably be supported.")
            }

            adt::DataType::Null
            | adt::DataType::Float16
            | adt::DataType::Date32
            | adt::DataType::Date64
            | adt::DataType::Time32(_)
            | adt::DataType::Duration(_)
            | adt::DataType::Interval(_)
            | adt::DataType::BinaryView
            | adt::DataType::Utf8View
            | adt::DataType::ListView(_)
            | adt::DataType::LargeListView(_)
            | adt::DataType::Struct(_)
            | adt::DataType::Union(_, _)
            | adt::DataType::Dictionary(_, _)
            | adt::DataType::Decimal128(_, _)
            | adt::DataType::Decimal256(_, _)
            | adt::DataType::Map(_, _)
            | adt::DataType::RunEndEncoded(_, _) => {
                return Err((array, Error::UnsupportedArrowType(dtype)));
            }
        }
    }
}

/// A utility that requires it contains exactly one of two variants
///
/// Unfortuantely, mutating enum variants through a mutable reference isn't
/// a thing that can be done safely, so we have a specialized utility struct
/// that does the same idea, at the cost of an extra None in the struct.
struct MutableOrShared {
    mutable: Option<Box<dyn NewBufferTraitThing>>,
    shared: Option<Arc<dyn aa::Array>>,
}

impl MutableOrShared {
    pub fn new(value: Arc<dyn aa::Array>) -> Self {
        Self {
            mutable: None,
            shared: Some(value),
        }
    }

    pub fn mutable(&mut self) -> Option<&mut Box<dyn NewBufferTraitThing>> {
        self.mutable.as_mut()
    }

    pub fn shared(&self) -> Option<Arc<dyn aa::Array>> {
        self.shared.as_ref().map(Arc::clone)
    }

    pub fn to_mutable(&mut self) -> Result<()> {
        self.validate();

        if self.mutable.is_some() {
            return Ok(());
        }

        let shared = self.shared.take().unwrap();
        let mutable: FromArrowResult<Box<dyn NewBufferTraitThing>> =
            shared.try_into();

        let ret = if mutable.is_ok() {
            self.mutable = mutable.ok();
            Ok(())
        } else {
            let (array, err) = mutable.err().unwrap();
            self.shared = Some(array);
            Err(err)
        };

        self.validate();
        ret
    }

    pub fn to_shared(&mut self) -> Result<()> {
        self.validate();

        if self.shared.is_some() {
            return Ok(());
        }

        let mutable = self.mutable.take().unwrap();
        let shared = mutable.into_arrow();

        let ret = if shared.is_ok() {
            self.shared = shared.ok();
            Ok(())
        } else {
            let (mutable, err) = shared.err().unwrap();
            self.mutable = Some(mutable);
            Err(err)
        };

        self.validate();
        ret
    }

    fn validate(&self) {
        assert!(
            (self.shared.is_some() && self.mutable.is_none())
                || (self.shared.is_none() && self.mutable.is_some())
        )
    }
}

pub struct BufferEntry {
    entry: MutableOrShared,
}

impl BufferEntry {
    pub fn as_shared(&self) -> Result<Arc<dyn aa::Array>> {
        let Some(ref array) = self.entry.shared() else {
            return Err(Error::UnshareableMutableBuffer);
        };

        return Ok(Arc::clone(array));
    }

    pub fn len(&self) -> usize {
        if self.entry.shared.is_some() {
            return self.entry.shared.as_ref().unwrap().len();
        } else {
            self.entry.mutable.as_ref().unwrap().len()
        }
    }

    pub fn is_compatible(&self, other: &BufferEntry) -> bool {
        if self.entry.mutable.is_some() && other.entry.mutable.is_some() {
            return self
                .entry
                .mutable
                .as_ref()
                .unwrap()
                .is_compatible(other.entry.mutable.as_ref().unwrap());
        }

        false
    }

    pub fn reset_len(&mut self) -> Result<()> {
        let Some(mutable) = self.entry.mutable() else {
            return Err(Error::ImmutableBuffer);
        };

        mutable.reset_len();
        Ok(())
    }

    pub fn shrink_len(&mut self) -> Result<()> {
        let Some(mutable) = self.entry.mutable() else {
            return Err(Error::ImmutableBuffer);
        };

        mutable.shrink_len();
        Ok(())
    }

    pub fn data_ptr(&mut self) -> Result<*mut std::ffi::c_void> {
        let Some(mutable) = self.entry.mutable() else {
            return Err(Error::ImmutableBuffer);
        };

        Ok(mutable.data_ptr())
    }

    pub fn data_size_ptr(&mut self) -> Result<*mut u64> {
        let Some(mutable) = self.entry.mutable() else {
            return Err(Error::ImmutableBuffer);
        };

        Ok(mutable.data_size_ptr())
    }

    pub fn has_offsets(&mut self) -> Result<bool> {
        let Some(mutable) = self.entry.mutable() else {
            return Err(Error::ImmutableBuffer);
        };

        Ok(mutable.offsets_ptr() != std::ptr::null_mut())
    }

    pub fn offsets_ptr(&mut self) -> Result<*mut u64> {
        let Some(mutable) = self.entry.mutable() else {
            return Err(Error::ImmutableBuffer);
        };

        Ok(mutable.offsets_ptr())
    }

    pub fn offsets_size_ptr(&mut self) -> Result<*mut u64> {
        let Some(mutable) = self.entry.mutable() else {
            return Err(Error::ImmutableBuffer);
        };

        Ok(mutable.offsets_size_ptr())
    }

    pub fn has_validity(&mut self) -> Result<bool> {
        let Some(mutable) = self.entry.mutable() else {
            return Err(Error::ImmutableBuffer);
        };

        Ok(mutable.validity_ptr() != std::ptr::null_mut())
    }

    pub fn validity_ptr(&mut self) -> Result<*mut u8> {
        let Some(mutable) = self.entry.mutable() else {
            return Err(Error::ImmutableBuffer);
        };

        Ok(mutable.validity_ptr())
    }

    pub fn validity_size_ptr(&mut self) -> Result<*mut u64> {
        let Some(mutable) = self.entry.mutable() else {
            return Err(Error::ImmutableBuffer);
        };

        Ok(mutable.validity_size_ptr())
    }

    fn to_mutable(&mut self) -> Result<()> {
        self.entry.to_mutable()
    }

    fn to_shared(&mut self) -> Result<()> {
        self.entry.to_shared()
    }
}

impl From<Arc<dyn aa::Array>> for BufferEntry {
    fn from(array: Arc<dyn aa::Array>) -> Self {
        Self {
            entry: MutableOrShared::new(array),
        }
    }
}

pub struct QueryBuffers {
    buffers: HashMap<String, BufferEntry>,
}

impl QueryBuffers {
    pub fn new(buffers: HashMap<String, Arc<dyn aa::Array>>) -> Self {
        let mut new_buffers = HashMap::with_capacity(buffers.len());
        for (field, array) in buffers.into_iter() {
            new_buffers.insert(field, BufferEntry::from(array));
        }
        Self {
            buffers: new_buffers,
        }
    }

    /// Reset all mutable buffers' len to match its total capacity.
    pub fn reset_lengths(&mut self) -> Result<()> {
        for array in self.buffers.values_mut() {
            array.reset_len()?;
        }
        Ok(())
    }

    /// Shrink all mutable buffers' len to match what the TileDB size read.
    pub fn shrink_lengths(&mut self) -> Result<()> {
        for array in self.buffers.values_mut() {
            array.shrink_len()?;
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.buffers.len()
    }

    pub fn fields(&self) -> Vec<String> {
        self.buffers.keys().cloned().collect::<Vec<_>>()
    }

    pub fn get(&self, key: &String) -> Option<&BufferEntry> {
        self.buffers.get(key)
    }

    pub fn get_mut(&mut self, key: &String) -> Option<&mut BufferEntry> {
        self.buffers.get_mut(key)
    }

    pub fn from_fields(schema: Schema, fields: QueryFields) -> Result<Self> {
        let conv = ToArrowConverter::strict();
        let mut ret = HashMap::with_capacity(fields.fields.len());
        for (name, field) in fields.fields.into_iter() {
            let tdb_field = schema.field(name.clone())?;

            if let QueryField::Buffer(array) = field {
                Self::validate_buffer(&tdb_field, &array)?;
                ret.insert(name.clone(), array);
                continue;
            }

            // ToDo: Clean these error conversions up so they clearly indicate
            // a failed buffer creation.
            let tdb_dtype = tdb_field.datatype()?;
            let tdb_cvn = tdb_field.cell_val_num()?;
            let tdb_nullable = tdb_field.nullability()?;
            let arrow_type = if let Some(dtype) = field.target_type() {
                conv.convert_datatype_to(
                    &tdb_dtype,
                    &tdb_cvn,
                    tdb_nullable,
                    dtype,
                )
            } else {
                conv.convert_datatype(&tdb_dtype, &tdb_cvn, tdb_nullable)
            }
            .map_err(|e| Error::ArrowConversionError(name.clone(), e))?;

            let array = alloc_array(
                arrow_type,
                tdb_nullable,
                field.capacity().unwrap(),
            )?;
            ret.insert(name.clone(), array);
        }

        Ok(Self::new(ret))
    }

    pub fn is_compatible(&self, other: &Self) -> bool {
        let mut my_keys = self.buffers.keys().collect::<Vec<_>>();
        let mut their_keys = other.buffers.keys().collect::<Vec<_>>();

        my_keys.sort();
        their_keys.sort();
        if my_keys != their_keys {
            return false;
        }

        for key in my_keys {
            let mine = self.buffers.get(key);
            let theirs = other.buffers.get(key);

            if mine.is_none() || theirs.is_none() {
                return false;
            }

            if !mine.unwrap().is_compatible(theirs.unwrap()) {
                return false;
            }
        }

        return true;
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &BufferEntry)> {
        self.buffers.iter()
    }

    pub fn iter_mut(
        &mut self,
    ) -> impl Iterator<Item = (&String, &mut BufferEntry)> {
        self.buffers.iter_mut()
    }

    pub fn to_mutable(&mut self) -> Result<()> {
        for value in self.buffers.values_mut() {
            value.to_mutable()?
        }
        Ok(())
    }

    pub fn to_shared(&mut self) -> Result<()> {
        for value in self.buffers.values_mut() {
            value.to_shared()?
        }
        Ok(())
    }

    /// When I get to it, this needs to ensure that the provided array matches
    /// the field's TileDB datatype.
    fn validate_buffer(
        _field: &Field,
        _buffer: &Arc<dyn aa::Array>,
    ) -> Result<()> {
        Ok(())
    }
}

/// A small helper for users writing code directly against the TileDB API
///
/// This struct is freely convertible to and from a HashMap of Arrow arrays.
#[derive(Clone)]
pub struct SharedBuffers {
    buffers: HashMap<String, Arc<dyn aa::Array>>,
}

impl SharedBuffers {
    pub fn get<T: Any>(&self, key: &str) -> Option<&T>
    where
        T: Any,
    {
        self.buffers.get(key)?.as_any().downcast_ref::<T>()
    }
}

impl From<HashMap<String, Arc<dyn aa::Array>>> for SharedBuffers {
    fn from(buffers: HashMap<String, Arc<dyn aa::Array>>) -> Self {
        Self { buffers }
    }
}

impl From<SharedBuffers> for HashMap<String, Arc<dyn aa::Array>> {
    fn from(buffers: SharedBuffers) -> Self {
        buffers.buffers
    }
}

fn alloc_array(
    dtype: adt::DataType,
    nullable: bool,
    capacity: usize,
) -> Result<Arc<dyn aa::Array>> {
    let num_cells = calculate_num_cells(dtype.clone(), nullable, capacity)?;

    match dtype {
        adt::DataType::Boolean => {
            Ok(Arc::new(aa::BooleanArray::new_null(num_cells)))
        }
        adt::DataType::LargeList(field) => {
            let offsets = abuf::OffsetBuffer::<i64>::new_zeroed(num_cells);
            let value_capacity =
                capacity - (num_cells * std::mem::size_of::<i64>());
            let values =
                alloc_array(field.data_type().clone(), false, value_capacity)?;
            let nulls = if nullable {
                Some(abuf::NullBuffer::new_null(num_cells))
            } else {
                None
            };
            Ok(Arc::new(
                aa::LargeListArray::try_new(field, offsets, values, nulls)
                    .map_err(Error::ArrayCreationFailed)?,
            ))
        }
        adt::DataType::FixedSizeList(field, cvn) => {
            let nulls = if nullable {
                Some(abuf::NullBuffer::new_null(num_cells))
            } else {
                None
            };
            let values =
                alloc_array(field.data_type().clone(), false, capacity)?;
            Ok(Arc::new(
                aa::FixedSizeListArray::try_new(field, cvn, values, nulls)
                    .map_err(Error::ArrayCreationFailed)?,
            ))
        }
        adt::DataType::LargeUtf8 => {
            let offsets = abuf::OffsetBuffer::<i64>::new_zeroed(num_cells);
            let values = ArrowBufferMut::from_len_zeroed(
                capacity - (num_cells * std::mem::size_of::<i64>()),
            );
            let nulls = if nullable {
                Some(abuf::NullBuffer::new_null(num_cells))
            } else {
                None
            };
            Ok(Arc::new(
                aa::LargeStringArray::try_new(offsets, values.into(), nulls)
                    .map_err(Error::ArrayCreationFailed)?,
            ))
        }
        adt::DataType::LargeBinary => {
            let offsets = abuf::OffsetBuffer::<i64>::new_zeroed(num_cells);
            let values = ArrowBufferMut::from_len_zeroed(
                capacity - (num_cells * std::mem::size_of::<i64>()),
            );
            let nulls = if nullable {
                Some(abuf::NullBuffer::new_null(num_cells))
            } else {
                None
            };
            Ok(Arc::new(
                aa::LargeBinaryArray::try_new(offsets, values.into(), nulls)
                    .map_err(Error::ArrayCreationFailed)?,
            ))
        }
        _ if dtype.is_primitive() => {
            let data = ArrowBufferMut::from_len_zeroed(
                num_cells * dtype.primitive_width().unwrap(),
            );

            let nulls = if nullable {
                Some(ArrowBufferMut::from_len_zeroed(num_cells).into())
            } else {
                None
            };

            let data = aa::ArrayData::try_new(
                dtype,
                num_cells,
                nulls,
                0,
                vec![data.into()],
                vec![],
            )
            .map_err(|e| Error::ArrayCreationFailed(e))?;

            Ok(aa::make_array(data))
        }
        _ => todo!(),
    }
}

fn calculate_num_cells(
    dtype: adt::DataType,
    nullable: bool,
    capacity: usize,
) -> Result<usize> {
    match dtype {
        adt::DataType::Boolean => {
            if nullable {
                Ok(capacity * 8 / 2)
            } else {
                Ok(capacity * 8)
            }
        }
        adt::DataType::LargeList(ref field) => {
            if !field.data_type().is_primitive() {
                return Err(Error::UnsupportedArrowType(dtype.clone()));
            }

            // Todo: Figure out a better way to approximate values to offsets ratios
            // based on whatever Python does or some such.
            //
            // For now, I'll pull a guess at of the ether and assume on average a
            // var sized primitive array averages two values per cell. Becuase why
            // not?
            let width = field.data_type().primitive_width().unwrap();
            let bytes_per_cell = (width * 2)
                + std::mem::size_of::<i64>()
                + if nullable { 1 } else { 0 };
            Ok(capacity / bytes_per_cell)
        }
        adt::DataType::FixedSizeList(ref field, cvn) => {
            if !field.data_type().is_primitive() {
                return Err(Error::UnsupportedArrowType(dtype));
            }

            if cvn < 2 {
                return Err(Error::InvalidFixedSizeListLength(cvn));
            }

            let cvn = cvn as usize;
            let width = field.data_type().primitive_width().unwrap();
            let bytes_per_cell = capacity / (width * cvn);
            let bytes_per_cell = if nullable {
                bytes_per_cell + 1
            } else {
                bytes_per_cell
            };
            Ok(capacity / bytes_per_cell)
        }
        adt::DataType::LargeUtf8 | adt::DataType::LargeBinary => {
            let bytes_per_cell =
                AVERAGE_STRING_LENGTH + std::mem::size_of::<i64>();
            let bytes_per_cell = if nullable {
                bytes_per_cell + 1
            } else {
                bytes_per_cell
            };
            Ok(capacity / bytes_per_cell)
        }
        _ if dtype.is_primitive() => {
            let width = dtype.primitive_width().unwrap();
            let bytes_per_cell = width + if nullable { 1 } else { 0 };
            Ok(capacity / bytes_per_cell)
        }
        _ => Err(Error::UnsupportedArrowType(dtype.clone())),
    }
}

// Private utility functions

fn to_tdb_offsets(offsets: abuf::OffsetBuffer<i64>) -> Result<ArrowBufferMut> {
    offsets
        .into_inner()
        .into_inner()
        .into_mutable()
        .map_err(|_| Error::ArrayInUse)
}

fn to_tdb_validity(nulls: Option<abuf::NullBuffer>) -> Option<ArrowBufferMut> {
    nulls.map(|nulls| {
        ArrowBufferMut::from(
            nulls
                .iter()
                .map(|v| if v { 1u8 } else { 0 })
                .collect::<Vec<_>>(),
        )
    })
}

fn from_tdb_offsets(offsets: ArrowBufferMut) -> abuf::OffsetBuffer<i64> {
    let buffer = abuf::ScalarBuffer::<i64>::from(offsets);
    abuf::OffsetBuffer::new(buffer)
}

fn from_tdb_validity(
    validity: &Option<QueryBuffer>,
) -> Option<abuf::NullBuffer> {
    validity.as_ref().map(|v| {
        abuf::NullBuffer::from(
            v.buffer
                .iter()
                .map(|f| if *f != 0 { true } else { false })
                .collect::<Vec<_>>(),
        )
    })
}
