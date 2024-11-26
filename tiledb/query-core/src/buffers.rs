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
use tiledb_api::array::schema::Schema;
use tiledb_api::error::Error as TileDBError;
use tiledb_api::query::read::aggregate::AggregateFunctionHandle;
use tiledb_api::ContextBound;
use tiledb_common::array::CellValNum;

use super::datatype::ToArrowConverter;
use super::field::QueryField;
use super::fields::{QueryField as RequestField, QueryFields};
use super::RawQuery;

const AVERAGE_STRING_LENGTH: usize = 64;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error converting to Arrow for field '{0}': {1}")]
    ArrowConversionError(String, crate::datatype::Error),
    #[error("Failed to convert Arrow Array for field '{0}': {1}")]
    FailedConversionFromArrow(String, Box<Error>),
    #[error("Failed to add field '{0}' to query: {1}")]
    Field(String, #[source] FieldError),
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
    #[error("Internal string type mismatch error")]
    InternalStringType,
    #[error("Error converting var sized buffers to arrow: {0}")]
    InvalidVarBuffers(ArrowError),
    #[error("Failed to convert internal list array: {0}")]
    ListSubarrayConversion(Box<Error>),
    #[error("Internal TileDB Error: {0}")]
    TileDB(#[from] TileDBError),
    #[error("Unexpected var sized arrow type: {0}")]
    UnexpectedArrowVarType(adt::DataType),
    #[error("Mutable buffers are not shareable.")]
    UnshareableMutableBuffer,
}

type Result<T> = std::result::Result<T, Error>;

/// An error that occurred when adding a field to the query.
#[derive(Debug, Error)]
pub enum FieldError {
    #[error("Error reading query field: {0}")]
    QueryField(#[from] crate::field::Error),
    #[error("Type mismatch for requested field: {0}")]
    TypeMismatch(crate::datatype::Error),
    #[error("Failed to allocate buffer: {0}")]
    BufferAllocation(ArrowError),
    #[error("Unsupported arrow array: {0}")]
    UnsupportedArrowArray(#[from] UnsupportedArrowArrayError),
}

type FieldResult<T> = std::result::Result<T, FieldError>;

#[derive(Debug, Error)]
pub enum UnsupportedArrowArrayError {
    #[error("Unsupported arrow array type: {0}")]
    UnsupportedArrowType(adt::DataType),
    #[error("TileDB does not support nullable list elements")]
    InvalidNullableListElements,
    #[error("Invalid fixed sized list length {0} is less than 2")]
    InvalidFixedSizeListLength(i32),
    #[error(
        "TileDB only supports fixed size lists of primitive types, not {0}"
    )]
    UnsupportedFixedSizeListType(adt::DataType),
    #[error("Invalid data type for bytes array: {0}")]
    InvalidBytesType(adt::DataType),
    #[error("Invalid data type for primitive data: {0}")]
    InvalidPrimitiveType(adt::DataType),
    #[error("TileDB does not support timezones")]
    UnsupportedTimeZones,
    #[error("Only the large variant is supported: {0}")]
    LargeVariantOnly(adt::DataType),
    #[error("Failed to create arrow array: {0}")]
    ArrayCreationFailed(ArrowError),
    #[error("Array is in use: {0}")]
    InUse(#[from] ArrayInUseError),
}

type UnsupportedArrowArrayResult<T> =
    std::result::Result<T, UnsupportedArrowArrayError>;

#[derive(Debug, Error)]
pub enum ArrayInUseError {
    #[error("External references to offsets buffer")]
    Offsets,
    #[error("External references to array")]
    Array,
}

type ArrayInUseResult<T> = std::result::Result<T, ArrayInUseError>;

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
    (Box<dyn NewBufferTraitThing>, UnsupportedArrowArrayError),
>;

/// The error type to use on TryFrom<Arc<dyn aa::Arrrow>> implementations
type FromArrowError = (Arc<dyn aa::Array>, UnsupportedArrowArrayError);

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
    fn is_compatible(&self, other: &dyn NewBufferTraitThing) -> bool;

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
            validity: validity.map(QueryBuffer::new),
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

    fn is_compatible(&self, other: &dyn NewBufferTraitThing) -> bool {
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
        Err((array, ArrayInUseError::Offsets.into()))
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
            t => Err((array, UnsupportedArrowArrayError::InvalidBytesType(t))),
        }
    }
}

impl NewBufferTraitThing for ByteBuffers {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn len(&self) -> usize {
        self.offsets.capacity_var_cells()
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

    fn is_compatible(&self, other: &dyn NewBufferTraitThing) -> bool {
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
        let num_cells = self.offsets.num_var_cells();

        let dtype = self.dtype;
        let data = ArrowBuffer::from(self.data.buffer);
        let offsets = ArrowBuffer::from(self.offsets.buffer);
        let validity = from_tdb_validity(&self.validity);

        // N.B., the calls to cloning the data/offsets/validity are as cheap
        // as an Arc::clone plus pointer and usize copy. They are *not* cloning
        // the underlying allocated data.
        match aa::ArrayData::try_new(
            dtype.clone(),
            num_cells,
            validity.clone().map(|v| v.into_inner().into_inner()),
            0,
            vec![offsets.clone(), data.clone()],
            vec![],
        )
        .map(aa::make_array)
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

                Err((boxed, UnsupportedArrowArrayError::ArrayCreationFailed(e)))
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
            return Err((
                array,
                UnsupportedArrowArrayError::InvalidNullableListElements,
            ));
        }

        if cvn < 2 {
            return Err((
                array,
                UnsupportedArrowArrayError::InvalidFixedSizeListLength(cvn),
            ));
        }

        // SAFETY: We just showed cvn >= 2 && cvn is i32 whicih means
        // it can't be u32::MAX
        let cvn = CellValNum::try_from(cvn as u32)
            .expect("Internal cell val num error");

        let dtype = field.data_type().clone();
        if !dtype.is_primitive() {
            return Err((
                array,
                UnsupportedArrowArrayError::UnsupportedFixedSizeListType(dtype),
            ));
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

    fn is_compatible(&self, other: &dyn NewBufferTraitThing) -> bool {
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
            vec![data.clone()],
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

                Err((boxed, UnsupportedArrowArrayError::ArrayCreationFailed(e)))
            }
        }
    }
}

pub struct QueryBuffer {
    buffer: ArrowBufferMut,
    size: Pin<Box<u64>>,
}

impl QueryBuffer {
    pub fn new(buffer: ArrowBufferMut) -> Self {
        let size = Box::pin(buffer.len() as u64);
        Self { buffer, size }
    }

    pub fn data_ptr(&mut self) -> *mut std::ffi::c_void {
        self.buffer.as_mut_ptr() as *mut std::ffi::c_void
    }

    pub fn offsets_ptr(&mut self) -> *mut u64 {
        self.buffer.as_mut_ptr() as *mut u64
    }

    pub fn validity_ptr(&mut self) -> *mut u8 {
        self.buffer.as_mut_ptr()
    }

    pub fn size_ptr(&mut self) -> *mut u64 {
        self.size.as_mut().get_mut()
    }

    pub fn reset(&mut self) {
        *self.size = self.buffer.capacity() as u64;
        self.resize()
    }

    pub fn resize(&mut self) {
        self.buffer.resize(*self.size as usize, 0);
    }

    /// Returns the number of variable-length cells which this buffer
    /// has room to hold offsets for
    pub fn capacity_var_cells(&self) -> usize {
        if self.buffer.is_empty() {
            0
        } else {
            (self.buffer.len() / std::mem::size_of::<u64>()) - 1
        }
    }

    /// Returns the number of variable-length cells which the offsets
    /// in this buffer describe
    pub fn num_var_cells(&self) -> usize {
        if *self.size == 0 {
            0
        } else {
            (*self.size as usize / std::mem::size_of::<u64>()) - 1
        }
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
            return Err((
                array,
                UnsupportedArrowArrayError::InvalidNullableListElements,
            ));
        }

        let dtype = field.data_type().clone();
        if !dtype.is_primitive() {
            return Err((
                array,
                UnsupportedArrowArrayError::UnsupportedFixedSizeListType(dtype),
            ));
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
                return Err((array, ArrayInUseError::Array.into()));
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
        self.offsets.num_var_cells()
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

    fn is_compatible(&self, other: &dyn NewBufferTraitThing) -> bool {
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

        let num_cells = self.offsets.num_var_cells();

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
            num_cells,
            None,
            0,
            vec![data.clone()],
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

                Err((boxed, UnsupportedArrowArrayError::ArrayCreationFailed(e)))
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
                (aa::make_array(data), ArrayInUseError::Array.into())
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
            t => Err((
                array,
                UnsupportedArrowArrayError::InvalidPrimitiveType(t),
            )),
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

    fn is_compatible(&self, other: &dyn NewBufferTraitThing) -> bool {
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

                Err((boxed, UnsupportedArrowArrayError::ArrayCreationFailed(e)))
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
                Err((array, UnsupportedArrowArrayError::UnsupportedTimeZones))
            }

            adt::DataType::Binary
            | adt::DataType::List(_)
            | adt::DataType::Utf8 => Err((
                array,
                UnsupportedArrowArrayError::LargeVariantOnly(dtype),
            )),

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
            | adt::DataType::RunEndEncoded(_, _) => Err((
                array,
                UnsupportedArrowArrayError::UnsupportedArrowType(dtype),
            )),
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

    pub fn make_mut(&mut self) -> UnsupportedArrowArrayResult<()> {
        self.validate();

        if self.mutable.is_some() {
            return Ok(());
        }

        let shared: Arc<dyn aa::Array> = self.shared.take().unwrap();
        let maybe_mutable = Box::<dyn NewBufferTraitThing>::try_from(shared);

        let ret = if maybe_mutable.is_ok() {
            self.mutable = maybe_mutable.ok();
            Ok(())
        } else {
            let (array, err) = maybe_mutable.err().unwrap();
            self.shared = Some(array);
            Err(err)
        };

        self.validate();
        ret
    }

    pub fn make_shared(&mut self) -> UnsupportedArrowArrayResult<()> {
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
    aggregate: Option<AggregateFunctionHandle>,
}

impl BufferEntry {
    pub fn as_shared(&self) -> Result<Arc<dyn aa::Array>> {
        let Some(ref array) = self.entry.shared() else {
            return Err(Error::UnshareableMutableBuffer);
        };

        Ok(Arc::clone(array))
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
                .is_compatible(other.entry.mutable.as_ref().unwrap().as_ref());
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

    pub fn data_mut(&mut self) -> Result<&mut QueryBuffer> {
        let Some(mutable) = self.entry.mutable() else {
            return Err(Error::ImmutableBuffer);
        };

        Ok(mutable.data())
    }

    pub fn offsets_mut(&mut self) -> Result<Option<&mut QueryBuffer>> {
        let Some(mutable) = self.entry.mutable() else {
            return Err(Error::ImmutableBuffer);
        };

        Ok(mutable.offsets())
    }

    pub fn validity_mut(&mut self) -> Result<Option<&mut QueryBuffer>> {
        let Some(mutable) = self.entry.mutable() else {
            return Err(Error::ImmutableBuffer);
        };

        Ok(mutable.validity())
    }

    fn make_mut(&mut self) -> UnsupportedArrowArrayResult<()> {
        self.entry.make_mut()
    }

    fn make_shared(&mut self) -> UnsupportedArrowArrayResult<()> {
        self.entry.make_shared()
    }

    pub fn aggregate(&self) -> Option<&AggregateFunctionHandle> {
        self.aggregate.as_ref()
    }
}

impl From<Arc<dyn aa::Array>> for BufferEntry {
    fn from(array: Arc<dyn aa::Array>) -> Self {
        Self {
            entry: MutableOrShared::new(array),
            aggregate: None,
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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

    pub(crate) fn from_fields(
        schema: Schema,
        raw: &RawQuery,
        fields: QueryFields,
    ) -> Result<Self> {
        let mut buffers = HashMap::with_capacity(fields.fields.len());
        for (name, request_field) in fields.fields.into_iter() {
            let query_field = QueryField::get(&schema.context(), raw, &name)
                .map_err(|e| Error::Field(name.clone(), e.into()))?;
            let array = to_array(request_field, query_field)
                .map_err(|e| Error::Field(name.clone(), e))?;

            buffers.insert(name.clone(), BufferEntry::from(array));
        }

        for (name, (function, request_field)) in fields.aggregates.into_iter() {
            let handle = AggregateFunctionHandle::new(function)?;

            let query_field = QueryField::get(&schema.context(), raw, &name)
                .map_err(|e| Error::Field(name.clone(), e.into()))?;
            let array = to_array(request_field, query_field)
                .map_err(|e| Error::Field(name.clone(), e))?;

            buffers.insert(
                name.to_owned(),
                BufferEntry {
                    entry: MutableOrShared::new(array),
                    aggregate: Some(handle),
                },
            );
        }

        Ok(Self { buffers })
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

        true
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &BufferEntry)> {
        self.buffers.iter()
    }

    pub fn iter_mut(
        &mut self,
    ) -> impl Iterator<Item = (&String, &mut BufferEntry)> {
        self.buffers.iter_mut()
    }

    pub fn make_mut(&mut self) -> Result<()> {
        for (name, value) in self.buffers.iter_mut() {
            value
                .make_mut()
                .map_err(|e| Error::Field(name.clone(), e.into()))?;
        }
        Ok(())
    }

    pub fn make_shared(&mut self) -> Result<()> {
        for (name, value) in self.buffers.iter_mut() {
            value
                .make_shared()
                .map_err(|e| Error::Field(name.clone(), e.into()))?;
        }
        Ok(())
    }
}

fn to_array(
    field: RequestField,
    tiledb_field: QueryField,
) -> FieldResult<Arc<dyn aa::Array>> {
    let conv = ToArrowConverter::strict();

    let tdb_dtype = tiledb_field.datatype()?;
    let tdb_cvn = tiledb_field.cell_val_num()?;
    let tdb_nullable = tiledb_field.nullable()?;

    if let RequestField::Buffer(array) = field {
        // FIXME: validate data type and nullability
        return Ok(array);
    }

    let arrow_type = if let Some(dtype) = field.target_type() {
        conv.convert_datatype_to(&tdb_dtype, &tdb_cvn, tdb_nullable, dtype)
    } else {
        conv.convert_datatype(&tdb_dtype, &tdb_cvn, tdb_nullable)
    }
    .map_err(FieldError::TypeMismatch)?;

    alloc_array(arrow_type, tdb_nullable, field.capacity().unwrap())
}

/// A small helper for users writing code directly against the TileDB API
///
/// This struct is freely convertible to and from a HashMap of Arrow arrays.
#[derive(Clone)]
pub struct SharedBuffers {
    buffers: HashMap<String, Arc<dyn aa::Array>>,
}

impl SharedBuffers {
    pub fn get<T>(&self, key: &str) -> Option<&T>
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
) -> FieldResult<Arc<dyn aa::Array>> {
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
                    .map_err(FieldError::BufferAllocation)?,
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
                    .map_err(FieldError::BufferAllocation)?,
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
                    .map_err(FieldError::BufferAllocation)?,
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
                    .map_err(FieldError::BufferAllocation)?,
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
            .map_err(FieldError::BufferAllocation)?;

            Ok(aa::make_array(data))
        }
        _ => todo!(),
    }
}

fn calculate_num_cells(
    dtype: adt::DataType,
    nullable: bool,
    capacity: usize,
) -> FieldResult<usize> {
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
                return Err(UnsupportedArrowArrayError::UnsupportedArrowType(
                    dtype.clone(),
                )
                .into());
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
                return Err(UnsupportedArrowArrayError::UnsupportedArrowType(
                    dtype,
                )
                .into());
            }

            if cvn < 2 {
                return Err(
                    UnsupportedArrowArrayError::InvalidFixedSizeListLength(cvn)
                        .into(),
                );
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
        _ => Err(UnsupportedArrowArrayError::UnsupportedArrowType(
            dtype.clone(),
        )
        .into()),
    }
}

// Private utility functions

fn to_tdb_offsets(
    offsets: abuf::OffsetBuffer<i64>,
) -> ArrayInUseResult<ArrowBufferMut> {
    offsets
        .into_inner()
        .into_inner()
        .into_mutable()
        .map_err(|_| ArrayInUseError::Offsets)
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
            v.buffer.iter().map(|f| *f != 0).collect::<Vec<_>>(),
        )
    })
}
