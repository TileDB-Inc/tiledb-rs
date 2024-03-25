use crate::error::Error;
use crate::Datatype;
use crate::Result as TileDBResult;

/// The ReferenceColumn is used when we can safely use the existing Vec's or
/// slices being used to create a column. This avoids the allocation, copy, and
/// eventual deallocation when providing column data to TileDB.
pub struct ReferenceColumn<'data> {
    num_values: u64,
    value_size: u64,
    data: &'data [u8],
    offsets: Option<&'data [u64]>,
}

/// If a column requires allocating a linearized buffer (i.e., when passing a
/// Vec or slice of AsRef<str> values), this structure is used to hold the
/// allocated buffers.
pub struct AllocatedColumn {
    num_values: u64,
    value_size: u64,
    data: Box<[u8]>,
    offsets: Option<Box<[u64]>>,
}

/// The Column struct is used to represent a column of data in TileDB. A column
/// contains a required data slice and an optional offsets slice when the
/// underlying column definition has a variable number of values per cell.
///
/// An AsColumn trait is provided for a number of common use cases such that
/// users should be able to pass common instances directly as arguments that
/// have the Column type.
pub enum Column<'data> {
    Referenced(ReferenceColumn<'data>),
    Allocated(AllocatedColumn),
}

impl<'data> Column<'data> {
    pub fn from_references(
        num_values: u64,
        value_size: u64,
        data: &'data [u8],
        offsets: Option<&'data [u64]>,
    ) -> Self {
        let offsets = if offsets.is_some() {
            if !offsets.unwrap().is_empty() {
                offsets
            } else {
                None
            }
        } else {
            None
        };
        Column::Referenced(ReferenceColumn {
            num_values,
            value_size,
            data,
            offsets: offsets,
        })
    }

    pub fn from_allocations(
        num_values: u64,
        value_size: u64,
        data: Box<[u8]>,
        offsets: Option<Box<[u64]>>,
    ) -> Self {
        let offsets = if offsets.is_some() {
            if !offsets.as_ref().unwrap().is_empty() {
                offsets
            } else {
                None
            }
        } else {
            None
        };
        Column::Allocated(AllocatedColumn {
            num_values,
            value_size,
            data,
            offsets,
        })
    }

    pub fn to_vec<T: 'static + Clone>(
        &self,
        datatype: Datatype,
        cell_val_size: u32,
    ) -> TileDBResult<Vec<T>> {
        // Check that the requested datatype is compatible with this datatype.
        if !datatype.is_compatible_type::<T>() {
            return Err(Error::from(format!(
                "Requested datatype is incompatible with {}",
                datatype.to_string().unwrap_or("<unknown type>".to_owned())
            )));
        }

        // For var sized data, use the to_type_vec methods for conversion.
        if cell_val_size == u32::MAX {
            return Err(Error::from(
                "to_vec<T> does not support var \
                sized data. Use one of the specialized \
                to_type_vec() methods.",
            ));
        }

        // Check that we have the right number of bytes for the given datatype.
        if self.data().len() % datatype.size() as usize != 0 {
            return Err(Error::from("Invalid datatype for column data."));
        }

        // Check that the number of elements satisfies the cell value size.
        let elems = self.data().len() / datatype.size() as usize;
        if elems % cell_val_size as usize != 0 {
            return Err(Error::from(
                "Invalid cell value size for column data.",
            ));
        }

        // At this point we're ready to create our Vec to return. For now
        // I'm going to cowboy it a bit and assume that alignment is
        // correct and/or not an issue for slices. To my knowledge, the
        // alignment requirements are triggered when we give Rust a pointer
        // to memory that it then takes over managing. I.e., if C++ allocated
        // something that we then tell Rust to deallocate. In those instances
        // Rust has specific alignment requirements due to its Allocator.
        //
        // Half the reason for using slices is to avoid that whole issue. The
        // goal is to let Rust manage Rust memory, and let C++ manage C++
        // memory.
        let vals: &[T] = unsafe {
            std::slice::from_raw_parts(
                self.data().as_ptr() as *const T,
                self.data().len() / std::mem::size_of::<T>(),
            )
        };

        Ok(vals.to_vec())
    }

    pub fn to_string_vec(
        &self,
        datatype: Datatype,
        cell_val_size: u32,
    ) -> TileDBResult<Vec<String>> {
        // Currently we're only supporting ASCII and UTF-8 strings.
        if !matches!(datatype, Datatype::StringAscii | Datatype::StringUtf8) {
            return Err(Error::from(format!(
                "Invalid datatype '{}' for to_string_vec",
                datatype.to_string().unwrap_or("<unknown type>".to_owned())
            )));
        }

        // For var sized data, use the to_type_vec methods for conversion.
        if cell_val_size != u32::MAX {
            return Err(Error::from("Use to_vec<T> for fixed sized data."));
        }

        if self.offsets().is_none() {
            return Ok(Vec::new());
        }

        let mut ret: Vec<String> = Vec::new();

        let data = self.data();
        let offsets = self.offsets().unwrap();
        let num_offsets = offsets.len();
        for (idx, offset) in offsets.iter().enumerate() {
            let len = if idx == num_offsets - 1 {
                self.data().len() as u64 - *offset
            } else {
                offsets[idx + 1] - offset
            };

            let val = String::from_utf8(
                data[(*offset as usize)..((*offset + len) as usize)].to_vec(),
            )
            .map_err(|e| {
                Error::from(format!(
                    "Invalid UTF-8 in column data. \
                    Use to_boxed_u8_vec instead: {}",
                    e
                ))
            })?;

            ret.push(val);
        }

        Ok(ret)
    }

    pub fn to_boxed_u8_vec(
        &self,
        datatype: Datatype,
        cell_val_size: u32,
    ) -> TileDBResult<Vec<Box<[u8]>>> {
        // Currently we're only supporting ASCII and UTF-8 strings.
        if !matches!(datatype, Datatype::StringAscii | Datatype::StringUtf8) {
            return Err(Error::from(format!(
                "Invalid datatype '{}' for to_string_vec",
                datatype.to_string().unwrap_or("<unknown type>".to_owned())
            )));
        }

        // For var sized data, use the to_type_vec methods for conversion.
        if cell_val_size != u32::MAX {
            return Err(Error::from("Use to_vec<T> for fixed sized data."));
        }

        if self.offsets().is_none() {
            return Ok(Vec::new());
        }

        let mut ret: Vec<Box<[u8]>> = Vec::new();

        let data = self.data();
        let offsets = self.offsets().unwrap();
        let num_offsets = offsets.len();
        for (idx, offset) in offsets.iter().enumerate() {
            let len = if idx == num_offsets - 1 {
                self.data().len() as u64 - *offset
            } else {
                offset - offsets[idx - 1]
            };

            let val = data[(*offset as usize)..((*offset + len) as usize)]
                .to_vec()
                .into_boxed_slice();

            ret.push(val);
        }

        Ok(ret)
    }

    pub fn num_values(&self) -> u64 {
        match self {
            Column::Referenced(col) => col.num_values,
            Column::Allocated(col) => col.num_values,
        }
    }

    pub fn value_size(&self) -> u64 {
        match self {
            Column::Referenced(col) => col.value_size,
            Column::Allocated(col) => col.value_size,
        }
    }

    pub fn data(&self) -> &[u8] {
        match self {
            Column::Referenced(col) => col.data,
            Column::Allocated(col) => &col.data[..],
        }
    }

    pub fn offsets(&self) -> Option<&[u64]> {
        match self {
            Column::Referenced(col) => col.offsets,
            Column::Allocated(col) => match &col.offsets {
                Some(offsets) => unsafe {
                    Some(std::slice::from_raw_parts(
                        offsets.as_ptr(),
                        self.num_values() as usize,
                    ))
                },
                None => None,
            },
        }
    }

    pub fn is_allocated(&self) -> bool {
        match self {
            Column::Referenced(_) => false,
            Column::Allocated(_) => true,
        }
    }
}

/// The AsColumn trait provides an auto-conversion from common datatypes into
/// the columnar format used by TileDB.
///
/// There are three type "patterns" that fit into the auto conversion scheme.
///
/// 1. The simplest is a Vec or slice of primitive values. This mode can be
///    used for any non-variable sized cell type, even multi-value cells. The
///    length of the Vec or slice must be a multiple of the cell value num.
///
/// 2. The second most common is for string based data. This allows users to
///    pass a Vec or slice of `AsRef<str>` instances which are then transformed
///    into the correct data and offsets buffers. This method allocates memory
///    to hold the linearized data buffer and offsets buffer.
///
/// 3. The third is a pair of Vec's or slices where the first is the contents
///    of the data buffer and the second is a `Vec<u64>` or `&[u64]` that
///    contains the offsets for each var sized cell. Generally speaking this
///    is an uncommon use case when users need a variable sized cell that is
///    not a string.
pub trait AsColumn<'data> {
    fn as_column(&'data self) -> Column<'data>;
}

pub trait TryAsColumn<'data> {
    fn try_as_column(&'data self) -> TileDBResult<Column<'data>>;
}

impl<'data, T: AsColumn<'data>> TryAsColumn<'data> for T {
    fn try_as_column(&'data self) -> TileDBResult<Column<'data>> {
        Ok(self.as_column())
    }
}

// Conversion for Vec or slices of primitive values for non-variadic columns.
macro_rules! derive_primitive_as_column {
    ($typename:ty) => {
        impl<'data> AsColumn<'data> for &'data [$typename] {
            fn as_column(&self) -> Column<'data> {
                let num_bytes = self.len() * std::mem::size_of::<$typename>();
                let data = unsafe {
                    std::slice::from_raw_parts(
                        self.as_ptr() as *const u8,
                        num_bytes,
                    )
                };

                Column::from_references(
                    self.len() as u64,
                    std::mem::size_of::<$typename>() as u64,
                    data,
                    Some(&[0u64; 0]),
                )
            }
        }

        impl<'data> AsColumn<'data> for Vec<$typename> {
            fn as_column(&self) -> Column<'data> {
                let num_bytes = self.len() * std::mem::size_of::<$typename>();
                let data = unsafe {
                    std::slice::from_raw_parts(
                        self.as_ptr() as *const u8,
                        num_bytes,
                    )
                };

                Column::from_references(
                    self.len() as u64,
                    std::mem::size_of::<$typename>() as u64,
                    data,
                    Some(&[0u64; 0]),
                )
            }
        }
    };
}

derive_primitive_as_column!(u8);
derive_primitive_as_column!(u16);
derive_primitive_as_column!(u32);
derive_primitive_as_column!(u64);
derive_primitive_as_column!(i8);
derive_primitive_as_column!(i16);
derive_primitive_as_column!(i32);
derive_primitive_as_column!(i64);
derive_primitive_as_column!(f32);
derive_primitive_as_column!(f64);

// I couldn't figure out how to derive directly from the AsRef<str> so I've
// made this for now. Ideally we could just implement two for Vec<AsRef<str>>
// and &[AsRef<str>]. Rather than beat my head against it I'm moving on.
macro_rules! derive_strings_as_column {
    ($typename:ty) => {
        impl AsColumn<'_> for &[$typename] {
            fn as_column(&self) -> Column {
                let (data, offsets) = strings_to_buffers(self);
                Column::from_allocations(
                    self.len() as u64,
                    1,
                    data,
                    Some(offsets),
                )
            }
        }

        impl AsColumn<'_> for Vec<$typename> {
            fn as_column(&self) -> Column {
                let (data, offsets) = strings_to_buffers(self);
                Column::from_allocations(
                    self.len() as u64,
                    1,
                    data,
                    Some(offsets),
                )
            }
        }
    };
}

derive_strings_as_column!(&str);
derive_strings_as_column!(&String);
derive_strings_as_column!(String);

// Internal helper to convert a slice of strings to a single linear data buffer
// and an offsets buffer.
fn strings_to_buffers(vals: &[impl AsRef<str>]) -> (Box<[u8]>, Box<[u64]>) {
    let mut num_bytes: usize = 0;
    for val in vals {
        num_bytes += val.as_ref().len();
    }

    let mut data: Box<[u8]> = vec![0; num_bytes].into_boxed_slice();
    let mut offsets: Box<[u64]> = vec![0; vals.len()].into_boxed_slice();

    let mut offset: usize = 0;
    for (idx, val) in vals.iter().enumerate() {
        let len = val.as_ref().len();
        data[offset..(offset + len)].copy_from_slice(val.as_ref().as_bytes());
        offsets[idx] = offset as u64;
        offset += len;
    }

    (data, offsets)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u32_test() {
        let data = vec![1u32, 2, 3, 4, 5];
        let col = data.as_column();
        assert_eq!(col.num_values(), 5);
        assert_eq!(col.value_size(), std::mem::size_of::<u32>() as u64);
        assert_eq!(col.data().len(), 5 * std::mem::size_of::<u32>());
        assert_eq!(col.data()[0], 1u8);
        assert_eq!(col.data()[16], 5u8);
        assert!(col.offsets().is_none());
        assert!(!col.is_allocated());
    }

    #[test]
    fn str_test() {
        let data = vec!["foo", "bar", "baz", "bing"];
        let col = data.as_column();
        assert_eq!(col.num_values(), 4);
        assert_eq!(col.value_size(), 1);
        assert_eq!(col.data().len(), 13);
        assert_eq!(col.data()[0], b'f');
        assert_eq!(col.data()[6], b'b');
        assert_eq!(col.data()[7], b'a');
        assert_eq!(col.data()[8], b'z');
        assert_eq!(col.offsets().expect("Invalid offsets").len(), 4);
        assert_eq!(col.offsets().expect("Invalid offsets")[1], 3);
        assert!(col.is_allocated());
    }
}
