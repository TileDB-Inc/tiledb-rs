pub use arrow::buffer::{Buffer, MutableBuffer};
use arrow::datatypes::ArrowNativeType;

mod private {
    pub trait Sealed {}
}

pub trait PhysicalType: ArrowNativeType + private::Sealed + 'static {
    fn slice_to_buffer(value: &[Self]) -> Buffer;
    fn vec_to_buffer(value: Vec<Self>) -> Buffer;
}

impl private::Sealed for i8 {}
impl private::Sealed for i16 {}
impl private::Sealed for i32 {}
impl private::Sealed for i64 {}
impl private::Sealed for u8 {}
impl private::Sealed for u16 {}
impl private::Sealed for u32 {}
impl private::Sealed for u64 {}
impl private::Sealed for f32 {}
impl private::Sealed for f64 {}

impl PhysicalType for i8 {
    fn slice_to_buffer(value: &[Self]) -> Buffer {
        Buffer::from_vec(value.to_vec())
    }

    fn vec_to_buffer(value: Vec<Self>) -> Buffer {
        Buffer::from_vec(value)
    }
}

impl PhysicalType for i16 {
    fn slice_to_buffer(value: &[Self]) -> Buffer {
        Buffer::from_vec(value.to_vec())
    }

    fn vec_to_buffer(value: Vec<Self>) -> Buffer {
        Buffer::from_vec(value)
    }
}

impl PhysicalType for i32 {
    fn slice_to_buffer(value: &[Self]) -> Buffer {
        Buffer::from_vec(value.to_vec())
    }

    fn vec_to_buffer(value: Vec<Self>) -> Buffer {
        Buffer::from_vec(value)
    }
}

impl PhysicalType for i64 {
    fn slice_to_buffer(value: &[Self]) -> Buffer {
        Buffer::from_vec(value.to_vec())
    }

    fn vec_to_buffer(value: Vec<Self>) -> Buffer {
        Buffer::from_vec(value)
    }
}

impl PhysicalType for u8 {
    fn slice_to_buffer(value: &[Self]) -> Buffer {
        Buffer::from_vec(value.to_vec())
    }

    fn vec_to_buffer(value: Vec<Self>) -> Buffer {
        Buffer::from_vec(value)
    }
}

impl PhysicalType for u16 {
    fn slice_to_buffer(value: &[Self]) -> Buffer {
        Buffer::from_vec(value.to_vec())
    }

    fn vec_to_buffer(value: Vec<Self>) -> Buffer {
        Buffer::from_vec(value)
    }
}

impl PhysicalType for u32 {
    fn slice_to_buffer(value: &[Self]) -> Buffer {
        Buffer::from_vec(value.to_vec())
    }

    fn vec_to_buffer(value: Vec<Self>) -> Buffer {
        Buffer::from_vec(value)
    }
}

impl PhysicalType for u64 {
    fn slice_to_buffer(value: &[Self]) -> Buffer {
        Buffer::from_vec(value.to_vec())
    }

    fn vec_to_buffer(value: Vec<Self>) -> Buffer {
        Buffer::from_vec(value)
    }
}

impl PhysicalType for f32 {
    fn slice_to_buffer(value: &[Self]) -> Buffer {
        Buffer::from_vec(value.to_vec())
    }

    fn vec_to_buffer(value: Vec<Self>) -> Buffer {
        Buffer::from_vec(value)
    }
}

impl PhysicalType for f64 {
    fn slice_to_buffer(value: &[Self]) -> Buffer {
        Buffer::from_vec(value.to_vec())
    }

    fn vec_to_buffer(value: Vec<Self>) -> Buffer {
        Buffer::from_vec(value)
    }
}
