pub trait CAPISameRepr: Copy + Default {}

impl CAPISameRepr for u8 {}
impl CAPISameRepr for u16 {}
impl CAPISameRepr for u32 {}
impl CAPISameRepr for u64 {}
impl CAPISameRepr for i8 {}
impl CAPISameRepr for i16 {}
impl CAPISameRepr for i32 {}
impl CAPISameRepr for i64 {}
impl CAPISameRepr for f32 {}
impl CAPISameRepr for f64 {}

pub trait CAPIConverter {
    type CAPIType: Default + Copy;

    fn to_capi(&self) -> Self::CAPIType;
    fn to_rust(value: &Self::CAPIType) -> Self;
}

impl<T: CAPISameRepr> CAPIConverter for T {
    type CAPIType = Self;

    fn to_capi(&self) -> Self::CAPIType {
        *self
    }

    fn to_rust(value: &Self::CAPIType) -> T {
        *value
    }
}
