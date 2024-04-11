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

/// Trait for comparisons based on value bits.
/// This exists to work around float NaN which is not equal to itself,
/// but we want it to be for generic operations with TileDB structures.
/*
 * Fun fact:
 * `impl<T> BitsEq for T where T: Eq` is forbidden in concert with
 * `impl BitsEq for f32` because the compiler says that `std` may
 * `impl Eq for f32` someday. Seems unlikely.
 */
pub trait BitsEq: PartialEq {
    fn bits_eq(&self, other: &Self) -> bool;
}

macro_rules! derive_reflexive_eq {
    ($typename:ty) => {
        impl BitsEq for $typename {
            fn bits_eq(&self, other: &Self) -> bool {
                <Self as PartialEq>::eq(self, other)
            }
        }
    };
}

derive_reflexive_eq!(bool);
derive_reflexive_eq!(u8);
derive_reflexive_eq!(u16);
derive_reflexive_eq!(u32);
derive_reflexive_eq!(u64);
derive_reflexive_eq!(i8);
derive_reflexive_eq!(i16);
derive_reflexive_eq!(i32);
derive_reflexive_eq!(i64);

impl BitsEq for f32 {
    fn bits_eq(&self, other: &Self) -> bool {
        self.to_bits() == other.to_bits()
    }
}

impl BitsEq for f64 {
    fn bits_eq(&self, other: &Self) -> bool {
        self.to_bits() == other.to_bits()
    }
}

impl<T1, T2> BitsEq for (T1, T2)
where
    T1: BitsEq,
    T2: BitsEq,
{
    fn bits_eq(&self, other: &Self) -> bool {
        self.0.bits_eq(&other.0) && self.1.bits_eq(&other.1)
    }
}
