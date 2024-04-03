pub trait AnyNumCmp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering;
}

macro_rules! derive_primitive_anynumcmp {
    ($($T:ty),+) => {
        $(
            impl AnyNumCmp for $T {
                fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                    std::cmp::Ord::cmp(self, other)
                }
            }
        )+
    };
}

macro_rules! derive_float_anynumcmp {
    ($($T:ty),+) => {
        $(
            impl AnyNumCmp for $T {
                fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                    self.total_cmp(other)
                }
            }
        )+
    };
}

derive_primitive_anynumcmp!(u8, u16, u32, u64, usize);
derive_primitive_anynumcmp!(i8, i16, i32, i64, isize);
derive_float_anynumcmp!(f32, f64);
