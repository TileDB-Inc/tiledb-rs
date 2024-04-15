use float_next_after::NextAfter;

pub trait AnyNumCmp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering;
}

macro_rules! derive_primitive_any_num_cmp {
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

macro_rules! derive_float_any_num_cmp {
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

derive_primitive_any_num_cmp!(u8, u16, u32, u64, usize);
derive_primitive_any_num_cmp!(i8, i16, i32, i64, isize);
derive_float_any_num_cmp!(f32, f64);

pub enum NextDirection {
    Up,
    Down,
}

pub trait NextNumericValue {
    fn next_numeric_value(&self, direction: NextDirection) -> Self;
}

macro_rules! derive_primitive_next_numeric_value {
    ($($T:ty),+) => {
        $(
            impl NextNumericValue for $T {
                fn next_numeric_value(&self, direction: NextDirection) -> Self {
                    let clamp = if matches!(direction, NextDirection::Up) {
                        <$T>::MAX
                    } else {
                        <$T>::MIN
                    };

                    if *self == clamp {
                        clamp
                    } else if matches!(direction, NextDirection::Up) {
                        self + <$T as num_traits::One>::one()
                    } else {
                        self - <$T as num_traits::One>::one()
                    }
                }
            }
        )+
    };
}

macro_rules! derive_float_next_numeric_value {
    ($($T:ty),+) => {
        $(
            impl NextNumericValue for $T {
                fn next_numeric_value(&self, direction: NextDirection) -> Self {
                    if matches!(direction, NextDirection::Up) {
                        self.next_after(<$T>::INFINITY)
                    } else {
                        self.next_after(<$T>::NEG_INFINITY)
                    }
                }
            }
        )+
    };
}

derive_primitive_next_numeric_value!(u8, u16, u32, u64, usize);
derive_primitive_next_numeric_value!(i8, i16, i32, i64, isize);
derive_float_next_numeric_value!(f32, f64);

pub trait SmallestPositiveValue {
    fn smallest_positive_value() -> Self;
}

macro_rules! derive_primitive_smallest_positive_value {
    ($($T:ty),+) => {
        $(
            impl SmallestPositiveValue for $T {
                fn smallest_positive_value() -> Self {
                    <$T as num_traits::One>::one()
                }
            }
        )+
    };
}

macro_rules! derive_float_smallest_positive_value {
    ($($T:ty),+) => {
        $(
            impl SmallestPositiveValue for $T {
                fn smallest_positive_value() -> Self {
                    <$T>::MIN_POSITIVE
                }
            }
        )+
    };
}

derive_primitive_smallest_positive_value!(u8, u16, u32, u64, usize);
derive_primitive_smallest_positive_value!(i8, i16, i32, i64, isize);
derive_float_smallest_positive_value!(f32, f64);
