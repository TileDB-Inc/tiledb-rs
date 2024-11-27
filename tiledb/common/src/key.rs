#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum LookupKey {
    Index(usize),
    Name(String),
}

macro_rules! impl_lookup_key_for_primitive {
    ($($t:ty),*) => {
        $(
            impl From<$t> for LookupKey {
                fn from(value: $t) -> Self {
                    LookupKey::Index(value as usize)
                }
            }
        )*
    };
}

macro_rules! impl_lookup_key_for_str {
    ($($t:ty),*) => {
        $(
            impl From<$t> for LookupKey {
                fn from(value: $t) -> Self {
                    LookupKey::Name(String::from(value))
                }
            }
        )*
    }
}

impl_lookup_key_for_primitive!(u8, u16, u32, u64, usize);
impl_lookup_key_for_primitive!(i8, i16, i32, i64, isize);
impl_lookup_key_for_str!(&str, String, &String);
