use std::convert::From;

use crate::datatype::Datatype;
use crate::datatype::Error as DatatypeError;

#[cfg(feature = "option-subset")]
use tiledb_utils::option::OptionSubset;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum Value {
    UInt8Value(Vec<u8>),
    UInt16Value(Vec<u16>),
    UInt32Value(Vec<u32>),
    UInt64Value(Vec<u64>),
    Int8Value(Vec<i8>),
    Int16Value(Vec<i16>),
    Int32Value(Vec<i32>),
    Int64Value(Vec<i64>),
    Float32Value(Vec<f32>),
    Float64Value(Vec<f64>),
}

/// Applies a generic expression to the interior of a `Value`.
///
/// # Examples
/// ```
/// use tiledb_common::metadata::Value;
/// use tiledb_common::metadata_value_go;
///
/// fn truncate(v: &mut Value, len: usize) {
///     metadata_value_go!(v, _DT, ref mut v_inner, v_inner.truncate(len));
/// }
///
/// let mut v = Value::UInt64Value(vec![0, 24, 48]);
/// truncate(&mut v, 2);
/// assert_eq!(v, Value::UInt64Value(vec![0, 24]));
/// ```
#[macro_export]
macro_rules! metadata_value_go {
    ($valuetype:expr, $typename:ident, $vec:pat, $then: expr) => {{
        use $crate::metadata::Value;
        match $valuetype {
            Value::Int8Value($vec) => {
                type $typename = i8;
                $then
            }
            Value::Int16Value($vec) => {
                type $typename = i16;
                $then
            }
            Value::Int32Value($vec) => {
                type $typename = i32;
                $then
            }
            Value::Int64Value($vec) => {
                type $typename = i64;
                $then
            }
            Value::UInt8Value($vec) => {
                type $typename = u8;
                $then
            }
            Value::UInt16Value($vec) => {
                type $typename = u16;
                $then
            }
            Value::UInt32Value($vec) => {
                type $typename = u32;
                $then
            }
            Value::UInt64Value($vec) => {
                type $typename = u64;
                $then
            }
            Value::Float32Value($vec) => {
                type $typename = f32;
                $then
            }
            Value::Float64Value($vec) => {
                type $typename = f64;
                $then
            }
        }
    }};
}
pub use metadata_value_go;

/// Applies a generic expression to the interiors of two `Value`s with matching variants,
/// i.e. with the same physical data type. Typical usage is for comparing the insides of the two
/// `Value`s.
#[macro_export]
macro_rules! value_cmp {
    ($lexpr:expr, $rexpr:expr, $typename:ident, $lpat:pat, $rpat:pat, $same_type:expr, $else:expr) => {{
        use $crate::metadata::Value;
        match ($lexpr, $rexpr) {
            (Value::Int8Value($lpat), Value::Int8Value($rpat)) => {
                type $typename = i8;
                $same_type
            }
            (Value::Int16Value($lpat), Value::Int16Value($rpat)) => {
                type $typename = i16;
                $same_type
            }
            (Value::Int32Value($lpat), Value::Int32Value($rpat)) => {
                type $typename = i32;
                $same_type
            }
            (Value::Int64Value($lpat), Value::Int64Value($rpat)) => {
                type $typename = i64;
                $same_type
            }
            (Value::UInt8Value($lpat), Value::UInt8Value($rpat)) => {
                type $typename = u8;
                $same_type
            }
            (Value::UInt16Value($lpat), Value::UInt16Value($rpat)) => {
                type $typename = u16;
                $same_type
            }
            (Value::UInt32Value($lpat), Value::UInt32Value($rpat)) => {
                type $typename = u32;
                $same_type
            }
            (Value::UInt64Value($lpat), Value::UInt64Value($rpat)) => {
                type $typename = u64;
                $same_type
            }
            (Value::Float32Value($lpat), Value::Float32Value($rpat)) => {
                type $typename = f32;
                $same_type
            }
            (Value::Float64Value($lpat), Value::Float64Value($rpat)) => {
                type $typename = f64;
                $same_type
            }
            _ => $else,
        }
    }};
}

impl Value {
    pub fn len(&self) -> usize {
        metadata_value_go!(self, _DT, ref v, v.len())
    }

    pub fn is_empty(&self) -> bool {
        metadata_value_go!(self, _DT, ref v, v.is_empty())
    }
}

macro_rules! metadata_value_impl {
    ($ty:ty, $constructor:expr) => {
        impl From<Vec<$ty>> for Value {
            fn from(vec: Vec<$ty>) -> Self {
                $constructor(vec)
            }
        }
    };
}

metadata_value_impl!(i8, Value::Int8Value);
metadata_value_impl!(i16, Value::Int16Value);
metadata_value_impl!(i32, Value::Int32Value);
metadata_value_impl!(i64, Value::Int64Value);
metadata_value_impl!(u8, Value::UInt8Value);
metadata_value_impl!(u16, Value::UInt16Value);
metadata_value_impl!(u32, Value::UInt32Value);
metadata_value_impl!(u64, Value::UInt64Value);
metadata_value_impl!(f32, Value::Float32Value);
metadata_value_impl!(f64, Value::Float64Value);

#[derive(Clone, Debug, PartialEq)]
pub struct Metadata {
    pub key: String,
    pub datatype: Datatype,
    pub value: Value,
}

impl Metadata {
    pub fn new<T>(
        key: String,
        datatype: Datatype,
        vec: Vec<T>,
    ) -> Result<Self, DatatypeError>
    where
        Value: From<Vec<T>>,
        T: 'static,
    {
        if !datatype.is_compatible_type::<T>() {
            return Err(DatatypeError::physical_type_incompatible::<T>(
                datatype,
            ));
        }
        Ok(Metadata {
            key,
            datatype,
            value: Value::from(vec),
        })
    }
}

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy {
    use super::*;
    use proptest::collection::{vec, SizeRange};
    use proptest::prelude::*;

    use crate::datatype::strategy::DatatypeContext;
    use crate::physical_type_go;

    pub struct Requirements {
        key: BoxedStrategy<String>,
        datatype: BoxedStrategy<Datatype>,
        value_length: SizeRange,
    }

    impl Requirements {
        const DEFAULT_VALUE_LENGTH_MIN: usize = 1; // SC-48955
        const DEFAULT_VALUE_LENGTH_MAX: usize = 64;
    }

    impl Default for Requirements {
        fn default() -> Self {
            Requirements {
                key: any::<String>().boxed(),
                datatype: any_with::<Datatype>(DatatypeContext::NotAny).boxed(),
                value_length: (Self::DEFAULT_VALUE_LENGTH_MIN
                    ..=Self::DEFAULT_VALUE_LENGTH_MAX)
                    .into(),
            }
        }
    }

    impl Arbitrary for Metadata {
        type Parameters = Requirements;
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
            params
                .datatype
                .prop_flat_map(move |dt| {
                    let value_strat = physical_type_go!(dt, DT, {
                        vec(any::<DT>(), params.value_length.clone())
                            .prop_map(Value::from)
                            .boxed()
                    });
                    (params.key.clone(), Just(dt), value_strat)
                })
                .prop_map(|(key, datatype, value)| Metadata {
                    key,
                    datatype,
                    value,
                })
                .boxed()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn do_value_cmp(m1: Metadata, m2: Metadata) {
        if m1.datatype.same_physical_type(&m2.datatype) {
            value_cmp!(&m1.value, &m2.value, _DT, _, _,
                (),
                unreachable!("Non-matching `Value` variants for same physical type: {:?} and {:?}",
                    m1, m2));
        } else {
            value_cmp!(&m1.value, &m2.value, _DT, _, _,
                unreachable!("Matching `Value` variants for different physical type: {:?} and {:?}",
                    m1, m2),
                ());
        }
    }

    proptest! {
        #[test]
        fn value_cmp((m1, m2) in (any::<Metadata>(), any::<Metadata>())) {
            do_value_cmp(m1, m2)
        }
    }
}
