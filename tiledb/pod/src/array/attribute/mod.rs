#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;

use std::rc::Rc;

#[cfg(feature = "option-subset")]
use tiledb_utils::option::OptionSubset;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize, Serializer};

use tiledb_common::array::CellValNum;
use tiledb_common::datatype::Datatype;
use tiledb_common::filter::FilterData;
use tiledb_common::metadata::Value as MetadataValue;

use crate::array::EnumerationData;

#[derive(Clone, Default, Debug, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct AttributeData {
    pub name: String,
    pub datatype: Datatype,
    pub nullability: Option<bool>,
    pub cell_val_num: Option<CellValNum>,
    pub fill: Option<FillData>,
    pub filters: Vec<FilterData>,
    pub enumeration: Option<EnumerationRef>,
}

/// Encapsulation of data needed to construct an Attribute's fill value
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct FillData {
    pub data: MetadataValue,
    pub nullability: Option<bool>,
}

/// Identifies an [EnumerationData] which this attribute indexes.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum EnumerationRef {
    /// The name of an enumeration whose data lives elsewhere.
    Name(String),
    /// The enumeration is uniquely owned by this attribute.
    ///
    /// In typical usage, this is an ephemeral state before the
    /// attribute is attached to a [SchemaData].
    OwnedByAttribute(EnumerationData),
    /// This attribute is owned by a [SchemaData] and is sharing
    /// its enumeration data.
    ///
    /// This is the expected variant for attributes which are
    /// owned by [SchemaData]. Other variants may be used transiently.
    /// For example, serialization of this variant writes only the enumeration name,
    /// and deserialization of the owning [SchemaData] is responsible for
    /// reconstructing this variant.
    #[cfg_attr(
        feature = "serde",
        serde(
            rename = "Name",
            serialize_with = "EnumerationRef::serialize_owned_by_schema",
            skip_deserializing
        )
    )]
    BorrowedFromSchema(Rc<EnumerationData>),
}

impl EnumerationRef {
    pub fn name(&self) -> &str {
        match self {
            Self::Name(ref s) => s,
            Self::OwnedByAttribute(ref e) => e.name.as_ref(),
            Self::BorrowedFromSchema(ref e) => e.name.as_ref(),
        }
    }

    pub fn values(&self) -> Option<&EnumerationData> {
        match self {
            Self::Name(_) => None,
            Self::OwnedByAttribute(e) => Some(e),
            Self::BorrowedFromSchema(e) => Some(e),
        }
    }

    pub fn for_schema(&mut self) -> Option<Rc<EnumerationData>> {
        let Self::OwnedByAttribute(owned) = self else {
            return None;
        };
        let shared = Rc::new(owned.clone());
        *self = Self::BorrowedFromSchema(Rc::clone(&shared));

        Some(shared)
    }
}

#[cfg(feature = "serde")]
impl EnumerationRef {
    fn serialize_owned_by_schema<S>(
        borrowed: &Rc<EnumerationData>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        borrowed.name.serialize(serializer)
    }
}

#[cfg(feature = "option-subset")]
impl OptionSubset for EnumerationRef {
    fn option_subset(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::OwnedByAttribute(mine), Self::OwnedByAttribute(theirs)) => {
                mine.option_subset(theirs)
            }
            (_, _) => self.name() == other.name(),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "serde")]
    mod serde {
        use proptest::prelude::*;

        use super::super::*;

        proptest! {
            #[test]
            fn enumeration_ref_name_roundtrip(name in any::<String>()) {
                let e = EnumerationRef::Name(name);
                assert_eq!(e, crate::test::serde::roundtrip(&e).unwrap());
            }

            #[test]
            fn enumeration_ref_owned_roundtrip(enmr in any::<EnumerationData>()) {
                let e = EnumerationRef::OwnedByAttribute(enmr);
                assert_eq!(e, crate::test::serde::roundtrip(&e).unwrap());
            }

            #[test]
            fn enumeration_ref_borrowed_roundtrip(enmr in any::<EnumerationData>()) {
                let e = EnumerationRef::BorrowedFromSchema(Rc::new(enmr));
                assert_eq!(
                    EnumerationRef::Name(e.name().to_owned()),
                    crate::test::serde::roundtrip(&e).unwrap()
                );
            }
        }
    }
}
