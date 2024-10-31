#[cfg(feature = "option-subset")]
use tiledb_utils::option::OptionSubset;

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;

use std::rc::Rc;

#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use tiledb_common::array::{ArrayType, CellOrder, CellValNum, TileOrder};
use tiledb_common::datatype::Datatype;
use tiledb_common::filter::FilterData;
use tiledb_common::key::LookupKey;

use crate::array::{AttributeData, DimensionData, DomainData, EnumerationData};

/// Encapsulation of data needed to construct a Schema
#[derive(Clone, Default, Debug, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct SchemaData {
    pub array_type: ArrayType,
    pub domain: DomainData,
    pub capacity: Option<u64>,
    pub cell_order: Option<CellOrder>,
    pub tile_order: Option<TileOrder>,
    pub allow_duplicates: Option<bool>,
    pub attributes: Vec<AttributeData>,
    #[cfg_attr(
        feature = "serde",
        serde(
            serialize_with = "SchemaData::serialize_enumerations",
            deserialize_with = "SchemaData::deserialize_enumerations"
        )
    )]
    pub enumerations: Vec<Rc<EnumerationData>>,
    pub coordinate_filters: Vec<FilterData>,
    pub offsets_filters: Vec<FilterData>,
    pub nullity_filters: Vec<FilterData>,
}

impl SchemaData {
    pub const DEFAULT_SPARSE_TILE_CAPACITY: u64 = 10000;

    pub fn num_fields(&self) -> usize {
        self.domain.dimension.len() + self.attributes.len()
    }

    pub fn field<K: Into<LookupKey>>(&self, key: K) -> Option<FieldData> {
        match key.into() {
            LookupKey::Index(idx) => {
                if idx < self.domain.dimension.len() {
                    Some(FieldData::from(self.domain.dimension[idx].clone()))
                } else if idx
                    < self.domain.dimension.len() + self.attributes.len()
                {
                    Some(FieldData::from(
                        self.attributes[idx - self.domain.dimension.len()]
                            .clone(),
                    ))
                } else {
                    None
                }
            }
            LookupKey::Name(name) => {
                for d in self.domain.dimension.iter() {
                    if d.name == name {
                        return Some(FieldData::from(d.clone()));
                    }
                }
                for a in self.attributes.iter() {
                    if a.name == name {
                        return Some(FieldData::from(a.clone()));
                    }
                }
                None
            }
        }
    }

    pub fn fields(&self) -> FieldDataIter {
        FieldDataIter::new(self)
    }

    /// Returns the number of cells per tile
    pub fn num_cells_per_tile(&self) -> usize {
        match self.array_type {
            ArrayType::Dense => {
                // it should be safe to unwrap, the two `None` conditions must not
                // be satisfied for a dense array domain
                // (TODO: what about for string ascii dense domains?)
                self.domain.num_cells_per_tile().unwrap()
            }
            ArrayType::Sparse => {
                self.capacity.unwrap_or(Self::DEFAULT_SPARSE_TILE_CAPACITY)
                    as usize
            }
        }
    }
}

#[cfg(feature = "serde")]
impl SchemaData {
    fn serialize_enumerations<S>(
        enumerations: &[Rc<EnumerationData>],
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        enumerations
            .iter()
            .map(|e| e.as_ref())
            .collect::<Vec<&EnumerationData>>()
            .serialize(serializer)
    }

    fn deserialize_enumerations<'de, D>(
        deserializer: D,
    ) -> Result<Vec<Rc<EnumerationData>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Vec::<EnumerationData>::deserialize(deserializer)
            .map(|v| v.into_iter().map(Rc::new).collect::<Vec<_>>())
    }
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum FieldData {
    Dimension(DimensionData),
    Attribute(AttributeData),
}

impl FieldData {
    pub fn is_attribute(&self) -> bool {
        matches!(self, Self::Attribute(_))
    }

    pub fn is_dimension(&self) -> bool {
        matches!(self, Self::Dimension(_))
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Dimension(d) => &d.name,
            Self::Attribute(a) => &a.name,
        }
    }

    pub fn datatype(&self) -> Datatype {
        match self {
            Self::Dimension(d) => d.datatype,
            Self::Attribute(a) => a.datatype,
        }
    }

    pub fn cell_val_num(&self) -> Option<CellValNum> {
        match self {
            Self::Dimension(d) => Some(d.cell_val_num()),
            Self::Attribute(a) => a.cell_val_num,
        }
    }

    pub fn nullability(&self) -> Option<bool> {
        match self {
            Self::Dimension(_) => Some(false),
            Self::Attribute(a) => a.nullability,
        }
    }
}

impl From<AttributeData> for FieldData {
    fn from(attr: AttributeData) -> Self {
        FieldData::Attribute(attr)
    }
}

impl From<DimensionData> for FieldData {
    fn from(dim: DimensionData) -> Self {
        FieldData::Dimension(dim)
    }
}

pub struct FieldDataIter<'a> {
    schema: &'a SchemaData,
    cursor: usize,
}

impl<'a> FieldDataIter<'a> {
    pub fn new(schema: &'a SchemaData) -> Self {
        FieldDataIter { schema, cursor: 0 }
    }
}

impl Iterator for FieldDataIter<'_> {
    type Item = FieldData;
    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor < self.schema.num_fields() {
            let item = self.schema.field(self.cursor);
            self.cursor += 1;
            Some(item.expect("Internal indexing error"))
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.schema.num_fields() - self.cursor;
        (exact, Some(exact))
    }
}

impl std::iter::FusedIterator for FieldDataIter<'_> {}

#[cfg(test)]
mod tests {
    #[cfg(feature = "serde")]
    mod serde {
        use std::cell::LazyCell;
        use std::collections::HashMap;

        use proptest::prelude::*;

        use super::super::*;
        use crate::array::attribute::EnumerationRef;

        fn do_roundtrip(schema_in: SchemaData) {
            let mut schema_out =
                crate::test::serde::roundtrip(&schema_in).unwrap();

            // attributes would have gone from `EnumerationRef::BorrowedBySchema` to
            // `EnumerationRef::Name`, resolve those
            let enumeration_map = LazyCell::new(|| {
                schema_in
                    .enumerations
                    .iter()
                    .map(|e| (e.name.clone(), Rc::clone(e)))
                    .collect::<HashMap<_, _>>()
            });

            for attribute in schema_out.attributes.iter_mut() {
                let Some(enumeration) = attribute.enumeration.as_mut() else {
                    continue;
                };
                let EnumerationRef::Name(ename) = enumeration else {
                    unreachable!()
                };

                let eref = Rc::clone(enumeration_map.get(ename).unwrap());
                *enumeration = EnumerationRef::BorrowedFromSchema(eref);
            }

            assert_eq!(schema_in, schema_out);
        }

        proptest! {
            #[test]
            fn roundtrip(schema_in in any::<SchemaData>()) {
                do_roundtrip(schema_in)
            }
        }
    }
}
