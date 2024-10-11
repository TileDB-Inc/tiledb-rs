#[cfg(feature = "option-subset")]
use tiledb_utils::option::OptionSubset;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use tiledb_common::array::{ArrayType, CellOrder, CellValNum, TileOrder};
use tiledb_common::datatype::Datatype;
use tiledb_common::filter::FilterData;
use tiledb_common::key::LookupKey;

use crate::array::attribute::AttributeData;
use crate::array::dimension::DimensionData;
use crate::array::domain::DomainData;

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
    pub coordinate_filters: Vec<FilterData>,
    pub offsets_filters: Vec<FilterData>,
    pub nullity_filters: Vec<FilterData>,
}

impl SchemaData {
    const DEFAULT_SPARSE_TILE_CAPACITY: u64 = 10000;

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

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;

#[cfg(test)]
mod tests {
    /// Test what the default values filled in for `None` with schema data are.
    /// Mostly because if we write code which does need the default, we're expecting
    /// to match core and need to be notified if something changes or we did something
    /// wrong.
    #[test]
    fn test_defaults() {
        use crate::array::dimension::DimensionConstraints;

        let ctx = Context::new().unwrap();

        let dense_spec = SchemaData {
            array_type: ArrayType::Dense,
            domain: DomainData {
                dimension: vec![DimensionData {
                    name: "d".to_string(),
                    datatype: Datatype::Int32,
                    constraints: DimensionConstraints::Int32([0, 100], None),
                    filters: None,
                }],
            },
            attributes: vec![AttributeData {
                name: "a".to_string(),
                datatype: Datatype::Int32,
                ..Default::default()
            }],
            ..Default::default()
        };

        let dense_schema = dense_spec
            .create(&ctx)
            .expect("Error creating schema from mostly-default settings");

        assert_eq!(ArrayType::Dense, dense_schema.array_type().unwrap());
        assert_eq!(10000, dense_schema.capacity().unwrap());
        assert_eq!(CellOrder::RowMajor, dense_schema.cell_order().unwrap());
        assert_eq!(TileOrder::RowMajor, dense_schema.tile_order().unwrap());
        assert!(!dense_schema.allows_duplicates().unwrap());

        let sparse_spec = SchemaData {
            array_type: ArrayType::Sparse,
            domain: DomainData {
                dimension: vec![DimensionData {
                    name: "d".to_string(),
                    datatype: Datatype::Int32,
                    constraints: DimensionConstraints::Int32([0, 100], None),
                    filters: None,
                }],
            },
            attributes: vec![AttributeData {
                name: "a".to_string(),
                datatype: Datatype::Int32,
                ..Default::default()
            }],
            ..Default::default()
        };
        let sparse_schema = sparse_spec
            .create(&ctx)
            .expect("Error creating schema from mostly-default settings");

        assert_eq!(ArrayType::Sparse, sparse_schema.array_type().unwrap());
        assert_eq!(
            SchemaData::DEFAULT_SPARSE_TILE_CAPACITY,
            sparse_schema.capacity().unwrap()
        );
        assert_eq!(CellOrder::RowMajor, sparse_schema.cell_order().unwrap());
        assert_eq!(TileOrder::RowMajor, sparse_schema.tile_order().unwrap());
        assert!(!sparse_schema.allows_duplicates().unwrap());
    }
}
